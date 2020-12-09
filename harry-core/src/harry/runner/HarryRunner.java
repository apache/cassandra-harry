/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package harry.runner;

import harry.core.Configuration;
import harry.core.Run;
import harry.corruptor.*;
import harry.util.ThrowingRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public interface HarryRunner
{

    Logger logger = LoggerFactory.getLogger(HarryRunner.class);

    default void run(Configuration configuration) throws Throwable
    {
        System.setProperty("cassandra.disable_tcactive_openssl", "true");
        System.setProperty("relocated.shaded.io.netty.transport.noNative", "true");
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.setRemoveOnCancelPolicy(true);

        Runner runner = configuration.createRunner();
        Run run = runner.getRun();

        CompletableFuture progress = runner.initAndStartAll();

        // Uncomment this if you want to have fun!
        // scheduleCorruption(run, executor);

        Object result = null;

        try
        {
            result = progress.handle((a, b) -> {
                if (b != null)
                    return b;
                return a;
            }).get(run.snapshot.run_time_unit.toSeconds(run.snapshot.run_time) + 30,
                   TimeUnit.SECONDS);
            if (result instanceof Throwable)
                logger.error("Execution failed", result);

        }
        catch (Throwable e)
        {
            logger.error("Failed due to exception: " + e.getMessage(),
                         e);
            result = e;
        }
        finally
        {
            logger.info("Shutting down executor..");
            tryRun(() -> {
                executor.shutdownNow();
                executor.awaitTermination(10, TimeUnit.SECONDS);
            });

            logger.info("Shutting down runner..");
            tryRun(runner::shutdown);
            boolean failed = result instanceof Throwable;
            if (!failed)
            {
                logger.info("Shutting down cluster..");
                tryRun(run.sut::shutdown);
            }
            logger.info("Exiting...");
            if (failed)
                System.exit(1);
            else
                System.exit(0);
        }
    }

    default void tryRun(ThrowingRunnable runnable)
    {
        try
        {
            runnable.run();
        }
        catch (Throwable t)
        {
            logger.error("Encountered an error while shutting down, ignoring.", t);
        }
    }

    /**
     * Parses the command-line args and returns a File for the configuration YAML.
     * @param args Command-line args.
     * @return Configuration YAML file.
     * @throws Exception If file is not found or cannot be read.
     */
    default File loadConfig(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            throw new Exception("Harry config YAML not provided.");
        }

        File configFile =  new File(args[0]);
        if (!configFile.exists()) {
            throw new FileNotFoundException(configFile.getAbsolutePath());
        }
        if (!configFile.canRead()) {
            throw new Exception("Cannot read config file, check your permissions on " + configFile.getAbsolutePath());
        }

        return configFile;
    }

    /**
     * If you want to see how Harry detects problems!
     */
    public static void scheduleCorruption(Run run, ScheduledExecutorService executor)
    {
        QueryResponseCorruptor[] corruptors = new QueryResponseCorruptor[]{
        new QueryResponseCorruptor.SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                run.clock,
                                                                HideRowCorruptor::new),
        new AddExtraRowCorruptor(run.schemaSpec,
                                 run.clock,
                                 run.descriptorSelector),
        new QueryResponseCorruptor.SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                run.clock,
                                                                HideValueCorruptor::new),
        new QueryResponseCorruptor.SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                run.clock,
                                                                ChangeValueCorruptor::new)
        };

        Random random = new Random();
        executor.scheduleWithFixedDelay(() -> {
            try
            {
                QueryResponseCorruptor corruptor = corruptors[random.nextInt(corruptors.length)];
                long lts = run.clock.maxLts();
                long pd = run.pdSelector.pd(random.nextInt((int) lts), run.schemaSpec);
                boolean success = corruptor.maybeCorrupt(Query.selectPartition(run.schemaSpec, pd, false),
                                                         run.sut);
                logger.info("{} tried to corrupt a partition with a pd {}@{}", success ? "Successfully" : "Unsuccessfully", pd, lts);
            }
            catch (Throwable t)
            {
                logger.error("Caught an exception while trying to corrupt a partition.", t);
            }
        }, 30, 1, TimeUnit.SECONDS);
    }

}