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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Configuration;
import harry.core.Run;
import harry.util.ThrowingRunnable;

public abstract class HarryRunner
{
    public static final Logger logger = LoggerFactory.getLogger(HarryRunner.class);

    protected final boolean localRun = Boolean.parseBoolean(System.getProperty("harry.local-run", "true"));
    protected final String cassandraVersion = System.getProperty("harry.cassandra-version");
    protected final long startedAt = Long.getLong("harry.start-time", System.currentTimeMillis());

    protected CompletableFuture<?> progress;
    protected ScheduledThreadPoolExecutor executor;
    public abstract void beforeRun(Runner.TimedRunner runner);
    public void afterRun(Runner runner, Object result)
    {
        boolean success = !(result instanceof Throwable);

        executor.shutdown();
        try
        {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            // ignore
        }
    }

    public void run(Configuration config) throws Throwable
    {
        System.setProperty("cassandra.disable_tcactive_openssl", "true");
        System.setProperty("relocated.shaded.io.netty.transport.noNative", "true");
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");

        executor = new ScheduledThreadPoolExecutor(1);
        executor.setRemoveOnCancelPolicy(true);

        Runner runner = config.createRunner();
        Run run = runner.getRun();

        progress = runner.initAndStartAll();
        
        assert runner instanceof Runner.TimedRunner : "Please use a timed runner at the top level.";
        beforeRun((Runner.TimedRunner) runner);

        Object result = null;

        try
        {
            result = progress.handle((a, b) -> {
                if (b != null)
                    return b;
                return a;
            }).get();
            if (result instanceof Throwable)
                logger.error("Execution failed!", (Throwable) result);

        }
        catch (Throwable e)
        {
            logger.error("Failed due to exception: " + e.getMessage(), e);
            result = e;
        }
        finally
        {
            afterRun(runner, result);

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

    public void tryRun(ThrowingRunnable runnable)
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
    public static File loadConfig(String[] args) throws Exception {
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
}