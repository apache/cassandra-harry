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
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Configuration;
import harry.core.Run;
import harry.corruptor.AddExtraRowCorruptor;
import harry.corruptor.ChangeValueCorruptor;
import harry.corruptor.HideRowCorruptor;
import harry.corruptor.HideValueCorruptor;
import harry.corruptor.QueryResponseCorruptor;
import harry.model.ExhaustiveChecker;
import harry.model.OpSelectors;

public interface HarryRunner
{

    Logger logger = LoggerFactory.getLogger(HarryRunner.class);

    default void run(Configuration.SutConfiguration sutConfig) throws Throwable
    {
        System.setProperty("cassandra.disable_tcactive_openssl", "true");
        System.setProperty("relocated.shaded.io.netty.transport.noNative", "true");
        System.setProperty("org.apache.cassandra.disable_mbean_registration", "true");

        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.setRemoveOnCancelPolicy(true);

        Configuration.ConfigurationBuilder configuration = new Configuration.ConfigurationBuilder();

        long seed = System.currentTimeMillis();
        configuration.setSeed(seed)
                     .setSUT(sutConfig)
                     .setPartitionDescriptorSelector(new Configuration.DefaultPDSelectorConfiguration(10, 100))
                     .setClusteringDescriptorSelector((builder) -> {
                         builder.setNumberOfModificationsDistribution(new Configuration.ConstantDistributionConfig(10))
                                .setRowsPerModificationDistribution(new Configuration.ConstantDistributionConfig(10))
                                .setMaxPartitionSize(100)
                                .setOperationKindWeights(new Configuration.OperationKindSelectorBuilder()
                                                         .addWeight(OpSelectors.OperationKind.DELETE_ROW, 1)
                                                         .addWeight(OpSelectors.OperationKind.DELETE_COLUMN, 1)
                                                         .addWeight(OpSelectors.OperationKind.DELETE_RANGE, 1)
                                                         .addWeight(OpSelectors.OperationKind.WRITE, 97)
                                                         .build());
                     })
                     .setRowVisitor(new Configuration.DefaultRowVisitorConfiguration())
                     .setClock(new Configuration.ApproximateMonotonicClockConfiguration((int) TimeUnit.HOURS.toSeconds(2) + 100,
                                                                                        1,
                                                                                        TimeUnit.SECONDS))
                     .setRunTime(2, TimeUnit.HOURS)
                     .setCreateSchema(true)
                     .setTruncateTable(false)
                     .setDropSchema(false)
                     .setModel(ExhaustiveChecker::new)
                     .setRunner(new Configuration.ConcurrentRunnerConfig(1, 1, 1));

        Runner runner = configuration.build().createRunner();
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
                ((Throwable) result).printStackTrace();

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