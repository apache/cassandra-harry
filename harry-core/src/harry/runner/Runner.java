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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Configuration;
import harry.core.Run;
import harry.model.OpSelectors;
import harry.visitors.PartitionVisitor;


public abstract class Runner
{
    private static final Logger logger = LoggerFactory.getLogger(Runner.class);

    protected final Run run;
    protected final Configuration config;

    // If there's an error, there's a good chance we're going to hit it more than once
    //  since we have multiple concurrent checkers running
    protected final CopyOnWriteArrayList<Throwable> errors;

    public Runner(Run run, Configuration config)
    {
        this.run = run;
        this.config = config;
        this.errors = new CopyOnWriteArrayList<>();
    }

    public Run getRun()
    {
        return run;
    }

    public void init()
    {
        if (config.create_schema)
        {
            // TODO: make RF configurable or make keyspace DDL configurable
            run.sut.schemaChange("CREATE KEYSPACE " + run.schemaSpec.keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");

            run.sut.schemaChange(String.format("DROP TABLE IF EXISTS %s.%s;",
                                          run.schemaSpec.keyspace,
                                          run.schemaSpec.table));
            String schema = run.schemaSpec.compile().cql();
            logger.info("Creating table: " + schema);
            run.sut.schemaChange(schema);
        }

        if (config.truncate_table)
        {
            run.sut.schemaChange(String.format("truncate %s.%s;",
                                               run.schemaSpec.keyspace,
                                               run.schemaSpec.table));
        }

        run.sut.afterSchemaInit();
    }

    public void teardown()
    {
        logger.info("Tearing down setup...");
        if (config.drop_schema)
        {
            if (!errors.isEmpty())
            {
                logger.info("Preserving table {} due to errors during execution.",
                            run.schemaSpec.table);
                return;
            }

            logger.info("Dropping table: " + run.schemaSpec.table);
            run.sut.schemaChange(String.format("DROP TABLE IF EXISTS %s.%s;",
                                               run.schemaSpec.keyspace,
                                               run.schemaSpec.table));
        }
    }

    protected void maybeReportErrors()
    {
        if (!errors.isEmpty())
            dumpStateToFile(run, config, errors);
    }

    public abstract CompletableFuture<?> initAndStartAll() throws InterruptedException;

    public abstract void shutdown() throws InterruptedException;

    protected Runnable reportThrowable(Runnable runnable, CompletableFuture<?> future)
    {
        return () -> {
            try
            {
                if (!future.isDone())
                    runnable.run();
            }
            catch (Throwable t)
            {
                errors.add(t);
                if (!future.isDone())
                    future.completeExceptionally(t);
            }
        };
    }

    public static class SequentialRunner extends Runner
    {
        private final ScheduledExecutorService executor;
        private final ScheduledExecutorService shutdownExceutor;
        private final List<PartitionVisitor> partitionVisitors;
        private final Configuration config;

        public SequentialRunner(Run run,
                                Configuration config,
                                List<? extends PartitionVisitor.PartitionVisitorFactory> partitionVisitorFactories)
        {
            super(run, config);

            this.executor = Executors.newSingleThreadScheduledExecutor();
            this.shutdownExceutor = Executors.newSingleThreadScheduledExecutor();
            this.config = config;
            this.partitionVisitors = new ArrayList<>();
            for (PartitionVisitor.PartitionVisitorFactory factory : partitionVisitorFactories)
                partitionVisitors.add(factory.make(run));
        }

        public CompletableFuture<?> initAndStartAll()
        {
            init();
            CompletableFuture<?> future = new CompletableFuture<>();
            future.whenComplete((a, b) -> maybeReportErrors());

            AtomicBoolean completed = new AtomicBoolean(false);
            shutdownExceutor.schedule(() -> {
                logger.info("Completed");
                // TODO: wait for the last full validation?
                completed.set(true);
            }, config.run_time, config.run_time_unit);

            executor.submit(reportThrowable(() -> {
                                                try
                                                {
                                                    SequentialRunner.run(partitionVisitors, run.clock, future,
                                                                         () -> Thread.currentThread().isInterrupted() || future.isDone() || completed.get());
                                                }
                                                catch (Throwable t)
                                                {
                                                    future.completeExceptionally(t);
                                                }
                                            },
                                            future));

            return future;
        }

        static void run(List<PartitionVisitor> visitors,
                        OpSelectors.MonotonicClock clock,
                        CompletableFuture<?> future,
                        BooleanSupplier exitCondition)
        {
            while (!exitCondition.getAsBoolean())
            {
                long lts = clock.nextLts();

                if (lts > 0 && lts % 10_000 == 0)
                    logger.info("Visited {} logical timestamps", lts);

                for (int i = 0; i < visitors.size() && !exitCondition.getAsBoolean(); i++)
                {
                    try
                    {
                        PartitionVisitor partitionVisitor = visitors.get(i);
                        partitionVisitor.visitPartition(lts);
                    }
                    catch (Throwable t)
                    {
                        future.completeExceptionally(t);
                        throw t;
                    }
                }
            }
            future.complete(null);
        }

        public void shutdown() throws InterruptedException
        {
            logger.info("Shutting down...");
            shutdownExceutor.shutdownNow();
            shutdownExceutor.awaitTermination(1, TimeUnit.MINUTES);

            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.MINUTES);
            // we need to wait for all threads that use schema to stop before we can tear down and drop the table
            teardown();
        }
    }

    public static interface RunnerFactory
    {
        public Runner make(Run run, Configuration config);
    }

    // TODO: this requires some significant improvement
    public static class ConcurrentRunner extends Runner
    {
        private final ScheduledExecutorService executor;
        private final ScheduledExecutorService shutdownExecutor;
        private final List<? extends PartitionVisitor.PartitionVisitorFactory> partitionVisitorFactories;
        private final List<PartitionVisitor> allVisitors;

        private final int concurrency;
        private final long runTime;
        private final TimeUnit runTimeUnit;

        public ConcurrentRunner(Run run,
                                Configuration config,
                                int concurrency,
                                List<? extends PartitionVisitor.PartitionVisitorFactory> partitionVisitorFactories)
        {
            super(run, config);
            this.concurrency = concurrency;
            this.runTime = config.run_time;
            this.runTimeUnit = config.run_time_unit;
            // TODO: configure concurrency
            this.executor = Executors.newScheduledThreadPool(concurrency);
            this.shutdownExecutor = Executors.newSingleThreadScheduledExecutor();
            this.partitionVisitorFactories = partitionVisitorFactories;
            this.allVisitors = new CopyOnWriteArrayList<>();
        }

        public CompletableFuture<?> initAndStartAll()
        {
            init();
            CompletableFuture<?> future = new CompletableFuture<>();
            future.whenComplete((a, b) -> maybeReportErrors());

            shutdownExecutor.schedule(() -> {
                logger.info("Completed");
                // TODO: wait for the last full validation?
                future.complete(null);
            }, runTime, runTimeUnit);

            BooleanSupplier exitCondition = () -> Thread.currentThread().isInterrupted() || future.isDone();
            for (int i = 0; i < concurrency; i++)
            {
                List<PartitionVisitor> partitionVisitors = new ArrayList<>();
                executor.submit(reportThrowable(() -> {
                                                    for (PartitionVisitor.PartitionVisitorFactory factory : partitionVisitorFactories)
                                                        partitionVisitors.add(factory.make(run));

                                                    allVisitors.addAll(partitionVisitors);
                                                    run(partitionVisitors, run.clock, exitCondition);
                                                },
                                                future));

            }

            return future;
        }

        void run(List<PartitionVisitor> visitors,
                 OpSelectors.MonotonicClock clock,
                 BooleanSupplier exitCondition)
        {
            while (!exitCondition.getAsBoolean())
            {
                long lts = clock.nextLts();
                for (PartitionVisitor visitor : visitors)
                    visitor.visitPartition(lts);
            }
        }

        public void shutdown() throws InterruptedException
        {
            logger.info("Shutting down...");
            for (PartitionVisitor visitor : allVisitors)
                visitor.shutdown();

            shutdownExecutor.shutdownNow();
            shutdownExecutor.awaitTermination(1, TimeUnit.MINUTES);

            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.MINUTES);
            // we need to wait for all threads that use schema to stop before we can tear down and drop the table
            teardown();
        }
    }

    private static void dumpExceptionToFile(BufferedWriter bw, Throwable throwable) throws IOException
    {
        if (throwable.getMessage() != null)
            bw.write(throwable.getMessage());
        else
            bw.write("<no message>");

        bw.newLine();
        for (StackTraceElement line : throwable.getStackTrace())
        {
            bw.write(line.toString());
            bw.newLine();
        }
        bw.newLine();
        if (throwable.getCause() != null)
        {
            bw.write("Inner Exception: ");
            dumpExceptionToFile(bw, throwable.getCause());
        }

        bw.newLine();
        bw.newLine();
    }

    private static void dumpStateToFile(Run run, Configuration config, List<Throwable> t)
    {
        try
        {
            File f = new File("failure.dump");
            logger.error("Dumping results into the file:" + f);
            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f))))
            {
                bw.write("Caught exception during the run: ");
                for (Throwable throwable : t)
                    dumpExceptionToFile(bw, throwable);

                bw.flush();
            }

            File file = new File("run.yaml");
            Configuration.ConfigurationBuilder builder = config.unbuild();

            // overrride stateful components
            builder.setClock(run.clock.toConfig());
            builder.setDataTracker(run.tracker.toConfig());

            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file))))
            {
                bw.write(Configuration.toYamlString(builder.build()));
                bw.flush();
            }
        }
        catch (Throwable e)
        {
            logger.error("Caught an error while trying to dump to file",
                         e);
            try
            {
                File f = new File("tmp.dump");
                f.createNewFile();
                BufferedWriter tmp = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f)));
                dumpExceptionToFile(tmp, e);
            }
            catch (IOException ex)
            {
                ex.printStackTrace();
            }

            throw new RuntimeException(e);
        }
    }
}