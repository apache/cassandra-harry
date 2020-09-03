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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Configuration;
import harry.core.Run;
import harry.model.OpSelectors;


public abstract class Runner
{
    public static final Object[] EMPTY_BINDINGS = {};

    private static final Logger logger = LoggerFactory.getLogger(Runner.class);

    protected final Run run;
    protected final Configuration config;
    // If there's an error, there's a good chance we're going to hit it more than once
    //  since we have multiple concurrent checkers running
    protected final CopyOnWriteArrayList<Throwable> errors;

    public Runner(Run run)
    {
        this.run = run;
        this.config = run.snapshot;
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
            run.sut.execute("CREATE KEYSPACE " + run.schemaSpec.keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");

            run.sut.execute(String.format("DROP TABLE IF EXISTS %s.%s;",
                                          run.schemaSpec.keyspace,
                                          run.schemaSpec.table),
                            EMPTY_BINDINGS);
            String schema = run.schemaSpec.compile().cql();
            logger.info("Creating table: " + schema);
            run.sut.schemaChange(schema);
        }

        if (config.truncate_table)
        {
            run.sut.execute(String.format("truncate %s.%s;",
                                          run.schemaSpec.keyspace,
                                          run.schemaSpec.table),
                            EMPTY_BINDINGS);
        }
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
            run.sut.schemaChange(String.format("DROP TABLE %s.%s;",
                                               run.schemaSpec.keyspace,
                                               run.schemaSpec.table));
        }
    }

    protected void maybeReportErrors()
    {
        if (!errors.isEmpty())
            dumpStateToFile(run, errors);
    }

    public abstract CompletableFuture initAndStartAll() throws InterruptedException;

    public abstract void shutdown() throws InterruptedException;

    protected Runnable reportThrowable(Runnable runnable, CompletableFuture future)
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
        private final int checkRecentAfter;
        private final int checkAllAfter;

        public SequentialRunner(Run run,
                                int roundRobinValidatorThreads,
                                int checkRecentAfter,
                                int checkAllAfter)
        {
            super(run);

            this.executor = Executors.newScheduledThreadPool(roundRobinValidatorThreads + 1);
            this.shutdownExceutor = Executors.newSingleThreadScheduledExecutor();
            this.checkAllAfter = checkAllAfter;
            this.checkRecentAfter = checkRecentAfter;
        }

        public CompletableFuture initAndStartAll()
        {
            init();
            CompletableFuture future = new CompletableFuture();
            future.whenComplete((a, b) -> maybeReportErrors());

            shutdownExceutor.schedule(() -> {
                logger.info("Completed");
                // TODO: wait for the last full validation?
                future.complete(null);
            }, run.snapshot.run_time, run.snapshot.run_time_unit);

            executor.submit(reportThrowable(() -> {
                                                try
                                                {
                                                    run(run.visitorFactory.get(), run.clock,
                                                        () -> Thread.currentThread().isInterrupted() || future.isDone());
                                                }
                                                catch (Throwable t)
                                                {
                                                    future.completeExceptionally(t);
                                                }
                                            },
                                            future));

            return future;
        }

        void run(PartitionVisitor visitor,
                 OpSelectors.MonotonicClock clock,
                 BooleanSupplier exitCondition) throws ExecutionException, InterruptedException
        {
            while (!exitCondition.getAsBoolean())
            {
                long lts = clock.nextLts();
                visitor.visitPartition(lts);
                if (lts % checkRecentAfter == 0)
                    run.validator.validateRecentPartitions(100);

                if (lts % checkAllAfter == 0)
                    run.validator.validateAllPartitions(executor, exitCondition, 10).get();
            }
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
        public Runner make(Run run);
    }

    // TODO: this requires some significant improvement
    public static class ConcurrentRunner extends Runner
    {
        private final ScheduledExecutorService executor;
        private final ScheduledExecutorService shutdownExceutor;

        private final int writerThreads;
        private final int roundRobinValidators;
        private final int recentPartitionValidators;
        private final long runTime;
        private final TimeUnit runTimeUnit;

        public ConcurrentRunner(Run run,
                                int writerThreads,
                                int roundRobinValidators,
                                int recentPartitionValidators)
        {
            super(run);
            this.writerThreads = writerThreads;
            this.roundRobinValidators = roundRobinValidators;
            this.recentPartitionValidators = recentPartitionValidators;
            this.runTime = run.snapshot.run_time;
            this.runTimeUnit = run.snapshot.run_time_unit;
            this.executor = Executors.newScheduledThreadPool(this.writerThreads + this.roundRobinValidators + this.recentPartitionValidators);
            this.shutdownExceutor = Executors.newSingleThreadScheduledExecutor();
        }

        public CompletableFuture initAndStartAll() throws InterruptedException
        {
            init();
            CompletableFuture future = new CompletableFuture();
            future.whenComplete((a, b) -> maybeReportErrors());

            shutdownExceutor.schedule(() -> {
                logger.info("Completed");
                // TODO: wait for the last full validation?
                future.complete(null);
            }, runTime, runTimeUnit);

            BooleanSupplier exitCondition = () -> Thread.currentThread().isInterrupted() || future.isDone();
            for (int i = 0; i < writerThreads; i++)
            {
                executor.submit(reportThrowable(() -> run(run.visitorFactory.get(), run.clock, exitCondition),
                                                future));
            }

            scheduleValidateAllPartitions(run.validator, executor, future, roundRobinValidators);

            // N threads to validate recently written partitions
            for (int i = 0; i < recentPartitionValidators; i++)
            {
                executor.scheduleWithFixedDelay(reportThrowable(() -> {
                    // TODO: make recent partitions configurable
                    run.validator.validateRecentPartitions(100);
                }, future), 1000, 1, TimeUnit.MILLISECONDS);
            }

            return future;
        }

        void run(PartitionVisitor visitor,
                 OpSelectors.MonotonicClock clock,
                 BooleanSupplier exitCondition)
        {
            while (!exitCondition.getAsBoolean())
            {
                long lts = clock.nextLts();
                visitor.visitPartition(lts);
            }
        }

        void scheduleValidateAllPartitions(Validator validator, ExecutorService executor, CompletableFuture future, int roundRobinValidators)
        {
            validator.validateAllPartitions(executor, () -> Thread.currentThread().isInterrupted() || future.isDone(),
                                            roundRobinValidators)
                     .handle((v, err) -> {
                         if (err != null)
                         {
                             errors.add(err);
                             if (!future.isDone())
                                 future.completeExceptionally(err);
                         }

                         if (Thread.currentThread().isInterrupted() || future.isDone())
                             return null;

                         scheduleValidateAllPartitions(validator, executor, future, roundRobinValidators);
                         return null;
                     });
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

    private static void dumpStateToFile(Run run, List<Throwable> t)
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

            File config = new File("run.yaml");
            Configuration.ConfigurationBuilder builder = run.snapshot.unbuild();

            // overrride stateful components
            builder.setClock(run.clock.toConfig());
            builder.setModel(run.model.toConfig());

            try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(config))))
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