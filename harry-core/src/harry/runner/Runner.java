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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Configuration;
import harry.core.Run;
import harry.visitors.Visitor;

public abstract class Runner
{
    private static final Logger logger = LoggerFactory.getLogger(Runner.class);

    protected final Run run;
    protected final Configuration config;
    protected final ScheduledExecutorService executor;

    // If there's an error, there's a good chance we're going to hit it more than once
    // since we have multiple concurrent checkers running
    protected final CopyOnWriteArrayList<Throwable> errors;

    public Runner(Run run, Configuration config, int concurrency)
    {
        this.run = run;
        this.config = config;
        this.errors = new CopyOnWriteArrayList<>();
        this.executor = Executors.newScheduledThreadPool(concurrency);
    }

    public Run getRun()
    {
        return run;
    }

    public void init()
    {
        if (config.create_schema)
        {
            if (config.keyspace_ddl == null)
                run.sut.schemaChange("CREATE KEYSPACE IF NOT EXISTS " + run.schemaSpec.keyspace + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
            else
                run.sut.schemaChange(config.keyspace_ddl);

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

    public CompletableFuture<?> initAndStartAll()
    {
        init();
        return start();
    }

    public abstract String type();
        
    public abstract void shutdown() throws InterruptedException;
    
    protected CompletableFuture<?> start()
    {
        return start(true, () -> false);
    }
    
    protected abstract void shutDownVisitors();

    @SuppressWarnings("ResultOfMethodCallIgnored")
    protected void shutDownExecutors() throws InterruptedException
    {
        executor.shutdownNow();
        executor.awaitTermination(1, TimeUnit.MINUTES);
    }
    
    protected abstract CompletableFuture<?> start(boolean reportErrors, BooleanSupplier parentExit);

    protected Runnable reportThrowable(Runnable runnable, CompletableFuture<?> future)
    {
        return () ->
        {
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

    public interface RunnerFactory
    {
        Runner make(Run run, Configuration config);
    }

    public abstract static class TimedRunner extends Runner
    {
        public final long runtime;
        public final TimeUnit runtimeUnit;
        
        protected final ScheduledExecutorService shutdownExecutor;

        public TimedRunner(Run run, Configuration config, int concurrency, long runtime, TimeUnit runtimeUnit)
        {
            super(run, config, concurrency);

            this.shutdownExecutor = Executors.newSingleThreadScheduledExecutor();
            this.runtime = runtime;
            this.runtimeUnit = runtimeUnit;
        }

        protected ScheduledFuture<?> scheduleTermination(AtomicBoolean terminated)
        {
            return shutdownExecutor.schedule(() ->
                                             {
                                                 logger.info("Runner has reached configured runtime. Stopping...");
                                                 // TODO: wait for the last full validation?
                                                 terminated.set(true);
                                             },
                                             runtime,
                                             runtimeUnit);
        }

        @Override
        @SuppressWarnings("ResultOfMethodCallIgnored")
        protected void shutDownExecutors() throws InterruptedException
        {
            shutdownExecutor.shutdownNow();
            shutdownExecutor.awaitTermination(1, TimeUnit.MINUTES);
            executor.shutdownNow();
            executor.awaitTermination(1, TimeUnit.MINUTES);
        }
    }

    public static class SingleVisitRunner extends Runner
    {
        private final List<Visitor> visitors;

        public SingleVisitRunner(Run run,
                                Configuration config,
                                List<? extends Visitor.VisitorFactory> visitorFactories)
        {
            super(run, config, 1);
            this.visitors = visitorFactories.stream().map(factory -> factory.make(run)).collect(Collectors.toList());
        }

        @Override
        public String type()
        {
            return "single";
        }

        @Override
        protected CompletableFuture<?> start(boolean reportErrors, BooleanSupplier parentExit)
        {
            CompletableFuture<?> future = new CompletableFuture<>();

            if (reportErrors)
                future.whenComplete((a, b) -> maybeReportErrors());

            executor.submit(reportThrowable(() -> run(visitors, future, parentExit), future));
            return future;
        }

        private void run(List<Visitor> visitors, CompletableFuture<?> future, BooleanSupplier parentExit)
        {
            for (Visitor value: visitors)
            {
                if (parentExit.getAsBoolean())
                    break;

                value.visit();
            }

            future.complete(null);
        }

        @Override
        public void shutdown() throws InterruptedException
        {
            logger.info("Shutting down...");
            shutDownVisitors();

            // we need to wait for all threads that use schema to stop before we can tear down and drop the table
            shutDownExecutors();

            teardown();
        }

        @Override
        protected void shutDownVisitors()
        {
            shutDownVisitors(visitors);
        }
    }

    public static class SequentialRunner extends TimedRunner
    {
        protected final List<Visitor> visitors;

        public SequentialRunner(Run run,
                                Configuration config,
                                List<? extends Visitor.VisitorFactory> visitorFactories,
                                long runtime, TimeUnit runtimeUnit)
        {
            super(run, config, 1, runtime, runtimeUnit);

            this.visitors = visitorFactories.stream().map(factory -> factory.make(run)).collect(Collectors.toList());
        }

        @Override
        public String type()
        {
            return "sequential";
        }

        @Override
        protected CompletableFuture<?> start(boolean reportErrors, BooleanSupplier parentExit)
        {
            CompletableFuture<?> future = new CompletableFuture<>();
            
            if (reportErrors)
                future.whenComplete((a, b) -> maybeReportErrors());

            AtomicBoolean terminated = new AtomicBoolean(false);
            scheduleTermination(terminated);
            BooleanSupplier exit = () -> Thread.currentThread().isInterrupted() || future.isDone() 
                                         || terminated.get() || parentExit.getAsBoolean();

            executor.submit(reportThrowable(() -> run(visitors, future, exit), future));
            return future;
        }

        protected void run(List<Visitor> visitors,
                           CompletableFuture<?> future,
                           BooleanSupplier exit)
        {
            while (!exit.getAsBoolean())
            {
                for (Visitor visitor: visitors)
                {
                    if (exit.getAsBoolean())
                        break;
                    visitor.visit();
                }
            }

            future.complete(null);
        }

        @Override
        public void shutdown() throws InterruptedException
        {
            logger.info("Shutting down...");
            shutDownVisitors();

            // we need to wait for all threads that use schema to stop before we can tear down and drop the table
            shutDownExecutors();
            
            teardown();
        }

        @Override
        protected void shutDownVisitors()
        {
            shutDownVisitors(visitors);
        }
    }

    // TODO: this requires some significant improvement
    public static class ConcurrentRunner extends TimedRunner
    {
        private final List<List<Visitor>> perThreadVisitors;
        private final int concurrency;

        public ConcurrentRunner(Run run,
                                Configuration config,
                                int concurrency,
                                List<? extends Visitor.VisitorFactory> visitorFactories,
                                long runtime, TimeUnit runtimeUnit)
        {
            super(run, config, concurrency, runtime, runtimeUnit);

            this.concurrency = concurrency;
            this.perThreadVisitors = new ArrayList<>(concurrency);

            for (int i = 0; i < concurrency; i++)
            {
                List<Visitor> visitors = new ArrayList<>();

                for (Visitor.VisitorFactory factory : visitorFactories)
                    visitors.add(factory.make(run));

                perThreadVisitors.add(visitors);
            }
        }

        @Override
        public String type()
        {
            return "concurrent";
        }

        @Override
        protected CompletableFuture<?> start(boolean reportErrors, BooleanSupplier parentExit)
        {
            CompletableFuture<?> future = new CompletableFuture<>();
            
            if (reportErrors)
                future.whenComplete((a, b) -> maybeReportErrors());

            AtomicBoolean terminated = new AtomicBoolean(false);
            scheduleTermination(terminated);
            BooleanSupplier exit = () -> Thread.currentThread().isInterrupted() || future.isDone() 
                                         || terminated.get() || parentExit.getAsBoolean();
            
            AtomicInteger liveCount = new AtomicInteger(0);
            
            for (int i = 0; i < concurrency; i++)
            {
                List<Visitor> visitors = perThreadVisitors.get(i);
                executor.submit(reportThrowable(() -> run(visitors, future, exit, liveCount), future));
            }

            return future;
        }

        private void run(List<Visitor> visitors,
                         CompletableFuture<?> future,
                         BooleanSupplier exit,
                         AtomicInteger liveCount)
        {
            liveCount.incrementAndGet();
            
            while (!exit.getAsBoolean())
            {
                for (Visitor visitor : visitors)
                {
                    if (exit.getAsBoolean())
                        break;

                    visitor.visit();
                }
            }
            
            // If we're the last worker still running, complete the future....
            if (liveCount.decrementAndGet() == 0)
                future.complete(null);
        }

        @Override
        public void shutdown() throws InterruptedException
        {
            logger.info("Shutting down...");
            shutDownVisitors();

            // we need to wait for all threads that use schema to stop before we can tear down and drop the table
            shutDownExecutors();

            teardown();
        }

        @Override
        protected void shutDownVisitors()
        {
            shutDownVisitors(perThreadVisitors.stream().flatMap(Collection::stream).collect(Collectors.toList()));
        }
    }

    protected static void shutDownVisitors(List<Visitor> visitors)
    {
        Throwable error = null;
        
        for (Visitor visitor : visitors)
        {
            try
            {
                visitor.shutdown();
            }
            catch (InterruptedException e)
            {
                if (error != null)
                    error.addSuppressed(e);
                else
                    error = e;
            }
        }
        
        if (error != null)
            logger.warn("Failed to shut down all visitors!", error);
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

            // override stateful components
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
            logger.error("Caught an error while trying to dump to file", e);
            try
            {
                File f = new File("tmp.dump");
                
                if (!f.createNewFile())
                    logger.info("File {} already exists. Appending...", f);
                
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