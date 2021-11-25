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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Configuration;
import harry.core.Run;

public class StagedRunner extends Runner.TimedRunner
{
    public static final String TYPE = "staged";

    public static void register()
    {
        Configuration.registerSubtypes(StagedRunner.StagedRunnerConfig.class);
    }
    
    private static final Logger logger = LoggerFactory.getLogger(StagedRunner.class);
    
    private final List<Configuration.RunnerConfiguration> runnerFactories;
    private final List<Runner> stages;

    public StagedRunner(Run run,
                        Configuration config,
                        List<Configuration.RunnerConfiguration> runnerFactories,
                        long runtime, TimeUnit runtimeUnit)
    {
        super(run, config, 1, runtime, runtimeUnit);
        this.runnerFactories = runnerFactories;
        this.stages = new CopyOnWriteArrayList<>();
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    protected CompletableFuture<?> start(boolean reportErrors, BooleanSupplier parentExit)
    {
        CompletableFuture<?> future = new CompletableFuture<>();

        if (reportErrors)
        {
            future.whenComplete((a, b) ->
            {
                collectErrors();
                maybeReportErrors();
            });
        }

        AtomicBoolean terminated = new AtomicBoolean(false);
        scheduleTermination(terminated);
        
        BooleanSupplier exit = () -> Thread.currentThread().isInterrupted() || future.isDone()
                                     || terminated.get() || parentExit.getAsBoolean();

        executor.submit(() ->
        {
            for (Configuration.RunnerConfiguration runnerConfig : runnerFactories)
            {
                try
                {
                    Runner runner = runnerConfig.make(run, config);
                    if (runner instanceof StagedRunner)
                        throw new IllegalArgumentException("StagedRunner can not be nested inside of a StagedRunner");
                    stages.add(runner);
                }
                catch (Throwable t)
                {
                    future.completeExceptionally(t);
                }
            }

            while (!exit.getAsBoolean())
            {
                logger.info("Starting next staged run...");
                int stage = 1;

                for (Runner runner : stages)
                {
                    if (exit.getAsBoolean())
                        break;

                    try
                    {
                        logger.info("Starting stage {}: {}...", stage++, runner.type());
                        runner.start(false, exit).get();

                        if (!terminated.get())
                            logger.info("...stage complete.");

                        // Wait for any previous runners to settle down...
                        while (run.tracker.maxConsecutiveFinished() != run.tracker.maxStarted())
                        {
                            TimeUnit.SECONDS.sleep(1);
                            logger.warn("Waiting for any previous runners to settle down: {}",
                                        run.tracker.toString());
                        }

                    }
                    catch (Throwable t)
                    {
                        future.completeExceptionally(t);
                        break;
                    }
                }

                if (!terminated.get())
                    logger.info("All stages complete!");
            }

            future.complete(null);
        });

        return future;
    }

    @Override
    public void shutdown() throws InterruptedException
    {
        logger.info("Shutting down...");

        for (Runner runner : stages)
        {
            runner.shutDownVisitors();
            runner.shutDownExecutors();
        }

        shutDownExecutors();
        teardown();
    }

    @Override
    protected void shutDownVisitors()
    {
        // Visitors are shut down in the sub-runners.    
    }

    private void collectErrors()
    {
        for (Runner runner : stages)
        {
            errors.addAll(runner.errors);
        }
    }

    @JsonTypeName(TYPE)
    public static class StagedRunnerConfig implements Configuration.RunnerConfiguration
    {
        @JsonProperty(value = "stages")
        public final List<Configuration.RunnerConfiguration> runnerFactories;

        public final long run_time;
        public final TimeUnit run_time_unit;

        @JsonCreator
        public StagedRunnerConfig(@JsonProperty(value = "stages") List<Configuration.RunnerConfiguration> stages,
                                  @JsonProperty(value = "run_time", defaultValue = "2") long runtime,
                                  @JsonProperty(value = "run_time_unit", defaultValue = "HOURS") TimeUnit runtimeUnit)
        {
            this.runnerFactories = stages;
            this.run_time = runtime;
            this.run_time_unit = runtimeUnit;
        }

        @Override
        public Runner make(Run run, Configuration config)
        {
            return new StagedRunner(run, config, runnerFactories, run_time, run_time_unit);
        }
    }
}
