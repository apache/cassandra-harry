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

package harry.model;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.visitors.LoggingVisitor;
import harry.visitors.MutatingRowVisitor;
import harry.visitors.Visitor;
import harry.runner.Runner;
import harry.visitors.SingleValidator;

public abstract class ModelTestBase extends IntegrationTestBase
{
    void negativeTest(Function<Run, Boolean> corrupt, BiConsumer<Throwable, Run> validate)
    {
        Supplier<SchemaSpec> supplier = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        for (int i = 0; i < SchemaGenerators.DEFAULT_RUNS; i++)
        {
            SchemaSpec schema = supplier.get();
            negativeTest(corrupt, validate, i, schema);
        }
    }

    void negativeIntegrationTest(Model.ModelFactory factory) throws Throwable
    {
        Supplier<SchemaSpec> supplier = SchemaGenerators.progression(1);
        for (int i = 0; i < SchemaGenerators.DEFAULT_RUNS; i++)
        {
            SchemaSpec schema = supplier.get();
            Configuration.ConfigurationBuilder builder = configuration(i, schema);
            builder.setClock(new Configuration.ApproximateMonotonicClockConfiguration((int) TimeUnit.MINUTES.toMillis(10),
                                                                                      1, TimeUnit.SECONDS))
                   .setRunTime(1, TimeUnit.MINUTES)
                   .setCreateSchema(false)
                   .setDropSchema(false)
                   .setRunner(new Configuration.SequentialRunnerConfig(Arrays.asList(new Configuration.LoggingVisitorConfiguration(new Configuration.MutatingRowVisitorConfiguration()),
                                                                                     new Configuration.RecentPartitionsValidatorConfiguration(10, 10, 1, factory::make),
                                                                                     new Configuration.AllPartitionsValidatorConfiguration(10, 10, factory::make))));
            Runner runner = builder.build().createRunner();
            try
            {
                Run run = runner.getRun();
                beforeEach();
                run.sut.schemaChange(run.schemaSpec.compile().cql());

                runner.initAndStartAll().get(2, TimeUnit.MINUTES);
            }
            catch (Throwable t)
            {
                throw t;
            }
            finally
            {
                runner.shutdown();
            }
        }
    }

    abstract Configuration.ModelConfiguration modelConfiguration();

    protected Visitor validator(Run run)
    {
        return new SingleValidator(100, run , modelConfiguration());
    }

    public Configuration.ConfigurationBuilder configuration(long seed, SchemaSpec schema)
    {
        return sharedConfiguration(seed, schema);
    }

    void negativeTest(Function<Run, Boolean> corrupt, BiConsumer<Throwable, Run> validate, int counter, SchemaSpec schemaSpec)
    {
        Configuration config = configuration(counter, schemaSpec).build();

        Run run = config.createRun();
        beforeEach();
        run.sut.schemaChange(run.schemaSpec.compile().cql());
        System.out.println(run.schemaSpec.compile().cql());
        OpSelectors.MonotonicClock clock = run.clock;

        Visitor validator = validator(run);
        Visitor visitor = new LoggingVisitor(run, MutatingRowVisitor::new);

        for (int i = 0; i < 20000; i++)
        {
            long lts = clock.nextLts();
            visitor.visit(lts);
        }

        validator.visit(0);

        if (!corrupt.apply(run))
        {
            System.out.println("Could not corrupt");
            return;
        }

        try
        {
            validator.visit(0);
        }
        catch (Throwable t)
        {
            validate.accept(t, run);
        }
    }
}

