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
import harry.runner.Runner;
import harry.visitors.SingleValidator;
import harry.visitors.Visitor;

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

    void negativeIntegrationTest(Configuration.RunnerConfiguration runnerConfig) throws Throwable
    {
        Supplier<SchemaSpec> supplier = SchemaGenerators.progression(1);
        for (int i = 0; i < SchemaGenerators.DEFAULT_RUNS; i++)
        {
            SchemaSpec schema = supplier.get();
            Configuration.ConfigurationBuilder builder = configuration(i, schema);

            builder.setClock(new Configuration.ApproximateMonotonicClockConfiguration((int) TimeUnit.MINUTES.toMillis(10),
                                                                                      1, TimeUnit.SECONDS))
                   .setCreateSchema(false)
                   .setDropSchema(false)
                   .setRunner(runnerConfig);

            Configuration config = builder.build();
            Runner runner = config.createRunner();
            
            try
            {
                Run run = runner.getRun();
                beforeEach();
                run.sut.schemaChange(run.schemaSpec.compile().cql());

                runner.initAndStartAll().get(4, TimeUnit.MINUTES);
            }
            finally
            {
                runner.shutdown();
            }
        }
    }

    abstract Configuration.ModelConfiguration modelConfiguration();

    protected SingleValidator validator(Run run)
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

        Visitor visitor = new LoggingVisitor(run, MutatingRowVisitor::new);

        for (int i = 0; i < 20000; i++)
            visitor.visit();

        SingleValidator validator = validator(run);
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

