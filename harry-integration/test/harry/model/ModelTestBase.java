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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.runner.LoggingPartitionVisitor;
import harry.runner.MutatingRowVisitor;
import harry.runner.PartitionVisitor;
import harry.runner.SinglePartitionValidator;

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

    abstract Configuration.ModelConfiguration modelConfiguration();

    protected PartitionVisitor validator(Run run)
    {
        return new SinglePartitionValidator(100, run, modelConfiguration());
    }

    void negativeTest(Function<Run, Boolean> corrupt, BiConsumer<Throwable, Run> validate, int counter, SchemaSpec schemaSpec)
    {
        Configuration config = sharedConfiguration(counter, schemaSpec)
                               .setClusteringDescriptorSelector((builder) -> {
                                   builder.setNumberOfModificationsDistribution(new Configuration.ConstantDistributionConfig(10))
                                          .setRowsPerModificationDistribution(new Configuration.ConstantDistributionConfig(10))
                                          .setMaxPartitionSize(100)
                                          .setOperationKindWeights(new Configuration.OperationKindSelectorBuilder()
                                                                   .addWeight(OpSelectors.OperationKind.DELETE_ROW, 1)
                                                                   .addWeight(OpSelectors.OperationKind.DELETE_COLUMN, 1)
                                                                   .addWeight(OpSelectors.OperationKind.DELETE_RANGE, 1)
                                                                   .addWeight(OpSelectors.OperationKind.DELETE_SLICE, 1)
                                                                   .addWeight(OpSelectors.OperationKind.WRITE, 96)
                                                                   .build());
                               })
                               .build();

        Run run = config.createRun();
        beforeEach();
        run.sut.schemaChange(run.schemaSpec.compile().cql());
        OpSelectors.MonotonicClock clock = run.clock;

        PartitionVisitor validator = validator(run);
        PartitionVisitor partitionVisitor = new LoggingPartitionVisitor(run, MutatingRowVisitor::new);

        for (int i = 0; i < 200; i++)
        {
            long lts = clock.nextLts();
            partitionVisitor.visitPartition(lts);
        }

        validator.visitPartition(0);

        if (!corrupt.apply(run))
        {
            System.out.println("Could not corrupt");
            return;
        }

        try
        {
            validator.visitPartition(0);
        }
        catch (Throwable t)
        {
            validate.accept(t, run);
        }
    }
}