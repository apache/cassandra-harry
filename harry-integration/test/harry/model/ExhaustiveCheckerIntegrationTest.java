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

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.corruptor.AddExtraRowCorruptor;
import harry.corruptor.ChangeValueCorruptor;
import harry.corruptor.HideRowCorruptor;
import harry.corruptor.HideValueCorruptor;
import harry.corruptor.QueryResponseCorruptor;
import harry.corruptor.QueryResponseCorruptor.SimpleQueryResponseCorruptor;
import harry.generators.Surjections;
import harry.generators.distribution.Distribution;
import harry.model.sut.SystemUnderTest;
import harry.runner.PartitionVisitor;
import harry.runner.Query;
import harry.runner.Validator;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

public class ExhaustiveCheckerIntegrationTest extends IntegrationTestBase
{
    @Test
    public void testVerifyPartitionState()
    {
        Configuration config = sharedConfiguration(1).build();
        Run run = config.createRun();
        run.sut.schemaChange(run.schemaSpec.compile().cql());

        OpSelectors.MonotonicClock clock = run.clock;
        Validator validator = run.validator;
        PartitionVisitor partitionVisitor = run.visitorFactory.get();

        for (int i = 0; i < 2000; i++)
        {
            long lts = clock.nextLts();
            partitionVisitor.visitPartition(lts);
        }

        validator.validatePartition(0);
    }

    @Test
    public void testDetectsMissingRow()
    {
        negativeTest((run) -> {
                         SimpleQueryResponseCorruptor corruptor = new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                                                   run.clock,
                                                                                                   HideRowCorruptor::new);

                         Assert.assertTrue(corruptor.maybeCorrupt(Query.selectPartition(run.schemaSpec,
                                                                                        run.pdSelector.pd(0, run.schemaSpec),
                                                                                        false),
                                                                  run.sut));
                     },
                     (t) -> Assert.assertTrue(String.format("Throwable: %s\nCause: %s", t, t.getCause()),
                                              t.getCause() != null &&t.getCause().toString().contains(OpSelectors.OperationKind.WRITE.toString())));
    }

    @Test
    public void testDetectsExtraRow()
    {
        negativeTest((run) -> {
                         QueryResponseCorruptor corruptor = new AddExtraRowCorruptor(run.schemaSpec,
                                                                                     run.clock,
                                                                                     run.descriptorSelector);

                         Assert.assertTrue(corruptor.maybeCorrupt(Query.selectPartition(run.schemaSpec,
                                                                                        run.pdSelector.pd(0, run.schemaSpec),
                                                                                        false),
                                                                  run.sut));
                     },
                     (t) -> {
                         Assert.assertTrue(String.format("Throwable: %s\nCause: %s", t, t.getCause()),
                                              // TODO: this is not entirely correct. Right now, after registering a deletion followed by no writes,
                                              // we would continue going back in time and checking other operations, even though we don't have to do this.
                                              t.getCause().getMessage().contains("Modification should have been visible but was not") ||
                                              t.getCause().getMessage().contains("Observed unvalidated rows") ||
                                              t.getCause() != null && t.getCause().getMessage().contains("was never written"));
                     });
    }


    @Test
    public void testDetectsRemovedColumn()
    {
        negativeTest((run) -> {
                         SimpleQueryResponseCorruptor corruptor = new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                                                   run.clock,
                                                                                                   HideValueCorruptor::new);

                         Assert.assertTrue(corruptor.maybeCorrupt(Query.selectPartition(run.schemaSpec,
                                                                                        run.pdSelector.pd(0, run.schemaSpec),
                                                                                        false),
                                                                  run.sut));
                     },
                     (t) -> Assert.assertTrue(String.format("Throwable: %s\nCause: %s", t, t.getCause()),
                                              t.getCause() != null && t.getCause().getMessage().contains("Modification should have been visible but was not")));
    }


    @Test
    @Ignore
    public void testDetectsOverwrittenRow()
    {
        negativeTest((run) -> {
                         SimpleQueryResponseCorruptor corruptor = new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                                                   run.clock,
                                                                                                   ChangeValueCorruptor::new);

                         Assert.assertTrue(corruptor.maybeCorrupt(Query.selectPartition(run.schemaSpec,
                                                                                        run.pdSelector.pd(0, run.schemaSpec),
                                                                                        false),
                                                                  run.sut));
                     },
                     (t) -> Assert.assertTrue(String.format("Throwable: %s\nCause: %s", t, t.getCause()),
                                              t.getCause() != null && t.getCause().getMessage().contains("Modification should have been visible but was not.")));
    }


    static void negativeTest(Consumer<Run> corrupt, Consumer<Throwable> validate)
    {
        Configuration config = sharedConfiguration()
                               .setClusteringDescriptorSelector((rng, schemaSpec) -> {
                                   return new OpSelectors.DefaultDescriptorSelector(rng,
                                                                                    new OpSelectors.ColumnSelectorBuilder().forAll(schemaSpec.regularColumns.size()).build(),
                                                                                    Surjections.pick(OpSelectors.OperationKind.DELETE_COLUMN,
                                                                                                     OpSelectors.OperationKind.DELETE_ROW,
                                                                                                     OpSelectors.OperationKind.WRITE),
                                                                                    new Distribution.ConstantDistribution(10),
                                                                                    new Distribution.ConstantDistribution(10),
                                                                                    100);
                               })
                               .build();
        Run run = config.createRun();
        run.sut.schemaChange(run.schemaSpec.compile().cql());
        OpSelectors.MonotonicClock clock = run.clock;
        Validator validator = run.validator;
        PartitionVisitor partitionVisitor = run.visitorFactory.get();

        for (int i = 0; i < 200; i++)
        {
            long lts = clock.nextLts();
            partitionVisitor.visitPartition(lts);
        }

        corrupt.accept(run);

        try
        {
            validator.validatePartition(0);
            Assert.fail("Should've thrown");
        }
        catch (Throwable t)
        {
            validate.accept(t);
        }
    }

    @Test
    public void testLocalOnlyExecution()
    {
        LocalOnlySut localOnlySut = new LocalOnlySut();
        Configuration config = sharedConfiguration()
                               .setClusteringDescriptorSelector((builder) -> {
                                   builder.setOperationKindWeights(new Configuration.OperationKindSelectorBuilder()
                                                                   .addWeight(OpSelectors.OperationKind.DELETE_ROW, 80)
                                                                   .addWeight(OpSelectors.OperationKind.DELETE_COLUMN, 10)
                                                                   .addWeight(OpSelectors.OperationKind.WRITE, 10)
                                                                   .build());
                               })
                               .setSUT(() -> localOnlySut)
                               .build();

        Run run = config.createRun();
        run.sut.schemaChange(run.schemaSpec.compile().cql());

        OpSelectors.MonotonicClock clock = run.clock;
        Validator validator = run.validator;
        PartitionVisitor partitionVisitor = run.visitorFactory.get();

        localOnlySut.localOnly(() -> {
            for (int i = 0; i < 5; i++)
            {
                long lts = clock.nextLts();
                partitionVisitor.visitPartition(lts);
            }
        });

        validator.validatePartition(0);
    }

    private static class LocalOnlySut implements SystemUnderTest
    {
        private boolean localOnly = false;
        private int counter = 0;

        public boolean isShutdown()
        {
            return cluster.size() == 0;
        }

        public void shutdown()
        {
            cluster.close();
        }

        public void schemaChange(String statement)
        {
            cluster.schemaChange(statement);
        }

        public Object[][] execute(String statement, Object... bindings)
        {
            if (localOnly)
                return cluster.get((counter++) % cluster.size() + 1).executeInternal(statement, bindings);
            else
                return cluster.coordinator((counter++) % cluster.size() + 1).execute(statement, ConsistencyLevel.ALL, bindings);
        }

        public CompletableFuture<Object[][]> executeAsync(String statement, Object... bindings)
        {
            return CompletableFuture.supplyAsync(() -> execute(statement, bindings));
        }

        public void localOnly(Runnable r)
        {
            localOnly = true;
            r.run();
            localOnly = false;
        }
    }
}