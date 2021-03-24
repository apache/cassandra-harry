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
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.corruptor.AddExtraRowCorruptor;
import harry.corruptor.ChangeValueCorruptor;
import harry.corruptor.HideRowCorruptor;
import harry.corruptor.HideValueCorruptor;
import harry.corruptor.QueryResponseCorruptor;
import harry.corruptor.QueryResponseCorruptor.SimpleQueryResponseCorruptor;
import harry.ddl.SchemaGenerators;
import harry.model.sut.InJvmSut;
import harry.model.sut.SystemUnderTest;
import harry.runner.MutatingPartitionVisitor;
import harry.runner.MutatingRowVisitor;
import harry.runner.PartitionVisitor;
import harry.runner.Query;
import harry.runner.SinglePartitionValidator;

public class ExhaustiveCheckerIntegrationTest extends ModelTestBase
{
    @Test
    public void testVerifyPartitionState()
    {
        Supplier<Configuration.ConfigurationBuilder> gen = sharedConfiguration();

        for (int cnt = 0; cnt < SchemaGenerators.DEFAULT_RUNS; cnt++)
        {
            Configuration config = gen.get().build();
            Run run = config.createRun();
            run.sut.schemaChange(run.schemaSpec.compile().cql());
            OpSelectors.MonotonicClock clock = run.clock;

            SinglePartitionValidator validator = new SinglePartitionValidator(100, run, modelConfiguration());
            PartitionVisitor partitionVisitor = new MutatingPartitionVisitor(run, MutatingRowVisitor::new);

            for (int i = 0; i < 2000; i++)
            {
                long lts = clock.nextLts();
                partitionVisitor.visitPartition(lts);
            }

            validator.visitPartition(0);
        }
    }

    @Test
    public void testDetectsMissingRow()
    {
        negativeTest((run) -> {
                         SimpleQueryResponseCorruptor corruptor = new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                                                   run.clock,
                                                                                                   HideRowCorruptor::new);

                         return corruptor.maybeCorrupt(Query.selectPartition(run.schemaSpec,
                                                                             run.pdSelector.pd(0, run.schemaSpec),
                                                                             false),
                                                       run.sut);
                     },
                     (t, run) -> Assert.assertTrue(String.format("Throwable: %s\nCause: %s", t, t.getCause()),
                                                   t.getCause() != null && t.getCause().toString().contains(OpSelectors.OperationKind.WRITE.toString())));
    }

    @Test
    public void testDetectsExtraRow()
    {
        negativeTest((run) -> {
                         QueryResponseCorruptor corruptor = new AddExtraRowCorruptor(run.schemaSpec,
                                                                                     run.clock,
                                                                                     run.descriptorSelector);

                         return corruptor.maybeCorrupt(Query.selectPartition(run.schemaSpec,
                                                                             run.pdSelector.pd(0, run.schemaSpec),
                                                                             false),
                                                       run.sut);
                     },
                     (t, run) -> {
                         Assert.assertTrue(String.format("Throwable: %s\nCause: %s", t, t.getCause()),
                                           // TODO: this is not entirely correct. Right now, after registering a deletion followed by no writes,
                                           // we would continue going back in time and checking other operations, even though we don't have to do this.
                                           t.getCause().getMessage().contains("Modification should have been visible but was not") ||
                                           // TODO: this is not entirely correct, either. This row is, in fact, present in both dataset _and_
                                           // in the model, it's just there might be _another_ row right in front of it.
                                           t.getCause().getMessage().contains("expected row not to be visible") ||
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

                         return corruptor.maybeCorrupt(Query.selectPartition(run.schemaSpec,
                                                                             run.pdSelector.pd(0, run.schemaSpec),
                                                                             false),
                                                       run.sut);
                     },
                     (t, run) -> Assert.assertTrue(String.format("Throwable: %s\nCause: %s", t, t.getCause()),
                                                   t.getCause() != null && t.getCause().getMessage().contains("Modification should have been visible but was not")));
    }


    @Test
    public void testDetectsOverwrittenRow()
    {
        negativeTest((run) -> {
                         SimpleQueryResponseCorruptor corruptor = new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                                                   run.clock,
                                                                                                   ChangeValueCorruptor::new);

                         return corruptor.maybeCorrupt(Query.selectPartition(run.schemaSpec,
                                                                             run.pdSelector.pd(0, run.schemaSpec),
                                                                             false),
                                                       run.sut);
                     },
                     (t, run) -> Assert.assertTrue(String.format("Throwable: %s\nCause: %s", t, t.getCause()),
                                                   t.getCause() != null && t.getCause().getMessage().contains("Modification should have been visible but was not.")));
    }

    @Test
    public void testLocalOnlyExecution()
    {
        LocalOnlySut localOnlySut = new LocalOnlySut();

        Supplier<Configuration.ConfigurationBuilder> gen = sharedConfiguration();

        for (int cnt = 0; cnt < SchemaGenerators.DEFAULT_RUNS; cnt++)
        {
            Configuration config = gen.get()
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

            PartitionVisitor partitionVisitor = new MutatingPartitionVisitor(run, MutatingRowVisitor::new);

            localOnlySut.localOnly(() -> {
                for (int i = 0; i < 5; i++)
                {
                    long lts = clock.nextLts();
                    partitionVisitor.visitPartition(lts);
                }
            });

            SinglePartitionValidator validator = new SinglePartitionValidator(100, run, ExhaustiveChecker::new);
            validator.visitPartition(0);
        }
    }

    Configuration.ModelConfiguration modelConfiguration()
    {
        return new Configuration.ExhaustiveCheckerConfig();
    }

    public static class LocalOnlySut implements SystemUnderTest
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

        public Object[][] execute(String statement, ConsistencyLevel cl, Object... bindings)
        {
            if (localOnly)
                return cluster.get((counter++) % cluster.size() + 1).executeInternal(statement, bindings);
            else
                return cluster.coordinator((counter++) % cluster.size() + 1).execute(statement, InJvmSut.toApiCl(ConsistencyLevel.ALL), bindings);
        }

        public CompletableFuture<Object[][]> executeAsync(String statement, ConsistencyLevel cl, Object... bindings)
        {
            return CompletableFuture.supplyAsync(() -> execute(statement, cl, bindings));
        }

        public void localOnly(Runnable r)
        {
            localOnly = true;
            r.run();
            localOnly = false;
        }
    }
}