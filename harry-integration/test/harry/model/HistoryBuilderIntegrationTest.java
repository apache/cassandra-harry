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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.dsl.HistoryBuilder;
import harry.operations.CompiledStatement;
import harry.operations.Query;
import harry.operations.QueryGenerator;
import harry.reconciler.Reconciler;
import harry.util.TestRunner;
import harry.visitors.MutatingVisitor;
import harry.visitors.MutatingRowVisitor;
import harry.visitors.ReplayingVisitor;

public class HistoryBuilderIntegrationTest extends ModelTestBase
{
    public Configuration.ConfigurationBuilder configuration(long seed, SchemaSpec schema)
    {
        return super.configuration(seed, schema)
                    .setPartitionDescriptorSelector((ignore) -> new HistoryBuilder.PdSelector())
                    // TODO: ideally, we want a custom/tailored clustering descriptor selector
                    .setClusteringDescriptorSelector((builder) -> builder.setNumberOfModificationsDistribution(new Configuration.ConstantDistributionConfig(100_000))
                                                                         .setRowsPerModificationDistribution(new Configuration.ConstantDistributionConfig(100_000)));
    }

    @Test
    public void simpleDSLTest()
    {
        Supplier<SchemaSpec> supplier = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        for (int i = 0; i < SchemaGenerators.DEFAULT_RUNS; i++)
        {
            SchemaSpec schema = supplier.get();
            Configuration config = configuration(i, schema).build();

            Run run = config.createRun();
            beforeEach();
            run.sut.schemaChange(schema.compile().cql());
            HistoryBuilder history = new HistoryBuilder(run);

            Set<Long> pds = new HashSet<>();

            for (int j = 0; j < 5; j++)
            {
                history.nextPartition()
                       .simultaneously()
                         .batch()
                           .insert()
                           .delete()
                           .finish()
                         .insert()
                         .finish()
                       .nextPartition()
                       .sequentially()
                       .randomOrder()
                         .batch()
                           .insert()
                           .delete()
                           .finish()
                         .updates(5)
                         .partitionDelete()
                         .finish()
                       .nextPartition()
                       .sequentially()
                       .randomOrder()
                         .batch()
                           .insert()
                           .delete()
                           .finish()
                         .updates(10)
                           .partitionDelete()
                           .finish();

                run.tracker.onLtsStarted((long lts) -> {
                    pds.add(run.pdSelector.pd(lts, run.schemaSpec));
                });
                ReplayingVisitor visitor = history.visitor(run);

                visitor.replayAll();

                Model model = new QuiescentChecker(run, new Reconciler(run,
                                                                       history::visitor));
                QueryGenerator.TypedQueryGenerator queryGenerator = new QueryGenerator.TypedQueryGenerator(run);
                Assert.assertFalse(pds.isEmpty());
                for (Long pd : pds)
                {
                    model.validate(Query.selectPartition(run.schemaSpec, pd,false));

                    int lts = new Random().nextInt((int) run.clock.peek());
                    for (int k = 0; k < 3; k++)
                        queryGenerator.inflate(lts, k);
                }
            }
        }
    }

    @Test
    public void testHistoryBuilder() throws Throwable
    {
        Supplier<SchemaSpec> supplier = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        for (int i = 0; i < SchemaGenerators.DEFAULT_RUNS; i++)
        {
            SchemaSpec schema = supplier.get();
            Configuration config = configuration(1L, schema).build();
            Run run = config.createRun();

            beforeEach();
            run.sut.schemaChange(schema.compile().cql());

            TestRunner.test((rng) -> {
                                HistoryBuilderTest.Counts counts = new HistoryBuilderTest.Counts();
                                counts.randomOrder = rng.nextBoolean();
                                counts.simultaneously = rng.nextBoolean();
                                counts.partitionDeletion = rng.nextInt(1, 10);
                                counts.update = rng.nextInt(1, 10);
                                counts.insert = rng.nextInt(1, 10);
                                counts.delete = rng.nextInt(1, 10);
                                counts.rangeDelete = rng.nextInt(1, 10);
                                counts.sliceDelete = rng.nextInt(1, 10);
                                counts.columnDelete = rng.nextInt(1, 10);
                                return counts;
                            },
                            () -> new HistoryBuilder(run),
                            (sut, counts) -> {
                                counts.apply(sut);
                                return sut;
                            },
                            (sut) -> {
                                Set<Long> pds = new HashSet<>();
                                ReplayingVisitor visitor = sut.visitor(new MutatingVisitor.MutatingVisitExecutor(run,
                                                                                                                 new MutatingRowVisitor(run)
                                                                                                                {
                                                                                                                    public CompiledStatement perform(OpSelectors.OperationKind op, long lts, long pd, long cd, long opId)
                                                                                                                    {
                                                                                                                        pds.add(pd);
                                                                                                                        return super.perform(op, lts, pd, cd, opId);
                                                                                                                    }
                                                                                                                }));

                                visitor.replayAll();

                                Model model = new QuiescentChecker(run, new Reconciler(run,
                                                                                       sut::visitor));
                                QueryGenerator.TypedQueryGenerator queryGenerator = new QueryGenerator.TypedQueryGenerator(run);
                                for (Long pd : pds)
                                {
                                    model.validate(Query.selectPartition(run.schemaSpec,
                                                                         pd,
                                                                         false));
                                    model.validate(Query.selectPartition(run.schemaSpec,
                                                                         pd,
                                                                         true));
                                    int lts = new Random().nextInt((int) run.clock.peek());
                                    for (int k = 0; k < 3; k++)
                                        queryGenerator.inflate(lts, k);
                                }
                            });
        }
    }

    Configuration.ModelConfiguration modelConfiguration()
    {
        return new Configuration.QuiescentCheckerConfig();
    }
}