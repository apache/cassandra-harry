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

import java.util.function.Consumer;

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
import harry.generators.Surjections;
import harry.generators.distribution.Distribution;
import harry.runner.PartitionVisitor;
import harry.runner.Query;
import harry.runner.Validator;

public class QuiescentCheckerIntegrationTest extends IntegrationTestBase
{
    @Test
    public void testNormalCondition()
    {
        negativeTest((run) -> {},
                     Assert::assertNull);
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
                     (t) -> {
                         String expected = "Expected results to have the same number of results, but expected result iterator has more results";
                         Assert.assertTrue(String.format("Exception string mismatch.\nExpected error: %s.\nActual error: %s", expected, t.getMessage()),
                                           t.getMessage().contains(expected));
                     });
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
                         String expected = "Found a row in the model that is not present in the resultset";
                         Assert.assertTrue(String.format("Exception string mismatch.\nExpected error: %s.\nActual error: %s", expected, t.getMessage()),
                                           t.getMessage().contains(expected));
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
                     (t) -> {
                         String expected = "Returned row state doesn't match the one predicted by the model";
                         Assert.assertTrue(String.format("Exception string mismatch.\nExpected error: %s.\nActual error: %s", expected, t.getMessage()),
                                           t.getMessage().contains(expected));
                     });
    }


    @Test
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
                     (t) -> {
                         String expected = "Returned row state doesn't match the one predicted by the model";
                         Assert.assertTrue(String.format("Exception string mismatch.\nExpected error: %s.\nActual error: %s", expected, t.getMessage()),
                                           t.getMessage().contains(expected));
                     });
    }


    static void negativeTest(Consumer<Run> corrupt, Consumer<Throwable> validate)
    {
        Configuration config = sharedConfiguration()
                               .setModel(new Configuration.QuiescentCheckerConfig())
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
        }
        catch (Throwable t)
        {
            validate.accept(t);
        }
    }
}