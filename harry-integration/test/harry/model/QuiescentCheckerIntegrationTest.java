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

import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.corruptor.AddExtraRowCorruptor;
import harry.corruptor.ChangeValueCorruptor;
import harry.corruptor.HideRowCorruptor;
import harry.corruptor.HideValueCorruptor;
import harry.corruptor.QueryResponseCorruptor;
import harry.corruptor.QueryResponseCorruptor.SimpleQueryResponseCorruptor;
import harry.ddl.SchemaSpec;
import harry.runner.PartitionVisitor;
import harry.runner.Query;
import harry.runner.SinglePartitionValidator;

public class QuiescentCheckerIntegrationTest extends ModelTestBase
{
    @Override
    protected PartitionVisitor validator(Run run)
    {
        return new SinglePartitionValidator(100, run, modelConfiguration());
    }

    @Test
    public void testNormalCondition()
    {
        negativeTest((run) -> true,
                     (t, run) -> {
                         if (t != null)
                             throw new AssertionError(String.format("Throwable was supposed to be null. Schema: %s",
                                                                    run.schemaSpec.compile().cql()),
                                                      t);
                     });
    }

    @Test
    public void normalConditionIntegrationTest() throws Throwable
    {
        negativeIntegrationTest(modelConfiguration()::make);
    }

    @Test
    public void testDetectsMissingRow()
    {
        negativeTest((run) -> {
                         SimpleQueryResponseCorruptor corruptor = new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                                                   run.clock,
                                                                                                   HideRowCorruptor::new);

                         Query query = Query.selectPartition(run.schemaSpec,
                                                             run.pdSelector.pd(0, run.schemaSpec),
                                                             false);

                         return corruptor.maybeCorrupt(query, run.sut);
                     },
                     (t, run) -> {
                         // TODO: We can actually pinpoint the difference
                         String expected = "Expected results to have the same number of results, but expected result iterator has more results";
                         String expected2 = "Found a row in the model that is not present in the resultset";

                         if (t.getMessage().contains(expected) || t.getMessage().contains(expected2))
                             return;

                         throw new AssertionError(String.format("Exception string mismatch.\nExpected error: %s.\nActual error: %s", expected, t.getMessage()),
                                                  t);
                     });
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
                         String expected = "Found a row in the model that is not present in the resultset";
                         String expected2 = "Expected results to have the same number of results, but actual result iterator has more results";

                         if (t.getMessage().contains(expected) || t.getMessage().contains(expected2))
                             return;

                         throw new AssertionError(String.format("Exception string mismatch.\nExpected error: %s.\nActual error: %s", expected, t.getMessage()),
                                                  t);
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
                     (t, run) -> {
                         String expected = "doesn't match the one predicted by the model";
                         String expected2 = "don't match ones predicted by the model";
                         if (t.getMessage().contains(expected) || t.getMessage().contains(expected2))
                             return;

                         throw new AssertionError(String.format("Exception string mismatch.\nExpected error: %s.\nActual error: %s", expected, t.getMessage()),
                                                  t);
                     });
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
                     (t, run) -> {
                         String expected = "Returned row state doesn't match the one predicted by the model";
                         String expected2 = "Timestamps in the row state don't match ones predicted by the model";

                         if (t.getMessage().contains(expected) || t.getMessage().contains(expected2))
                             return;

                         throw new AssertionError(String.format("Exception string mismatch.\nExpected error: %s.\nActual error: %s", expected, t.getMessage()),
                                                  t);
                     });
    }

    @Override
    Configuration.ModelConfiguration modelConfiguration()
    {
        return new Configuration.QuiescentCheckerConfig();
    }

    public Configuration.ConfigurationBuilder configuration(long seed, SchemaSpec schema)
    {
        return super.configuration(seed, schema)
                    .setClusteringDescriptorSelector(sharedCDSelectorConfiguration()
                                                     .setNumberOfModificationsDistribution(new Configuration.ConstantDistributionConfig(2))
                                                     .setRowsPerModificationDistribution(new Configuration.ConstantDistributionConfig(2))
                                                     .setMaxPartitionSize(100)
                                                     .build());
    }
}