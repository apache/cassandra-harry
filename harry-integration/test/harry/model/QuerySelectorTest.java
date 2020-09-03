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
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.SchemaSpec;
import harry.operations.CompiledStatement;
import harry.runner.PartitionVisitor;
import harry.runner.Query;
import harry.runner.QuerySelector;

public class QuerySelectorTest extends IntegrationTestBase
{
    private static int CYCLES = 100;

    @Test
    public void basicQuerySelectorTest()
    {
        for (int cnt = 0; cnt < 50; cnt++)
        {
            SchemaSpec schemaSpec = MockSchema.randomSchema("harry", "table" + cnt, cnt);
            int partitionSize = 200;
            int[] fractions = new int[schemaSpec.clusteringKeys.size()];
            int last = partitionSize;
            for (int i = fractions.length - 1; i >= 0; i--)
            {
                fractions[i] = last;
                last = last / 2;
            }

            Configuration config = sharedConfiguration(cnt, schemaSpec)
                                   .setClusteringDescriptorSelector((builder) -> {
                                       builder.setMaxPartitionSize(partitionSize)
                                              .setFractions(fractions);
                                   })
                                   .build();
            Run run = config.createRun();
            run.sut.schemaChange(run.schemaSpec.compile().cql());
            OpSelectors.MonotonicClock clock = run.clock;
            PartitionVisitor partitionVisitor = run.visitorFactory.get();

            for (int i = 0; i < CYCLES; i++)
            {
                long lts = clock.nextLts();
                partitionVisitor.visitPartition(lts);
            }

            QuerySelector querySelector = new QuerySelector(run.schemaSpec,
                                                            run.pdSelector,
                                                            run.descriptorSelector,
                                                            run.rng);

            for (int i = 0; i < CYCLES; i++)
            {
                Query query = querySelector.inflate(i, i);
                Object[][] results = run.sut.execute(query.toSelectStatement());
                Set<Long> matchingClusterings = new HashSet<>();
                for (Object[] row : results)
                {
                    long cd = SelectHelper.resultSetToRow(run.schemaSpec,
                                                          run.clock,
                                                          row).cd;
                    matchingClusterings.add(cd);
                }

                // the simplest test there can be: every row that is in the partition and was returned by the query,
                // has to "match", every other row has to be a non-match
                CompiledStatement selectPartition = SelectHelper.select(run.schemaSpec, run.pdSelector.pd(i));
                Object[][] partition = run.sut.execute(selectPartition);
                for (Object[] row : partition)
                {
                    long cd = SelectHelper.resultSetToRow(run.schemaSpec,
                                                          run.clock,
                                                          row).cd;

                    Assert.assertEquals(matchingClusterings.contains(cd),
                                        query.match(cd));
                }
            }
        }
    }

    @Test
    public void querySelectorModelTest()
    {
        for (int cnt = 0; cnt < 50; cnt++)
        {
            SchemaSpec schemaSpec = MockSchema.randomSchema("harry", "table" + cnt, cnt);
            int[] fractions = new int[schemaSpec.clusteringKeys.size()];
            int partitionSize = 200;
            int last = partitionSize;
            for (int i = fractions.length - 1; i >= 0; i--)
            {
                fractions[i] = last;
                last = last / 2;
            }

            Configuration config = sharedConfiguration(cnt, schemaSpec)
                                   .setClusteringDescriptorSelector((builder) -> {
                                       builder.setMaxPartitionSize(partitionSize)
                                              .setFractions(fractions);
                                   })
                                   .build();
            Run run = config.createRun();
            run.sut.schemaChange(run.schemaSpec.compile().cql());
            OpSelectors.MonotonicClock clock = run.clock;
            PartitionVisitor partitionVisitor = run.visitorFactory.get();

            for (int i = 0; i < CYCLES; i++)
            {
                long lts = clock.nextLts();
                partitionVisitor.visitPartition(lts);
            }

            QuerySelector querySelector = new QuerySelector(run.schemaSpec,
                                                            run.pdSelector,
                                                            run.descriptorSelector,
                                                            run.rng);

            Model model = run.model;

            long verificationLts = 10;
            for (int i = 0; i < CYCLES; i++)
            {
                Query query = querySelector.inflate(verificationLts, i);
                model.validatePartitionState(verificationLts, query);
            }
        }
    }
}