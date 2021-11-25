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
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.model.sut.SystemUnderTest;
import harry.operations.CompiledStatement;
import harry.visitors.MutatingVisitor;
import harry.visitors.MutatingRowVisitor;
import harry.operations.Query;
import harry.operations.QueryGenerator;
import harry.visitors.Visitor;

import static harry.generators.DataGenerators.NIL_DESCR;

public class QuerySelectorTest extends IntegrationTestBase
{
    private static int CYCLES = 300;

    @Test
    public void basicQuerySelectorTest()
    {
        Supplier<SchemaSpec> schemaGen = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        for (int cnt = 0; cnt < SchemaGenerators.DEFAULT_RUNS; cnt++)
        {
            beforeEach();
            SchemaSpec schemaSpec = schemaGen.get();
            int partitionSize = 200;

            int[] fractions = new int[schemaSpec.clusteringKeys.size()];
            int last = partitionSize;
            for (int i = fractions.length - 1; i >= 0; i--)
            {
                fractions[i] = last;
                last = last / 2;
            }

            Configuration config = sharedConfiguration(cnt, schemaSpec)
                                   .setClusteringDescriptorSelector(sharedCDSelectorConfiguration()
                                                                    .setMaxPartitionSize(partitionSize)
                                                                    .setFractions(fractions)
                                                                    .build())
                                   .build();

            Run run = config.createRun();
            run.sut.schemaChange(run.schemaSpec.compile().cql());

            Visitor visitor = new MutatingVisitor(run, MutatingRowVisitor::new);

            for (int i = 0; i < CYCLES; i++)
                visitor.visit();

            QueryGenerator.TypedQueryGenerator querySelector = new QueryGenerator.TypedQueryGenerator(run);

            for (int i = 0; i < CYCLES; i++)
            {
                Query query = querySelector.inflate(i, i);

                Object[][] results = run.sut.execute(query.toSelectStatement(), SystemUnderTest.ConsistencyLevel.QUORUM);
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
                Object[][] partition = run.sut.execute(selectPartition, SystemUnderTest.ConsistencyLevel.QUORUM);
                for (Object[] row : partition)
                {
                    long cd = SelectHelper.resultSetToRow(run.schemaSpec,
                                                          run.clock,
                                                          row).cd;

                    // Skip static clustering
                    if (cd == NIL_DESCR)
                        continue;

                    boolean expected = matchingClusterings.contains(cd);
                    boolean actual = query.match(cd);
                    Assert.assertEquals(String.format("Mismatch for clustering: %d. Expected: %s. Actual: %s.\nQuery: %s",
                                                      cd, expected, actual, query.toSelectStatement()),
                                        expected,
                                        actual);
                }
            }
        }
    }

    @Test
    public void querySelectorModelTest()
    {
        Supplier<SchemaSpec> gen = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        for (int cnt = 0; cnt < SchemaGenerators.DEFAULT_RUNS; cnt++)
        {
            SchemaSpec schemaSpec = gen.get();
            int[] fractions = new int[schemaSpec.clusteringKeys.size()];
            int partitionSize = 200;
            int last = partitionSize;
            for (int i = fractions.length - 1; i >= 0; i--)
            {
                fractions[i] = last;
                last = last / 2;
            }

            Configuration config = sharedConfiguration(cnt, schemaSpec)
                                   .setClusteringDescriptorSelector(sharedCDSelectorConfiguration()
                                                                    .setMaxPartitionSize(partitionSize)
                                                                    .setFractions(fractions)
                                                                    .build())
                                   .build();
            Run run = config.createRun();
            run.sut.schemaChange(run.schemaSpec.compile().cql());
            Visitor visitor = new MutatingVisitor(run, MutatingRowVisitor::new);

            for (int i = 0; i < CYCLES; i++)
                visitor.visit();

            QueryGenerator.TypedQueryGenerator querySelector = new QueryGenerator.TypedQueryGenerator(run);
            Model model = new QuiescentChecker(run);

            long verificationLts = 10;
            for (int i = 0; i < CYCLES; i++)
            {
                Query query = querySelector.inflate(verificationLts, i);
                model.validate(query);
            }
        }
    }
}