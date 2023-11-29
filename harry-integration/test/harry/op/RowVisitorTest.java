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

package harry.op;

import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import harry.core.MetricReporter;
import harry.core.Run;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.generators.RandomGenerator;
import harry.generators.distribution.Distribution;
import harry.model.OpSelectors;
import harry.model.clock.OffsetClock;
import harry.model.sut.SystemUnderTest;
import harry.operations.CompiledStatement;
import harry.runner.DataTracker;
import harry.visitors.MutatingRowVisitor;
import org.apache.cassandra.cql3.CQLTester;

import static harry.model.OpSelectors.DefaultDescriptorSelector.DEFAULT_OP_SELECTOR;

public class RowVisitorTest extends CQLTester
{
    @Before
    public void beforeTest() throws Throwable {
        super.beforeTest();
        schemaChange(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", SchemaGenerators.DEFAULT_KEYSPACE_NAME));
    }

    @Test
    public void rowWriteGeneratorTest()
    {
        Supplier<SchemaSpec> specGenerator = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        RandomGenerator rand = RandomGenerator.forTests(6371747244598697093L);

        OpSelectors.Rng rng = new OpSelectors.PCGFast(1);

        OpSelectors.PdSelector pdSelector = new OpSelectors.DefaultPdSelector(rng, 10, 10);

        for (int i = 0; i < SchemaGenerators.DEFAULT_RUNS; i++)
        {
            SchemaSpec schema = specGenerator.get();
            createTable(schema.compile().cql());

            OpSelectors.DescriptorSelector descriptorSelector = new OpSelectors.DefaultDescriptorSelector(rng,
                                                                                                          new OpSelectors.ColumnSelectorBuilder().forAll(schema)
                                                                                                                                                 .build(),
                                                                                                          DEFAULT_OP_SELECTOR,
                                                                                                          new Distribution.ScaledDistribution(1, 30),
                                                                                                          100);

            Run run = new Run(rng,
                              new OffsetClock(10000),
                              pdSelector,
                              descriptorSelector,
                              schema,
                              DataTracker.NO_OP,
                              SystemUnderTest.NO_OP,
                              MetricReporter.NO_OP);

            MutatingRowVisitor visitor = new MutatingRowVisitor(run);
            long[] descriptors = rand.next(4);

            execute(visitor.insert(Math.abs(descriptors[0]),
                                   descriptors[1],
                                   descriptors[2],
                                   descriptors[3]));
        }
    }

    public void execute(CompiledStatement statement)
    {
        try
        {
            execute(statement.cql(),
                    statement.bindings());
        }
        catch (Throwable throwable)
        {
            Assert.fail(String.format("Failed to execute %s. Error: %s",
                                      statement,
                                      throwable.getMessage()));
        }
    }
}