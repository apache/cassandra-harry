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

import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.generators.Generator;
import harry.generators.PcgRSUFast;
import harry.generators.RandomGenerator;
import harry.generators.Surjections;
import harry.generators.distribution.Distribution;
import harry.model.OpSelectorsTest;
import harry.runner.DefaultRowVisitor;
import harry.model.clock.OffsetClock;
import harry.model.OpSelectors;
import harry.operations.CompiledStatement;
import harry.runner.QuerySelector;
import harry.util.BitSet;
import org.apache.cassandra.cql3.CQLTester;

import static harry.util.TestRunner.test;
import static harry.model.OpSelectors.DefaultDescriptorSelector.DEFAULT_OP_TYPE_SELECTOR;

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
        Supplier<SchemaSpec> specGenerator = SchemaGenerators.progression(5);
        RandomGenerator rand = RandomGenerator.forTests(6371747244598697093L);

        OpSelectors.Rng opRng = new OpSelectors.PCGFast(1);
        OpSelectors.DefaultDescriptorSelector descriptorSelector = new OpSelectors.DefaultDescriptorSelector(new OpSelectors.PCGFast(1L),
                                                                                                             OpSelectors.columnSelectorBuilder().forAll(BitSet.create(0b001, 3),
                                                                                                                                                        BitSet.create(0b011, 3),
                                                                                                                                                        BitSet.create(0b111, 3))
                                                                                                                        .build(),
                                                                                                             DEFAULT_OP_TYPE_SELECTOR,
                                                                                                             new Distribution.ScaledDistribution(1, 3),
                                                                                                             new Distribution.ScaledDistribution(2, 30),
                                                                                                             100);

        for (int i = 0; i < SchemaGenerators.PROGRESSIVE_GENERATORS.length * 5; i++)
        {
            SchemaSpec schema = specGenerator.get();
            createTable(schema.compile().cql());

            DefaultRowVisitor visitor = new DefaultRowVisitor(schema,
                                                              new OffsetClock(10000),
                                                              descriptorSelector,
                                                              new QuerySelector(schema,
                                                                                new OpSelectors.DefaultPdSelector(opRng, 10, 10),
                                                                                descriptorSelector,
                                                                                opRng));
            long[] descriptors = rand.next(4);

            execute(visitor.write(Math.abs(descriptors[0]),
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