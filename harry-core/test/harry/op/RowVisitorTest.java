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

import org.junit.Assert;
import org.junit.Test;

import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.generators.Generator;
import harry.generators.distribution.Distribution;
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
    @Test
    public void rowWriteGeneratorTest()
    {
        Generator<SchemaSpec> specGenerator = SchemaGenerators.schema(KEYSPACE)
                                                              .partitionKeyColumnCount(1, 5)
                                                              .clusteringColumnCount(1, 5)
                                                              .regularColumnCount(2, 5)
                                                              .generator();

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

        test(specGenerator,
             (schema) -> {
                 createTable(schema.compile().cql());
                 return (rng) -> {
                     DefaultRowVisitor visitor = new DefaultRowVisitor(schema,
                                                                       new OffsetClock(10000),
                                                                       descriptorSelector,
                                                                       new QuerySelector(schema,
                                                                                         new OpSelectors.DefaultPdSelector(opRng, 10, 10),
                                                                                         descriptorSelector,
                                                                                         opRng));
                     long[] descriptors = rng.next(4);

                     return visitor.write(Math.abs(descriptors[0]),
                                          descriptors[1],
                                          descriptors[2],
                                          descriptors[3]);
                 };
             },
             (Consumer<CompiledStatement>) this::execute);
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