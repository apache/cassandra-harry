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

package harry.examples;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.Test;

import harry.checker.ModelChecker;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.dsl.HistoryBuilder;
import harry.dsl.ReplayingHistoryBuilder;
import harry.generators.EntropySource;
import harry.generators.JdkRandomEntropySource;
import harry.model.AgainstSutChecker;
import harry.model.IntegrationTestBase;
import harry.model.sut.QueryModifyingSut;
import harry.runner.DataTracker;
import harry.runner.DefaultDataTracker;

public class RangeTombstoneBurnTest extends IntegrationTestBase
{
    private final long seed = 1;
    private final int ITERATIONS = 5;
    private final int STEPS_PER_ITERATION = 1_000;

    @Test
    public void rangeTombstoneBurnTest() throws Throwable
    {
        Supplier<SchemaSpec> supplier = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);

        for (int i = 0; i < SchemaGenerators.DEFAULT_RUNS; i++)
        {
            SchemaSpec schema = supplier.get();
            beforeEach();
            SchemaSpec doubleWriteSchema = schema.cloneWithName(schema.keyspace, schema.keyspace + "_debug");

            sut.schemaChange(schema.compile().cql());
            sut.schemaChange(doubleWriteSchema.compile().cql());

            QueryModifyingSut sut = new QueryModifyingSut(this.sut,
                                                          schema.table,
                                                          doubleWriteSchema.table);

            cluster.get(1).nodetool("disableautocompaction");

            for (int iteration = 0; iteration < ITERATIONS; iteration++)
            {
                ModelChecker<ReplayingHistoryBuilder> modelChecker = new ModelChecker<>();
                EntropySource entropySource = new JdkRandomEntropySource(iteration);

                // TODO: Weigh higher towards edges? quantize and generate numbers?
                int maxPartitionSize = entropySource.nextInt(1, 1 << entropySource.nextInt(5, 11));

                int[] partitions = new int[10];
                for (int j = 0; j < partitions.length; j++)
                    partitions[j] = iteration * partitions.length + j;

                float deleteRowChance = entropySource.nextFloat(0.99f, 1.0f);
                float deletePartitionChance = entropySource.nextFloat(0.999f, 1.0f);
                float deleteRangeChance = entropySource.nextFloat(0.95f, 1.0f);
                float flushChance = entropySource.nextFloat(0.999f, 1.0f);
                AtomicInteger flushes = new AtomicInteger();

                DataTracker tracker = new DefaultDataTracker();
                modelChecker.init(new ReplayingHistoryBuilder(seed, maxPartitionSize, STEPS_PER_ITERATION, schema, new DefaultDataTracker(), sut))
                            .step((history) -> {
                                      int rowIdx = entropySource.nextInt(maxPartitionSize);
                                      int partitionIdx = partitions[entropySource.nextInt(partitions.length)];
                                      history.visitPartition(partitionIdx).insert(rowIdx);
                                      return history;
                                  })
                            .step((history) -> entropySource.nextFloat() > deleteRowChance,
                                  (history) -> {
                                      int partitionIdx = partitions[entropySource.nextInt(partitions.length)];
                                      history.visitPartition(partitionIdx).deleteRow();
                                      return history;
                                  })
                            .step((history) -> entropySource.nextFloat() > deleteRowChance,
                                  (history) -> {
                                      int partitionIdx = partitions[entropySource.nextInt(partitions.length)];
                                      history.visitPartition(partitionIdx).deleteColumns();
                                      return history;
                                  })
                            .step((history) -> entropySource.nextFloat() > deletePartitionChance,
                                  (history) -> {
                                      int partitionIdx = partitions[entropySource.nextInt(partitions.length)];
                                      history.visitPartition(partitionIdx).deletePartition();
                                      return history;
                                  })
                            .step((history) -> entropySource.nextFloat() > flushChance,
                                  (history) -> {
                                      cluster.get(1).nodetool("flush", schema.keyspace, schema.table);
                                      flushes.incrementAndGet();
                                      return history;
                                  })
                            .step((history) -> entropySource.nextFloat() > deleteRangeChance,
                                  (history) -> {
                                      int partitionIdx = partitions[entropySource.nextInt(partitions.length)];
                                      history.visitPartition(partitionIdx).deleteRowSlice();
                                      return history;
                                  })
                            .step((history) -> entropySource.nextFloat() > deleteRangeChance,
                                  (history) -> {
                                      int row1 = entropySource.nextInt(maxPartitionSize);
                                      int row2 = entropySource.nextInt(maxPartitionSize);
                                      int partitionIdx = partitions[entropySource.nextInt(partitions.length)];
                                      history.visitPartition(partitionIdx).deleteRowRange(Math.min(row1, row2),
                                                                                      Math.max(row1, row2),
                                                                                      entropySource.nextBoolean(),
                                                                                      entropySource.nextBoolean());
                                      return history;
                                  })
                            .afterAll((history) -> {
                                // Sanity check
                                history.validate(new AgainstSutChecker(tracker, history.clock(), sut, schema, doubleWriteSchema),
                                                 partitions);
                                history.validate(partitions);
                            })
                            .run(STEPS_PER_ITERATION, seed);
            }
        }
    }
}