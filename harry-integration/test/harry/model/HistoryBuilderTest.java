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

import java.util.ArrayList;
import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.SchemaSpec;
import harry.dsl.HistoryBuilder;
import harry.util.TestRunner;
import harry.visitors.ReplayingVisitor;
import harry.visitors.VisitExecutor;

public class HistoryBuilderTest
{
    @Test
    public void testHistoryBuilder() throws Throwable
    {
        SchemaSpec schema = MockSchema.tbl1;

        Configuration config = IntegrationTestBase.sharedConfiguration(1, schema)
                                                  .setPartitionDescriptorSelector((ignore) -> new HistoryBuilder.PdSelector())
                                                  .setClusteringDescriptorSelector((builder) -> builder.setNumberOfModificationsDistribution(new Configuration.ConstantDistributionConfig(100_000))
                                                                                                       .setRowsPerModificationDistribution(new Configuration.ConstantDistributionConfig(100_000)))
                                                  .build();

        Run run = config.createRun();
        TestRunner.test((rng) -> {
                            Counts counts = new Counts();
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
                        () -> new ArrayList<Counts>(),
                        () -> new HistoryBuilder(run),
                        (model, counts) -> {
                            model.add(counts);
                            return model;
                        },
                        (sut, counts) -> {
                            counts.apply(sut);
                            return sut;
                        },
                        (model, sut) -> {
                            Iterator<Counts> iter = model.iterator();
                            ReplayingVisitor visitor = sut.visitor(new VisitExecutor()
                            {
                                Counts current = iter.next();
                                long lastPd = Long.MIN_VALUE;

                                public void beforeLts(long lts, long pd)
                                {
                                    if (lastPd == Long.MIN_VALUE)
                                    {
                                        lastPd = pd;
                                        return;
                                    }

                                    if (current.allDone())
                                    {
                                        Assert.assertNotEquals("Should have switched partition after finishing all operations",
                                                               pd, lastPd);
                                        Assert.assertTrue("System under test still has operations, while model expects none",
                                                          iter.hasNext());
                                        current = iter.next();
                                        lastPd = pd;
                                    }
                                }

                                public void operation(long lts, long pd, long cd, long m, long opId, OpSelectors.OperationKind kind)
                                {
                                    switch (kind)
                                    {
                                        case UPDATE:
                                        case UPDATE_WITH_STATICS:
                                            current.update--;
                                            break;
                                        case INSERT:
                                        case INSERT_WITH_STATICS:
                                            current.insert--;
                                            break;
                                        case DELETE_PARTITION:
                                            current.partitionDeletion--;
                                            break;
                                        case DELETE_ROW:
                                            current.delete--;
                                            break;
                                        case DELETE_RANGE:
                                            current.rangeDelete--;
                                            break;
                                        case DELETE_SLICE:
                                            current.sliceDelete--;
                                            break;
                                        case DELETE_COLUMN:
                                        case DELETE_COLUMN_WITH_STATICS:
                                            current.columnDelete--;
                                            break;
                                    }
                                }

                                public void afterLts(long lts, long pd){}
                                public void beforeBatch(long lts, long pd, long m){}
                                public void afterBatch(long lts, long pd, long m){}
                            });
                            visitor.replayAll(run);
                            for (Counts counts : model)
                                Assert.assertTrue(counts.toString(), counts.allDone());
                        });
    }

    public static class Counts
    {
        boolean randomOrder;
        boolean simultaneously;
        int partitionDeletion;
        int update;
        int insert;
        int delete;
        int rangeDelete;
        int sliceDelete;
        int columnDelete;

        public void apply(HistoryBuilder sut)
        {
            HistoryBuilder.PartitionBuilder builder = sut.nextPartition();
            if (randomOrder)
                builder.randomOrder();
            else
                builder.strictOrder();

            if (simultaneously)
                builder.simultaneously();
            else
                builder.sequentially();

            builder.deletes(delete);
            builder.inserts(insert);
            builder.updates(update);
            builder.partitionDeletions(partitionDeletion);
            builder.rangeDeletes(rangeDelete);
            builder.sliceDeletes(sliceDelete);
            builder.columnDeletes(columnDelete);

            builder.finish();
        }
        boolean allDone()
        {
            return partitionDeletion == 0 &&
                   update == 0 &&
                   insert == 0 &&
                   delete == 0 &&
                   rangeDelete == 0 &&
                   sliceDelete == 0 &&
                   columnDelete == 0;
        }
    }

}
