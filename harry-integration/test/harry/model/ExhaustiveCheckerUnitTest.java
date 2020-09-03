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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.Assert;
import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.model.sut.NoOpSut;
import harry.operations.CompiledStatement;
import harry.runner.PartitionVisitor;
import harry.runner.Query;
import harry.runner.RowVisitor;

public class ExhaustiveCheckerUnitTest
{
    @Test
    public void testOperationConsistency()
    {
        LoggingRowVisitor rowVisitor = new LoggingRowVisitor();

        Configuration config = IntegrationTestBase.sharedConfiguration()
                                                  .setPartitionDescriptorSelector(new Configuration.DefaultPDSelectorConfiguration(10, 10))
                                                  .setClusteringDescriptorSelector((builder) -> {
                                                      builder.setOperationKindWeights(new Configuration.OperationKindSelectorBuilder()
                                                                                      .addWeight(OpSelectors.OperationKind.DELETE_ROW, 33)
                                                                                      .addWeight(OpSelectors.OperationKind.DELETE_COLUMN, 33)
                                                                                      .addWeight(OpSelectors.OperationKind.WRITE, 34)
                                                                                      .build());
                                                  })
                                                  .setRowVisitor((a_, b_, c_, d_) -> rowVisitor)
                                                  .setSUT(NoOpSut::new)
                                                  .setCreateSchema(true)
                                                  .build();

        Run run = config.createRun();

        ExhaustiveChecker checker = (ExhaustiveChecker) run.model;

        int iterations = 10;
        PartitionVisitor partitionVisitor = run.visitorFactory.get();
        for (int i = 0; i < iterations; i++)
            partitionVisitor.visitPartition(i);

        for (List<ExhaustiveChecker.Operation> value : rowVisitor.executed.values())
            Collections.sort(value);

        for (int lts = 0; lts < iterations; lts++)
        {
            // TODO: turn query into interface to make it easier to deal with here
            for (Collection<ExhaustiveChecker.Operation> modelOps : checker.inflatePartitionState(lts,
                                                                                                  (iterations - 1),
                                                                                                  Query.selectPartition(run.schemaSpec,
                                                                                                                        run.pdSelector.pd(lts),
                                                                                                                        false))
                                                                           .operations.values())
            {
                ExhaustiveChecker.Operation op = modelOps.iterator().next();
                List<ExhaustiveChecker.Operation> executedOps = rowVisitor.executed.get(new Pair(op.pd, op.cd));
                Iterator<ExhaustiveChecker.Operation> modelIterator = modelOps.iterator();
                Iterator<ExhaustiveChecker.Operation> executedIterator = executedOps.iterator();
                while (modelIterator.hasNext() && executedIterator.hasNext())
                {
                    Assert.assertEquals(String.format("\n%s\n%s", modelOps, executedOps),
                                        executedIterator.next(), modelIterator.next());
                }
                Assert.assertEquals(String.format("\n%s\n%s", modelOps, executedOps),
                                    modelIterator.hasNext(), executedIterator.hasNext());
            }
        }
    }

    private static class LoggingRowVisitor implements RowVisitor
    {
        private final Map<Pair, List<ExhaustiveChecker.Operation>> executed = new HashMap<>();

        public CompiledStatement visitRow(OpSelectors.OperationKind op, long lts, long pd, long cd, long opId)
        {
            executed.compute(new Pair(pd, cd), (lts_, list) -> {
                if (list == null)
                    list = new ArrayList<>();
                list.add(new ExhaustiveChecker.Operation(pd, cd, lts, opId, op));
                return list;
            });

            return CompiledStatement.create("");
        }

        public CompiledStatement write(long lts, long pd, long cd, long opId)
        {
            return null;
        }

        public CompiledStatement deleteColumn(long lts, long pd, long cd, long opId)
        {
            return null;
        }

        public CompiledStatement deleteRow(long lts, long pd, long cd, long opId)
        {
            return null;
        }

        public CompiledStatement deleteRange(long lts, long pd, long opId)
        {
            return null;
        }
    }

    private static class Pair
    {
        final long pd;
        final long cd;

        Pair(long pd, long cd)
        {
            this.pd = pd;
            this.cd = cd;
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Pair pair = (Pair) o;
            return pd == pair.pd &&
                   cd == pair.cd;
        }

        public int hashCode()
        {
            return Objects.hash(pd, cd);
        }
    }
}