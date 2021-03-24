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

package harry.reconciler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import harry.ddl.SchemaSpec;
import harry.model.ExhaustiveChecker;
import harry.model.OpSelectors;
import harry.runner.AbstractPartitionVisitor;
import harry.runner.PartitionVisitor;
import harry.runner.Query;
import harry.runner.QueryGenerator;
import harry.util.BitSet;
import harry.util.Ranges;

import static harry.generators.DataGenerators.NIL_DESCR;
import static harry.generators.DataGenerators.UNSET_DESCR;
import static harry.model.Model.NO_TIMESTAMP;

/**
 * A simple Cassandra-style reconciler for operations against model state.
 * <p>
 * It is useful both as a testing/debugging tool (to avoid starting Cassandra
 * cluster to get a result set), and as a quiescent model checker.
 */
public class Reconciler
{
    private final OpSelectors.DescriptorSelector descriptorSelector;
    private final OpSelectors.PdSelector pdSelector;
    private final QueryGenerator rangeSelector;
    private final SchemaSpec schema;

    public Reconciler(SchemaSpec schema,
                      OpSelectors.PdSelector pdSelector,
                      OpSelectors.DescriptorSelector descriptorSelector,
                      QueryGenerator rangeSelector)
    {
        this.descriptorSelector = descriptorSelector;
        this.pdSelector = pdSelector;
        this.schema = schema;
        this.rangeSelector = rangeSelector;
    }

    public PartitionState inflatePartitionState(final long pd, long maxLts, Query query)
    {
        List<Ranges.Range> ranges = new ArrayList<>();

        // TODO: we should think of a single-pass algorithm that would allow us to inflate all deletes and range deletes for a partition
        PartitionVisitor partitionVisitor = new AbstractPartitionVisitor(pdSelector, descriptorSelector, schema)
        {
            public void operation(long lts, long pd, long cd, long m, long opId)
            {
                OpSelectors.OperationKind opType = descriptorSelector.operationType(pd, lts, opId);
                if (opType == OpSelectors.OperationKind.DELETE_RANGE)
                {
                    ranges.add(rangeSelector.inflate(lts, opId, Query.QueryKind.CLUSTERING_RANGE).toRange(lts));
                }
                else if (opType == OpSelectors.OperationKind.DELETE_SLICE)
                {
                    ranges.add(rangeSelector.inflate(lts, opId, Query.QueryKind.CLUSTERING_SLICE).toRange(lts));
                }
                else if (opType == OpSelectors.OperationKind.DELETE_ROW)
                {
                    ranges.add(new Ranges.Range(cd, cd, true, true, lts));
                }
            }
        };

        long currentLts = pdSelector.minLtsFor(pd);

        while (currentLts <= maxLts && currentLts >= 0)
        {
            partitionVisitor.visitPartition(currentLts);
            currentLts = pdSelector.nextLts(currentLts);
        }

        PartitionState partitionState = new PartitionState();
        partitionVisitor = new AbstractPartitionVisitor(pdSelector, descriptorSelector, schema)
        {
            public void operation(long lts, long pd, long cd, long m, long opId)
            {
                if (!query.match(cd))
                    return;

                OpSelectors.OperationKind opType = descriptorSelector.operationType(pd, lts, opId);

                if (opType == OpSelectors.OperationKind.DELETE_ROW
                    || opType == OpSelectors.OperationKind.DELETE_RANGE
                    || opType == OpSelectors.OperationKind.DELETE_SLICE)
                    return;

                // TODO: avoid linear scan
                for (Ranges.Range range : ranges)
                {
                    if (range.timestamp >= lts && range.contains(cd))
                        return;
                }

                if (opType == OpSelectors.OperationKind.WRITE)
                {
                    partitionState.add(cd,
                                       descriptorSelector.vds(pd, cd, lts, opId, schema),
                                       lts);
                }
                else if (opType == OpSelectors.OperationKind.DELETE_COLUMN)
                {
                    partitionState.deleteColumns(cd,
                                                 descriptorSelector.columnMask(pd, lts, opId));
                }
                else
                {
                    throw new AssertionError();
                }
            }
        };

        currentLts = pdSelector.minLtsFor(pd);
        while (currentLts <= maxLts && currentLts >= 0)
        {
            partitionVisitor.visitPartition(currentLts);
            currentLts = pdSelector.nextLts(currentLts);
        }

        return partitionState;
    }

    public static class PartitionState implements Iterable<RowState>
    {
        private NavigableMap<Long, RowState> rows;

        private PartitionState()
        {
            rows = new TreeMap<>();
        }

        private void add(long cd,
                         long[] vds,
                         long lts)
        {
            RowState state = rows.get(cd);

            if (state == null)
            {
                long[] ltss = new long[vds.length];
                long[] vdsCopy = new long[vds.length];
                for (int i = 0; i < vds.length; i++)
                {
                    if (vds[i] != UNSET_DESCR)
                    {
                        ltss[i] = lts;
                        vdsCopy[i] = vds[i];
                    }
                    else
                    {
                        ltss[i] = NO_TIMESTAMP;
                        vdsCopy[i] = NIL_DESCR;
                    }
                }

                state = new RowState(cd, vdsCopy, ltss);
                rows.put(cd, state);
            }
            else
            {
                for (int i = 0; i < vds.length; i++)
                {
                    if (vds[i] != UNSET_DESCR)
                    {
                        state.vds[i] = vds[i];
                        assert lts > state.lts[i]; // sanity check; we're iterating in lts order
                        state.lts[i] = lts;
                    }
                }
            }
        }

        private void deleteColumns(long cd, BitSet mask)
        {
            RowState state = rows.get(cd);
            if (state == null)
                return;

            for (int i = 0; i < mask.size(); i++)
            {
                if (mask.isSet(i))
                {
                    state.vds[i] = NIL_DESCR;
                    state.lts[i] = NO_TIMESTAMP;
                }
            }
        }

        public Iterator<RowState> iterator()
        {
            return iterator(false);
        }

        public Iterator<RowState> iterator(boolean reverse)
        {
            if (reverse)
                return rows.descendingMap().values().iterator();

            return rows.values().iterator();
        }

        public Collection<RowState> rows(boolean reverse)
        {
            if (reverse)
                return rows.descendingMap().values();

            return rows.values();
        }
    }

    public static class RowState
    {
        public final long cd;
        public final long[] vds;
        public final long[] lts;

        public RowState(long cd,
                        long[] vds,
                        long[] lts)
        {
            this.cd = cd;
            this.vds = vds;
            this.lts = lts;
        }

        public String toString()
        {
            return "RowState{" +
                   "cd=" + cd +
                   ", vds=" + Arrays.toString(vds) +
                   ", lts=" + Arrays.toString(lts) +
                   '}';
        }
    }
}