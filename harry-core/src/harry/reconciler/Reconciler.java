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
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Run;
import harry.ddl.ColumnSpec;
import harry.ddl.SchemaSpec;
import harry.model.OpSelectors;
import harry.visitors.AbstractPartitionVisitor;
import harry.visitors.PartitionVisitor;
import harry.operations.Query;
import harry.operations.QueryGenerator;
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
 *
 * TODO: it might be useful to actually record deletions instead of just removing values as we do right now.
 */
public class Reconciler
{
    private static final Logger logger = LoggerFactory.getLogger(Reconciler.class);

    private static long STATIC_CLUSTERING = NIL_DESCR;

    private final OpSelectors.DescriptorSelector descriptorSelector;
    private final OpSelectors.PdSelector pdSelector;
    private final QueryGenerator rangeSelector;
    private final SchemaSpec schema;

    public Reconciler(Run run)
    {
        this.descriptorSelector = run.descriptorSelector;
        this.pdSelector = run.pdSelector;
        this.schema = run.schemaSpec;
        this.rangeSelector = run.rangeSelector;
    }

    private final long debugCd = Long.getLong("harry.reconciler.debug_cd", -1L);

    public PartitionState inflatePartitionState(final long pd, long maxLts, Query query)
    {
        PartitionState partitionState = new PartitionState();

        class Processor extends AbstractPartitionVisitor
        {
            public Processor(OpSelectors.PdSelector pdSelector, OpSelectors.DescriptorSelector descriptorSelector, SchemaSpec schema)
            {
                super(pdSelector, descriptorSelector, schema);
            }

            // Whether or not a partition deletion was encountered on this LTS.
            private boolean hadPartitionDeletion = false;
            private final List<Ranges.Range> rangeDeletes = new ArrayList<>();
            private final List<Long> writes = new ArrayList<>();
            private final List<Long> columnDeletes = new ArrayList<>();

            @Override
            public void operation(long lts, long pd, long cd, long m, long opId)
            {
                if (hadPartitionDeletion)
                    return;

                OpSelectors.OperationKind opType = descriptorSelector.operationType(pd, lts, opId);
                switch (opType)
                {
                    case DELETE_RANGE:
                        Query query = rangeSelector.inflate(lts, opId, Query.QueryKind.CLUSTERING_RANGE);
                        Ranges.Range range = query.toRange(lts);
                        rangeDeletes.add(range);
                        partitionState.delete(range, lts);
                        break;
                    case DELETE_SLICE:
                        query = rangeSelector.inflate(lts, opId, Query.QueryKind.CLUSTERING_SLICE);
                        range = query.toRange(lts);
                        rangeDeletes.add(range);
                        partitionState.delete(range, lts);
                        break;
                    case DELETE_ROW:
                        range = new Ranges.Range(cd, cd, true, true, lts);
                        rangeDeletes.add(range);
                        partitionState.delete(cd, lts);
                        break;
                    case DELETE_PARTITION:
                        partitionState.deletePartition(lts);
                        rangeDeletes.clear();
                        writes.clear();
                        columnDeletes.clear();

                        hadPartitionDeletion = true;
                        break;
                    case INSERT_WITH_STATICS:
                    case INSERT:
                    case UPDATE:
                    case UPDATE_WITH_STATICS:
                        if (debugCd != -1 && cd == debugCd)
                            logger.info("Writing {} ({}) at {}/{}", cd, opType, lts, opId);
                        writes.add(opId);
                        break;
                    case DELETE_COLUMN_WITH_STATICS:
                    case DELETE_COLUMN:
                        columnDeletes.add(opId);
                        break;
                    default:
                        throw new IllegalStateException();
                }
            }

            @Override
            protected void beforeLts(long lts, long pd)
            {
                rangeDeletes.clear();
                writes.clear();
                columnDeletes.clear();
                hadPartitionDeletion = false;
            }

            @Override
            protected void afterLts(long lts, long pd)
            {
                if (hadPartitionDeletion)
                    return;

                outer: for (Long opIdBoxed : writes)
                {
                    long opId = opIdBoxed;
                    long cd = descriptorSelector.cd(pd, lts, opId, schema);

                    OpSelectors.OperationKind opType = descriptorSelector.operationType(pd, lts, opId);

                    switch (opType)
                    {
                        case INSERT_WITH_STATICS:
                        case UPDATE_WITH_STATICS:
                            // We could apply static columns during the first iteration, but it's more convenient
                            // to reconcile static-level deletions.
                            partitionState.writeStaticRow(descriptorSelector.sds(pd, cd, lts, opId, schema),
                                                          lts);
                        case INSERT:
                        case UPDATE:
                            if (!query.match(cd))
                            {
                                if (debugCd != -1 && cd == debugCd)
                                    logger.info("Hiding {} at {}/{} because there was no query match", debugCd, lts, opId);
                                continue outer;
                            }

                            for (Ranges.Range range : rangeDeletes)
                            {
                                if (range.timestamp >= lts && range.contains(cd))
                                {
                                    if (debugCd != -1 && cd == debugCd)
                                        logger.info("Hiding {} at {}/{} because of range tombstone {}", debugCd, lts, opId, range);
                                    continue outer;
                                }
                            }

                            partitionState.write(cd,
                                                 descriptorSelector.vds(pd, cd, lts, opId, schema),
                                                 lts,
                                                 opType == OpSelectors.OperationKind.INSERT || opType == OpSelectors.OperationKind.INSERT_WITH_STATICS);
                            break;
                        default:
                            throw new IllegalStateException();
                    }
                }

                outer: for (Long opIdBoxed : columnDeletes)
                {
                    long opId = opIdBoxed;
                    long cd = descriptorSelector.cd(pd, lts, opId, schema);

                    OpSelectors.OperationKind opType = descriptorSelector.operationType(pd, lts, opId);

                    switch (opType)
                    {
                        case DELETE_COLUMN_WITH_STATICS:
                            partitionState.deleteStaticColumns(lts,
                                                               schema.staticColumnsOffset,
                                                               descriptorSelector.columnMask(pd, lts, opId),
                                                               schema.staticColumnsMask());
                        case DELETE_COLUMN:
                            if (!query.match(cd))
                            {
                                if (debugCd != -1 && cd == debugCd)
                                    logger.info("Hiding {} at {}/{} because there was no query match", debugCd, lts, opId);
                                continue outer;
                            }

                            for (Ranges.Range range : rangeDeletes)
                            {
                                if (range.timestamp >= lts && range.contains(cd))
                                {
                                    if (debugCd != -1 && cd == debugCd)
                                        logger.info("Hiding {} at {}/{} because of range tombstone {}", debugCd, lts, opId, range);
                                    continue outer;
                                }
                            }

                            partitionState.deleteRegularColumns(lts,
                                                                cd,
                                                                schema.regularColumnsOffset,
                                                                descriptorSelector.columnMask(pd, lts, opId),
                                                                schema.regularColumnsMask());
                            break;
                    }
                }
            }
        }

        PartitionVisitor partitionVisitor = new Processor(pdSelector, descriptorSelector, schema);

        long currentLts = pdSelector.minLtsFor(pd);

        while (currentLts <= maxLts && currentLts >= 0)
        {
            partitionVisitor.visitPartition(currentLts);
            currentLts = pdSelector.nextLts(currentLts);
        }

        return partitionState;
    }

    public class PartitionState implements Iterable<RowState>
    {
        private final NavigableMap<Long, RowState> rows;
        private RowState staticRow;

        private PartitionState()
        {
            rows = new TreeMap<>();
            if (!schema.staticColumns.isEmpty())
            {
                staticRow = new RowState(STATIC_CLUSTERING,
                                         arr(schema.staticColumns.size(), NIL_DESCR),
                                         arr(schema.staticColumns.size(), NO_TIMESTAMP));
            }
        }

        private void writeStaticRow(long[] staticVds,
                                    long lts)
        {
            if (staticRow != null)
                staticRow = updateRowState(staticRow, schema.staticColumns, STATIC_CLUSTERING, staticVds, lts, false);
        }

        private void write(long cd,
                           long[] vds,
                           long lts,
                           boolean writeParimaryKeyLiveness)
        {
            rows.compute(cd, (cd_, current) -> updateRowState(current, schema.regularColumns, cd, vds, lts, writeParimaryKeyLiveness));
        }

        private void delete(Ranges.Range range,
                            long lts)
        {
            if (range.minBound > range.maxBound)
                return;

            Iterator<Map.Entry<Long, RowState>> iter = rows.subMap(range.minBound, range.minInclusive,
                                                                   range.maxBound, range.maxInclusive)
                                                           .entrySet()
                                                           .iterator();
            while (iter.hasNext())
            {
                Map.Entry<Long, RowState> e = iter.next();
                if (debugCd != -1 && e.getKey() == debugCd)
                    logger.info("Hiding {} at {} because of range tombstone {}", debugCd, lts, range);

                // assert row state doesn't have fresher lts
                iter.remove();
            }
        }

        private void delete(long cd,
                            long lts)
        {
            RowState state = rows.remove(cd);
            if (state != null)
            {
                for (long v : state.lts)
                    assert lts >= v : String.format("Attempted to remove a row with a tombstone that has older timestamp (%d): %s", lts, state);
            }
        }
        public boolean isEmpty()
        {
            return rows.isEmpty();
        }

        private RowState updateRowState(RowState currentState, List<ColumnSpec<?>> columns, long cd, long[] vds, long lts, boolean writePrimaryKeyLiveness)
        {
            if (currentState == null)
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

                currentState = new RowState(cd, vdsCopy, ltss);
            }
            else
            {
                assert currentState.vds.length == vds.length;
                for (int i = 0; i < vds.length; i++)
                {
                    if (vds[i] == UNSET_DESCR)
                        continue;

                    assert lts >= currentState.lts[i] : String.format("Out-of-order LTS: %d. Max seen: %s", lts, currentState.lts[i]); // sanity check; we're iterating in lts order

                    if (currentState.lts[i] == lts)
                    {
                        // Timestamp collision case
                        ColumnSpec<?> column = columns.get(i);
                        if (column.type.compareLexicographically(vds[i], currentState.vds[i]) > 0)
                            currentState.vds[i] = vds[i];
                    }
                    else
                    {
                        currentState.vds[i] = vds[i];
                        assert lts > currentState.lts[i];
                        currentState.lts[i] = lts;
                    }
                }
            }

            if (writePrimaryKeyLiveness)
                currentState.hasPrimaryKeyLivenessInfo = true;

            return currentState;
        }

        private void deleteRegularColumns(long lts, long cd, int columnOffset, BitSet columns, BitSet mask)
        {
            deleteColumns(lts, rows.get(cd), columnOffset, columns, mask);
        }

        private void deleteStaticColumns(long lts, int columnOffset, BitSet columns, BitSet mask)
        {
            deleteColumns(lts, staticRow, columnOffset, columns, mask);
        }

        private void deleteColumns(long lts, RowState state, int columnOffset, BitSet columns, BitSet mask)
        {
            if (state == null)
                return;

            //TODO: optimise by iterating over the columns that were removed by this deletion
            //TODO: optimise final decision to fully remove the column by counting a number of set/unset columns
            boolean allNil = true;
            for (int i = 0; i < state.vds.length; i++)
            {
                if (columns.isSet(columnOffset + i, mask))
                {
                    state.vds[i] = NIL_DESCR;
                    state.lts[i] = NO_TIMESTAMP;
                }
                else if (state.vds[i] != NIL_DESCR)
                {
                    allNil = false;
                }
            }

            if (state.cd != STATIC_CLUSTERING && allNil & !state.hasPrimaryKeyLivenessInfo)
                delete(state.cd, lts);
        }

        private void deletePartition(long lts)
        {
            if (debugCd != -1)
                logger.info("Hiding {} at {} because partition deletion", debugCd, lts);

            rows.clear();
            if (!schema.staticColumns.isEmpty())
            {
                Arrays.fill(staticRow.vds, NIL_DESCR);
                Arrays.fill(staticRow.lts, NO_TIMESTAMP);
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

        public RowState staticRow()
        {
            return staticRow;
        }

        public String toString(SchemaSpec schema)
        {
            StringBuilder sb = new StringBuilder();

            if (staticRow != null)
                sb.append("Static row: " + staticRow.toString(schema)).append("\n");

            for (RowState row : rows.values())
                sb.append(row.toString(schema)).append("\n");

            return sb.toString();
        }
    }

    public static long[] arr(int length, long fill)
    {
        long[] arr = new long[length];
        Arrays.fill(arr, fill);
        return arr;
    }

    public static class RowState
    {
        public boolean hasPrimaryKeyLivenessInfo = false;
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
                   "cd=" + (cd == STATIC_CLUSTERING ? "static" : cd) +
                   ", vds=" + Arrays.toString(vds) +
                   ", lts=" + Arrays.toString(lts) +
                   '}';
        }

        public String toString(SchemaSpec schema)
        {
            return "RowState{" +
                   "cd=" + (cd == STATIC_CLUSTERING ? "static" : cd) +
                   ", vds=" + Arrays.toString(vds) +
                   ", lts=" + Arrays.toString(lts) +
                   ", clustering=" + (cd == STATIC_CLUSTERING ? "static" : Arrays.toString(schema.inflateClusteringKey(cd))) +
                   ", values=" + Arrays.toString(cd == STATIC_CLUSTERING ? schema.inflateStaticColumns(vds) : schema.inflateRegularColumns(vds)) +
                   '}';
        }
    }
}