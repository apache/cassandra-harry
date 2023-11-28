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

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.ddl.ColumnSpec;
import harry.ddl.SchemaSpec;
import harry.operations.Query;
import harry.util.BitSet;
import harry.util.Ranges;

import static harry.generators.DataGenerators.NIL_DESCR;
import static harry.generators.DataGenerators.UNSET_DESCR;
import static harry.model.Model.NO_TIMESTAMP;

public class PartitionState implements Iterable<Reconciler.RowState>
{
    private static final Logger logger = LoggerFactory.getLogger(Reconciler.class);

    final long pd;
    final long debugCd;
    final SchemaSpec schema;
    final List<Long> visitedLts = new ArrayList<>();
    final List<Long> skippedLts = new ArrayList<>();

    // Collected state
    Reconciler.RowState staticRow;
    final NavigableMap<Long, Reconciler.RowState> rows;

    public PartitionState(long pd, long debugCd, SchemaSpec schema)
    {
        this.pd = pd;
        this.rows = new TreeMap<>();
        if (!schema.staticColumns.isEmpty())
        {
            staticRow = new Reconciler.RowState(this,
                                                Reconciler.STATIC_CLUSTERING,
                                                Reconciler.arr(schema.staticColumns.size(), NIL_DESCR),
                                                Reconciler.arr(schema.staticColumns.size(), NO_TIMESTAMP));
        }
        this.debugCd = debugCd;
        this.schema = schema;
    }

    public NavigableMap<Long, Reconciler.RowState> rows()
    {
        return rows;
    }

    public void writeStaticRow(long[] staticVds, long lts)
    {
        if (staticRow != null)
            staticRow = updateRowState(staticRow, schema.staticColumns, Reconciler.STATIC_CLUSTERING, staticVds, lts, false);
    }

    public void write(long cd, long[] vds, long lts, boolean writePrimaryKeyLiveness)
    {
        rows.compute(cd, (cd_, current) -> updateRowState(current, schema.regularColumns, cd, vds, lts, writePrimaryKeyLiveness));
    }

    public void delete(Ranges.Range range, long lts)
    {
        if (range.minBound > range.maxBound)
            return;

        Iterator<Map.Entry<Long, Reconciler.RowState>> iter = rows.subMap(range.minBound, range.minInclusive,
                                                                          range.maxBound, range.maxInclusive)
                                                                  .entrySet()
                                                                  .iterator();
        while (iter.hasNext())
        {
            Map.Entry<Long, Reconciler.RowState> e = iter.next();
            if (debugCd != -1 && e.getKey() == debugCd)
                logger.info("Hiding {} at {} because of range tombstone {}", debugCd, lts, range);

            // assert row state doesn't have fresher lts
            iter.remove();
        }
    }

    public void delete(long cd, long lts)
    {
        Reconciler.RowState state = rows.remove(cd);
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

    private Reconciler.RowState updateRowState(Reconciler.RowState currentState, List<ColumnSpec<?>> columns, long cd, long[] vds, long lts, boolean writePrimaryKeyLiveness)
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

            currentState = new Reconciler.RowState(this, cd, vdsCopy, ltss);
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

    public void deleteRegularColumns(long lts, long cd, int columnOffset, harry.util.BitSet columns, harry.util.BitSet mask)
    {
        deleteColumns(lts, rows.get(cd), columnOffset, columns, mask);
    }

    public void deleteStaticColumns(long lts, int columnOffset, harry.util.BitSet columns, harry.util.BitSet mask)
    {
        deleteColumns(lts, staticRow, columnOffset, columns, mask);
    }

    public void deleteColumns(long lts, Reconciler.RowState state, int columnOffset, harry.util.BitSet columns, BitSet mask)
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

        if (state.cd != Reconciler.STATIC_CLUSTERING && allNil & !state.hasPrimaryKeyLivenessInfo)
            delete(state.cd, lts);
    }

    public void deletePartition(long lts)
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

    public Iterator<Reconciler.RowState> iterator()
    {
        return iterator(false);
    }

    public Iterator<Reconciler.RowState> iterator(boolean reverse)
    {
        if (reverse)
            return rows.descendingMap().values().iterator();

        return rows.values().iterator();
    }

    public Collection<Reconciler.RowState> rows(boolean reverse)
    {
        if (reverse)
            return rows.descendingMap().values();

        return rows.values();
    }

    public Reconciler.RowState staticRow()
    {
        return staticRow;
    }

    public PartitionState apply(Query query)
    {
        PartitionState partitionState = new PartitionState(pd, debugCd, schema);
        partitionState.staticRow = staticRow;
        // TODO: we could improve this if we could get original descriptors
        for (Reconciler.RowState rowState : rows.values())
            if (query.match(rowState.cd))
                partitionState.rows.put(rowState.cd, rowState);

        return partitionState;
    }

    public String toString(SchemaSpec schema)
    {
        StringBuilder sb = new StringBuilder();

        sb.append("Visited LTS: " + visitedLts).append("\n");
        sb.append("Skipped LTS: " + skippedLts).append("\n");

        if (staticRow != null)
            sb.append("Static row: " + staticRow.toString(schema)).append("\n");

        for (Reconciler.RowState row : rows.values())
            sb.append(row.toString(schema)).append("\n");

        return sb.toString();
    }
}
