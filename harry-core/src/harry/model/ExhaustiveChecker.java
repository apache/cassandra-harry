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
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Configuration;
import harry.data.ResultSetRow;
import harry.ddl.SchemaSpec;
import harry.model.sut.SystemUnderTest;
import harry.runner.AbstractPartitionVisitor;
import harry.runner.PartitionVisitor;
import harry.runner.Query;
import harry.runner.QuerySelector;
import harry.util.BitSet;
import harry.util.Ranges;

import static harry.generators.DataGenerators.NIL_DESCR;
import static harry.generators.DataGenerators.UNSET_DESCR;

public class ExhaustiveChecker implements Model
{
    private static final Logger logger = LoggerFactory.getLogger(ExhaustiveChecker.class);

    public static LongComparator FORWARD_COMPARATOR = Long::compare;
    public static LongComparator REVERSE_COMPARATOR = (a, b) -> Long.compare(b, a);

    protected final OpSelectors.DescriptorSelector descriptorSelector;
    protected final OpSelectors.PdSelector pdSelector;
    protected final OpSelectors.MonotonicClock clock;
    protected final SystemUnderTest sut;
    protected final QuerySelector querySelector;

    protected final DataTracker tracker;

    private final SchemaSpec schema;

    public ExhaustiveChecker(SchemaSpec schema,
                             OpSelectors.PdSelector pdSelector,
                             OpSelectors.DescriptorSelector descriptorSelector,
                             OpSelectors.MonotonicClock clock,
                             QuerySelector querySelector,
                             SystemUnderTest sut)
    {
        this.descriptorSelector = descriptorSelector;
        this.pdSelector = pdSelector;
        this.tracker = new DataTracker();
        this.schema = schema;
        this.sut = sut;
        this.clock = clock;
        this.querySelector = querySelector;
    }

    public void recordEvent(long lts, boolean quorumAchieved)
    {
        tracker.recordEvent(lts, quorumAchieved);
    }

    public DataTracker tracker()
    {
        return tracker;
    }

    public void validatePartitionState(long validationLts, Query query)
    {
        validatePartitionState(validationLts,
                               query,
                               () -> {
                                   while (!Thread.currentThread().isInterrupted())
                                   {
                                       try
                                       {
                                           return SelectHelper.execute(sut, clock, query);
                                       }
                                       catch (Throwable t)
                                       {
                                           logger.error(String.format("Caught error while trying execute query %s", query),
                                                        t);
                                       }
                                   }
                                   throw new RuntimeException("Interrupted");
                               });
    }

    void validatePartitionState(long validationLts, Query query, Supplier<List<ResultSetRow>> rowsSupplier)
    {
        // Before we execute SELECT, we know what was the lts of operation that is guaranteed to be visible
        long visibleLtsBound = tracker.maxCompleteLts();

        // TODO: when we implement a reorder buffer through a bitmap, we can just grab a bitmap _before_,
        //       and know that a combination of `consecutive` + `bitmap` gives us _all possible guaranteed-to-be-seen_ values
        List<ResultSetRow> rows = rowsSupplier.get();

        // by the time SELECT done, we grab max "possible" lts
        long inFlightLtsBound = tracker.maxSeenLts();
        PartitionState partitionState = inflatePartitionState(validationLts, inFlightLtsBound, query);
        NavigableMap<Long, List<Operation>> operations = partitionState.operations;
        LongComparator cmp = FORWARD_COMPARATOR;
        if (query.reverse)
        {
            operations = partitionState.operations.descendingMap();
            cmp = REVERSE_COMPARATOR;
        }

        if (!rows.isEmpty() && operations.isEmpty())
        {
            throw new ValidationException(String.format("Returned rows are not empty, but there were no records in the event log.\nRows: %s\nMax seen LTS: %s\nQuery: %s",
                                                        rows, inFlightLtsBound, query));
        }

        PeekingIterator<ResultSetRow> rowIter = Iterators.peekingIterator(rows.iterator());

        // TODO: these two are here only for debugging/logging purposes
        List<ResultSetRow> validatedRows = new ArrayList<>();
        List<Long> validatedNoRows = new ArrayList<>();
        try
        {
            for (Map.Entry<Long, List<Operation>> entry : operations.entrySet())
            {
                long cd = entry.getKey();
                Iterator<Operation> modificationIter = entry.getValue().iterator();

                // Found a row that is present both in the model and in the resultset
                if (rowIter.hasNext() && rowIter.peek().cd == cd)
                {
                    ResultSetRow row = rowIter.next();
                    RowValidationState validationState = new RowValidationState(row, visibleLtsBound, inFlightLtsBound, partitionState.rangeTombstones);

                    // TODO: We only need to go for as long as we explain every column. In fact, we make state _less_ precise by allowing
                    // to continue moving back in time. So far this hasn't proven to be a source of any issues, but we should fix that.
                    // One of the examples is a deletion followed by no writes, or a row write that completely overwrites all columns.
                    while (modificationIter.hasNext())
                        validationState.process(modificationIter.next());

                    long minLts = Long.MAX_VALUE;
                    for (int col = 0; col < validationState.causingOperations.length; col++)
                    {
                        long colLts = row.lts[col];
                        if (colLts != NO_TIMESTAMP && colLts < minLts)
                            minLts = colLts;

                        long rowValueDescr = row.vds[col];
                        switch (validationState.columnStates[col])
                        {
                            case REMOVED:
                                if (colLts != NO_TIMESTAMP || rowValueDescr != NIL_DESCR)
                                    throw new ValidationException("Inconsistency found: value of the column %d was supposed to be removed", col);
                                break;
                            case OBSERVED:
                                if (colLts == NO_TIMESTAMP || rowValueDescr == NIL_DESCR)
                                    throw new ValidationException("Inconsistency found: value of the column %d was supposed to be observed", col);
                                break;
                            case UNOBSERVED:
                                if (colLts != NO_TIMESTAMP || rowValueDescr != NIL_DESCR)
                                    throw new ValidationException("Inconsistency found: value of the column %d was never written. " +
                                                                  "Row timestamp: %d. " +
                                                                  "Row descriptor: %d",
                                                                  col, colLts, rowValueDescr);
                        }
                    }

                    // for any visible row, we have to make sure it is not shadowed by any range tombstones
                    for (Ranges.Range rt : partitionState.rangeTombstones.shadowedBy(cd, minLts))
                    {
                        if (rt.timestamp <= visibleLtsBound)
                        {
                            throw new ValidationException("Row was supposed to be shadowed by the range tombstone." +
                                                          "\nRow: %s" +
                                                          "\nRange tombstone: %s" +
                                                          "\nMin LTS: %d" +
                                                          "\nVisible LTS Bound: %d",
                                                          row,
                                                          rt,
                                                          minLts,
                                                          visibleLtsBound);
                        }
                    }

                    validatedRows.add(row);
                }
                // Modifications for this clustering are are not visible
                else
                {
                    validateNoRow(cd, visibleLtsBound, modificationIter, partitionState.rangeTombstones);
                    validatedNoRows.add(cd);

                    // Row is not present in the resultset, and we currently look at modifications with a clustering past it
                    if (rowIter.hasNext() && cmp.compare(rowIter.peek().cd, cd) < 0)
                        throw new ValidationException("Couldn't find a corresponding explanation for the row %s in the model. %s",
                                                      rowIter.next(),
                                                      partitionState.operations.get(cd));
                }
            }

            if (rowIter.hasNext())
                throw new ValidationException(String.format("Observed unvalidated rows : %s", Iterators.toString(rowIter)));
        }
        catch (Throwable t)
        {
            throw new ValidationException(String.format("Caught exception while validating the resultset %s." +
                                                        "\nchecker.tracker().forceLts(%dL, %dL)" +
                                                        "\nrun.validator.validatePartition(%dL)" +
                                                        "\nRow Iter Peek: %s" +
                                                        "\nValidated no rows:\n%s" +
                                                        "\nValidated rows:\n%s" +
                                                        "\nRows:\n%s",
                                                        query,
                                                        inFlightLtsBound, visibleLtsBound, validationLts,
                                                        rowIter.hasNext() ? rowIter.peek() : null,
                                                        validatedNoRows,
                                                        validatedRows.stream().map(Object::toString).collect(Collectors.joining(",\n")),
                                                        rows.stream().map(Object::toString).collect(Collectors.joining(",\n"))),
                                          t);
        }
    }

    // there seems to be some issue here, when validating too many in-flight rows
    public void validateNoRow(long cd, long visibleLtsBound, Iterator<Operation> ops, Ranges rangeTombstones)
    {
        // Row was never written
        if (!ops.hasNext())
            return;

        // There should have been at least one removal followed by no live updates
        List<Operation> visibleWrites = new ArrayList<>();
        while (ops.hasNext())
        {
            Operation op = ops.next();
            boolean isVisible = op.lts <= visibleLtsBound;

            switch (descriptorSelector.operationType(op.pd, op.lts, op.opId))
            {
                // we are going from the newest operation to the oldest one;
                // eventually, we're getting to some write. if it should have propagated, we save it to overwrites
                // if we find a row delete
                case WRITE:
                    if (isVisible)
                        visibleWrites.add(op);

                    break;
                case DELETE_COLUMN:
                    // TODO: continue only in case of non-compact storage. In case of compact storage, deletion of all cells results into row deletion
                    if (!descriptorSelector.columnMask(op.pd, op.lts, op.opId).allUnset())
                        continue;
                    // otherwise, fall through, since we can't distinguish this from row delete
                case DELETE_ROW:
                    // row delete, followed by any number of non-propagated writes explains why descriptor is not visible
                    if (!visibleWrites.isEmpty())
                    {
                        long newestVisibleLts = visibleWrites.get(0).lts;
                        if (rangeTombstones.isShadowed(cd, newestVisibleLts))
                            return;

                        // if we have at least one write not shadowed by delete, we have an error
                        throw new ValidationException("While validating %d, expected row not to be visible: a deletion, followed by no overwrites or all incomplete writes, but found %s." +
                                                      "\nRange tombstones: %s",
                                                      cd,
                                                      visibleWrites,
                                                      rangeTombstones.newerThan(newestVisibleLts));
                    }
                    return;
            }
        }

        if (!visibleWrites.isEmpty())
        {
            long newestVisibleLts = visibleWrites.get(0).lts;
            if (rangeTombstones.isShadowed(cd, newestVisibleLts))
                return;

            throw new ValidationException("While validating %d, expected row not to be visible: a deletion, followed by no overwrites or all incomplete writes, but found %s." +
                                          "\nRange tombstones: %s",
                                          cd,
                                          visibleWrites,
                                          rangeTombstones.newerThan(newestVisibleLts));
        }
    }

    public static class PartitionState
    {
        public final NavigableMap<Long, List<Operation>> operations;
        public final Ranges rangeTombstones;

        public PartitionState(NavigableMap<Long, List<Operation>> operations, Ranges rangeTombstones)
        {
            this.operations = operations;
            this.rangeTombstones = rangeTombstones;
        }
    }

    public PartitionState inflatePartitionState(long validationLts, long maxLts, Query query)
    {
        long currentLts = pdSelector.maxLts(validationLts);
        long pd = pdSelector.pd(currentLts, schema);

        if (pd != pdSelector.pd(validationLts, schema))
            throw new ValidationException("Partition descriptor %d doesn't match partition descriptor %d for LTS %d",
                                          pd, pdSelector.pd(validationLts, schema), validationLts);
        NavigableMap<Long, List<Operation>> operations = new TreeMap<>();
        List<Ranges.Range> ranges = new ArrayList<>();

        PartitionVisitor partitionVisitor = new AbstractPartitionVisitor(pdSelector, descriptorSelector, schema)
        {
            public void operation(long lts, long pd, long cd, long m, long opId)
            {
                OpSelectors.OperationKind opType = descriptorSelector.operationType(pd, lts, opId);
                if (opType == OpSelectors.OperationKind.DELETE_RANGE)
                {
                    ranges.add(maybeWrap(lts, opId, querySelector.inflate(lts, opId).toRange(lts)));
                }
                else if (query.match(cd)) // skip descriptors that are out of range
                {
                    operations.computeIfAbsent(cd, (cd_) -> new ArrayList<>());
                    Operation operation = new Operation(pd, cd, lts, opId, opType);
                    operations.get(cd).add(operation);
                }
            }
        };

        while (currentLts >= 0)
        {
            if (currentLts <= maxLts)
                partitionVisitor.visitPartition(currentLts);

            currentLts = pdSelector.prevLts(currentLts);
        }
        return new PartitionState(operations, new Ranges(ranges));
    }

    private static Ranges.Range maybeWrap(long lts, long opId, Ranges.Range range)
    {
        if (logger.isDebugEnabled())
            return new DebugRange(lts, opId, range.minBound, range.maxBound, range.minInclusive, range.maxInclusive, range.timestamp);

        return range;
    }

    private static class DebugRange extends Ranges.Range
    {
        private final long lts;
        private final long opId;

        public DebugRange(long lts, long opId,
                          long minBound, long maxBound, boolean minInclusive, boolean maxInclusive, long timestamp)
        {
            super(minBound, maxBound, minInclusive, maxInclusive, timestamp);
            this.lts = lts;
            this.opId = opId;
        }

        public String toString()
        {
            return super.toString() +
                   "(lts=" + lts +
                   ", opId=" + opId +
                   ')';
        }
    }

    public Configuration.ExhaustiveCheckerConfig toConfig()
    {
        return new Configuration.ExhaustiveCheckerConfig(tracker.maxSeenLts(), tracker.maxCompleteLts());
    }

    public String toString()
    {
        return "ExhaustiveChecker{" + tracker.toString() + '}';
    }

    public class RowValidationState
    {
        private final ColumnState[] columnStates;
        private final Operation[] causingOperations;
        private final PeekingIterator<Long> ltsIterator;
        private final Ranges rangeTombstones;
        private final ResultSetRow row;
        private final long visibleLtsBound;
        private final long inFlightLtsBound;

        public RowValidationState(ResultSetRow row, long visibleLtsBound, long inFlightLtsBound, Ranges rangeTombstones)
        {
            this.row = row;
            this.visibleLtsBound = visibleLtsBound;
            this.inFlightLtsBound = inFlightLtsBound;

            this.columnStates = new ColumnState[row.vds.length];
            Arrays.fill(columnStates, ColumnState.UNOBSERVED);

            this.causingOperations = new Operation[columnStates.length];
            long[] ltsVector = new long[row.lts.length];
            System.arraycopy(row.lts, 0, ltsVector, 0, ltsVector.length);
            this.ltsIterator = Iterators.peekingIterator(ltsIterator(ltsVector).iterator());
            this.rangeTombstones = rangeTombstones;
        }

        public void process(Operation op)
        {
            if (ltsIterator.hasNext() && op.lts > ltsIterator.peek())
                ltsIterator.next();

            assert row.pd == op.pd : String.format("Row and operation descriptors do not match: %d != %d", row.pd, op.pd);

            switch (descriptorSelector.operationType(op.pd, op.lts, op.opId))
            {
                case WRITE:
                    processInsert(op);
                    break;
                case DELETE_ROW:
                    // In case of a visible row, deletion that was followed with a write can be considered equivalent
                    // to a deletion of all column values.
                    processDelete(op, schema.ALL_COLUMNS_BITSET);
                    break;
                case DELETE_COLUMN:
                    BitSet mask = descriptorSelector.columnMask(row.pd, op.lts, op.opId);
                    if (mask.allUnset())
                        throw new IllegalArgumentException("Can't have a delete column query with no columns set. Column mask: " + mask);

                    processDelete(op, mask);
                    break;
            }
        }

        private void transitionState(int idx, ColumnState newState, Operation modification)
        {
            ColumnState oldState = columnStates[idx];
            switch (newState)
            {
                case UNOBSERVED:
                    throw new IllegalArgumentException("Can not transition to UNOBSERVED state");
                case REMOVED:
                    if (!(oldState == ColumnState.UNOBSERVED || oldState == ColumnState.REMOVED))
                        throw new ValidationException("Can not transition from %s to %s.", oldState, newState);
                    break;
                case OBSERVED:
                    if (!(oldState == ColumnState.OBSERVED || oldState == ColumnState.UNOBSERVED))
                        throw new ValidationException("Can not transition from %s to %s.", oldState, newState);
                    break;
            }
            columnStates[idx] = newState;
            causingOperations[idx] = modification;
        }

        private void processInsert(Operation op)
        {
            if (op.lts > inFlightLtsBound)
                throw new IllegalStateException(String.format("Observed LTS not yet recorded by this model: %s. Max seen LTS: %s",
                                                              op.lts, inFlightLtsBound));

            boolean isVisible = op.lts <= visibleLtsBound;

            long[] inflatedDescriptors = descriptorSelector.vds(op.pd, op.cd, op.lts, op.opId, schema);
            for (int col = 0; col < row.lts.length; col++)
            {
                final long valueDescriptor = inflatedDescriptors[col];

                // Write is visible
                if (row.vds[col] == valueDescriptor && row.lts[col] == op.lts)
                {
                    transitionState(col, ColumnState.OBSERVED, op);
                    continue;
                }

                if (!isVisible                            // write has never propagated
                    || valueDescriptor == UNSET_DESCR     // this modification did not make this write
                    || (columnStates[col] == ColumnState.REMOVED && causingOperations[col].lts >= op.lts)  // confirmed that this column was removed later
                    || (columnStates[col] == ColumnState.OBSERVED && causingOperations[col].lts >= op.lts)  // we could confirm the overwrite earlier
                    // TODO: that won't work. To reproduce this, take testDetectsRemovedColumn with range tombstones. Removed column will have a timestamp of min long,
                    //       and _any_ range tombstone is going to be able to shadow it.
                    || rangeTombstones.isShadowed(row.cd, row.lts[col])) // if this row's lts is shadowed, we can be certain that whole row is shadowed
                    continue;

                throw new ValidationException("Error caught while validating column %d. " +
                                              "Expected value: %d. " +
                                              "Modification should have been visible but was not." +
                                              "\nOperation: %s" +
                                              "\nRow: %s" +
                                              "\nRow ID: %d" +
                                              "\nColumn States: %s " +
                                              "\nRange tombstones: %s",
                                              col,
                                              valueDescriptor,
                                              op,
                                              row,
                                              descriptorSelector.rowId(row.pd, row.lts[col], row.cd),
                                              Arrays.toString(columnStates),
                                              rangeTombstones);
            }
        }

        public void processDelete(Operation op, BitSet mask)
        {
            boolean isVisible = op.lts <= visibleLtsBound;

            for (int col = 0; col < columnStates.length; col++)
            {
                // Deletion must have propagated
                if (mask.isSet(col) && row.lts[col] == NO_TIMESTAMP)
                {
                    transitionState(col, ColumnState.REMOVED, op);
                    continue;
                }

                if (!isVisible
                    || mask.isSet(col)
                    || columnStates[col] != ColumnState.OBSERVED
                    || op.lts < causingOperations[col].lts)
                    continue;

                throw new ValidationException("Error caught wile validating column %d. " +
                                              "Delete operation with lts %s should have been visible or shadowed by the later update, but was not. " +
                                              "\nOperation: %s" +
                                              "\nRow: %s" +
                                              "\nColumn States: %s",
                                              col, op.lts,
                                              op, row, Arrays.toString(columnStates));
            }
        }

        public String toString()
        {
            return String.format("Validated: %s." +
                                 "\nObserved timestamps: %s",
                                 Arrays.toString(columnStates),
                                 row);
        }
    }

    @VisibleForTesting
    public void forceLts(long maxSeen, long maxComplete)
    {
        tracker.forceLts(maxSeen, maxComplete);
    }

    public static class Operation implements Comparable<Operation>
    {
        public final long pd;
        public final long lts;
        public final long cd;
        public final long opId;
        public final OpSelectors.OperationKind op;

        public Operation(long pd, long cd, long lts, long opId, OpSelectors.OperationKind op)
        {
            this.pd = pd;
            this.lts = lts;
            this.cd = cd;
            this.opId = opId;
            this.op = op;
        }

        private static final Comparator<Operation> comparator = Comparator.comparingLong((Operation a) -> a.lts);

        public int compareTo(Operation other)
        {
            // reverse order
            return comparator.compare(other, this);
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Operation operation = (Operation) o;
            return pd == operation.pd &&
                   lts == operation.lts &&
                   cd == operation.cd &&
                   opId == operation.opId;
        }

        public int hashCode()
        {
            return Objects.hash(pd, lts, cd, opId);
        }

        public String toString()
        {
            return "Operation{" +
                   "pd=" + pd +
                   ", cd=" + cd +
                   ", lts=" + lts +
                   ", opId=" + opId +
                   ", op=" + op +
                   '}';
        }
    }

    public enum ColumnState
    {
        UNOBSERVED,
        REMOVED,
        OBSERVED
    }

    public static interface LongComparator
    {
        int compare(long o1, long o2);
    }

    public static List<Long> ltsIterator(long[] lts)
    {
        long[] sorted = Arrays.copyOf(lts, lts.length);
        Arrays.sort(sorted);
        List<Long> deduplicated = new ArrayList<>(lts.length);
        for (int i = 0; i < sorted.length; i++)
        {
            if (sorted[i] <= 0)
                continue;

            if (deduplicated.size() == 0 || deduplicated.get(deduplicated.size() - 1) != sorted[i])
                deduplicated.add(sorted[i]);
        }

        return deduplicated;
    }
}
