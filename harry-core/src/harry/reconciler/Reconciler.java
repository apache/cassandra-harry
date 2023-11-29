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
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Run;
import harry.ddl.SchemaSpec;
import harry.model.OpSelectors;
import harry.operations.Query;
import harry.operations.QueryGenerator;
import harry.runner.DataTracker;
import harry.util.Ranges;
import harry.util.StringUtils;
import harry.visitors.GeneratingVisitor;
import harry.visitors.LtsVisitor;
import harry.visitors.ReplayingVisitor;
import harry.visitors.VisitExecutor;

import static harry.generators.DataGenerators.NIL_DESCR;

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

    public static long STATIC_CLUSTERING = NIL_DESCR;

    private final OpSelectors.DescriptorSelector descriptorSelector;
    private final OpSelectors.PdSelector pdSelector;
    private final QueryGenerator rangeSelector;
    private final SchemaSpec schema;

    private final Function<VisitExecutor, LtsVisitor> visitorFactory;

    public Reconciler(Run run)
    {
        this(run,
             (processor) -> new GeneratingVisitor(run, processor));
    }

    public Reconciler(Run run, Function<VisitExecutor, LtsVisitor> ltsVisitorFactory)
    {
        this.descriptorSelector = run.descriptorSelector;
        this.pdSelector = run.pdSelector;
        this.schema = run.schemaSpec;
        this.rangeSelector = run.rangeSelector;
        this.visitorFactory = ltsVisitorFactory;
    }

    private final long debugCd = Long.getLong("harry.reconciler.debug_cd", -1L);

    public PartitionState inflatePartitionState(final long pd, DataTracker tracker, Query query)
    {
        PartitionState partitionState = new PartitionState(pd, debugCd, schema);

        class Processor extends VisitExecutor
        {
            // Whether a partition deletion was encountered on this LTS.
            private boolean hadPartitionDeletion = false;
            private final List<Ranges.Range> rangeDeletes = new ArrayList<>();
            private final List<ReplayingVisitor.Operation> writes = new ArrayList<>();
            private final List<ReplayingVisitor.Operation> columnDeletes = new ArrayList<>();

            @Override
            protected void operation(long lts, long pd, long cd, long opId, OpSelectors.OperationKind opType)
            {
                if (hadPartitionDeletion)
                    return;

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
                        writes.add(new ReplayingVisitor.Operation(cd, opId, opType));
                        break;
                    case DELETE_COLUMN_WITH_STATICS:
                    case DELETE_COLUMN:
                        columnDeletes.add(new ReplayingVisitor.Operation(cd, opId, opType));
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

                outer: for (ReplayingVisitor.Operation op : writes)
                {
                    long opId = op.opId;
                    long cd = op.cd;

                    switch (op.opType)
                    {
                        case INSERT_WITH_STATICS:
                        case UPDATE_WITH_STATICS:
                            // We could apply static columns during the first iteration, but it's more convenient
                            // to reconcile static-level deletions.
                            partitionState.writeStaticRow(descriptorSelector.sds(pd, cd, lts, opId, op.opType, schema),
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
                                                 descriptorSelector.vds(pd, cd, lts, opId, op.opType, schema),
                                                 lts,
                                                 op.opType == OpSelectors.OperationKind.INSERT || op.opType == OpSelectors.OperationKind.INSERT_WITH_STATICS);
                            break;
                        default:
                            throw new IllegalStateException(op.opType.toString());
                    }
                }

                outer: for (ReplayingVisitor.Operation op : columnDeletes)
                {
                    long opId = op.opId;
                    long cd = op.cd;

                    switch (op.opType)
                    {
                        case DELETE_COLUMN_WITH_STATICS:
                            partitionState.deleteStaticColumns(lts,
                                                               schema.staticColumnsOffset,
                                                               descriptorSelector.columnMask(pd, lts, opId, op.opType),
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
                                                                descriptorSelector.columnMask(pd, lts, opId, op.opType),
                                                                schema.regularColumnsMask());
                            break;
                    }
                }
            }

            @Override
            public void shutdown() throws InterruptedException {}
        }

        LtsVisitor visitor = visitorFactory.apply(new Processor());

        long currentLts = pdSelector.minLtsFor(pd);
        long maxStarted = tracker.maxStarted();
        while (currentLts <= maxStarted && currentLts >= 0)
        {
            if (tracker.isFinished(currentLts))
            {
                partitionState.visitedLts.add(currentLts);
                visitor.visit(currentLts);
            }
            else
            {
                partitionState.skippedLts.add(currentLts);
            }

            currentLts = pdSelector.nextLts(currentLts);
        }

        return partitionState;
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
        public final PartitionState partitionState;
        public final long cd;
        public final long[] vds;
        public final long[] lts;

        public RowState(PartitionState partitionState,
                        long cd,
                        long[] vds,
                        long[] lts)
        {
            this.partitionState = partitionState;
            this.cd = cd;
            this.vds = vds;
            this.lts = lts;
        }

        public RowState clone()
        {
            return new RowState(partitionState, cd, Arrays.copyOf(vds, vds.length), Arrays.copyOf(lts, lts.length));
        }

        public String toString()
        {
            return toString(null);
        }

        public String toString(SchemaSpec schema)
        {
            return " rowStateRow("
                   + partitionState.pd +
                   "L, " + cd +
                   (partitionState.staticRow == null ? "" : ", values(" + StringUtils.toString(partitionState.staticRow.vds) + ")") +
                   (partitionState.staticRow == null ? "" : ", lts(" + StringUtils.toString(partitionState.staticRow.lts) + ")") +
                   ", values(" + StringUtils.toString(vds) + ")" +
                   ", lts(" + StringUtils.toString(lts) + ")" +
                   (schema == null ? "" : ", clustering=" + (cd == STATIC_CLUSTERING ? "static" : Arrays.toString(schema.inflateClusteringKey(cd)))) +
                   (schema == null ? "" : ", values=" + Arrays.toString(cd == STATIC_CLUSTERING ? schema.inflateStaticColumns(vds) : schema.inflateRegularColumns(vds))) +
                   ")";
        }
    }
}