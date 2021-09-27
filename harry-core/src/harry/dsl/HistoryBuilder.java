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

package harry.dsl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import harry.core.Run;
import harry.model.OpSelectors;
import harry.visitors.ReplayingVisitor;
import harry.visitors.VisitExecutor;

import static harry.model.OpSelectors.DefaultPdSelector.PARTITION_DESCRIPTOR_STREAM_ID;

// TODO: we could use some sort of compact data structure or file format for navigable operation history
public class HistoryBuilder implements Iterable<ReplayingVisitor.Visit>
{
    private final Run run;
    private final List<ReplayingVisitor.Visit> log;

    private long lts;
    private final Set<Long> pds = new HashSet<>();

    public Map<Long, NavigableSet<Long>> pdToLtsMap = new HashMap<>();

    private int partitions;

    public HistoryBuilder(Run run)
    {
        this.run = run;
        this.log = new ArrayList<>();
        this.lts = 0;
        this.partitions = 0;

        assert run.pdSelector instanceof PdSelector;
        ((PdSelector) run.pdSelector).historyBuilder = this;
    }

    public Iterator<ReplayingVisitor.Visit> iterator()
    {
        return log.iterator();
    }

    public static class PdSelector extends OpSelectors.PdSelector
    {
        // We can only lazy-initialise it since history builder is created after pd selector
        private HistoryBuilder historyBuilder;

        protected long pd(long lts)
        {
            return historyBuilder.log.get((int) lts).pd;
        }

        public long nextLts(long lts)
        {
            Long next = historyBuilder.pdToLtsMap.get(pd(lts)).higher(lts);
            if (null == next)
                return -1;
            return next;
        }

        public long prevLts(long lts)
        {
            Long prev = historyBuilder.pdToLtsMap.get(pd(lts)).lower(lts);
            if (null == prev)
                return -1;
            return prev;
        }

        public long maxLtsFor(long pd)
        {
            return historyBuilder.pdToLtsMap.get(pd).last();
        }

        public long minLtsAt(long position)
        {
            return historyBuilder.pdToLtsMap.get(historyBuilder.pd(position)).first();
        }

        public long minLtsFor(long pd)
        {
            return historyBuilder.pdToLtsMap.get(pd).first();
        }

        public long positionFor(long lts)
        {
            return historyBuilder.position(pd(lts));
        }
    }

    private static abstract class Step
    {
        public abstract ReplayingVisitor.Batch toBatch(long pd, long lts, long m, LongSupplier opIdSupplier);
    }

    private class BatchStep extends Step
    {
        private final List<OperationStep> steps;

        protected BatchStep(List<OperationStep> steps)
        {
            this.steps = steps;
        }

        public ReplayingVisitor.Batch toBatch(long pd, long lts, long m, LongSupplier opIdSupplier)
        {
            ReplayingVisitor.Operation[] ops = new ReplayingVisitor.Operation[steps.size()];
            for (int i = 0; i < ops.length; i++)
            {
                OperationStep opStep = steps.get(i);
                long opId = opIdSupplier.getAsLong();
                long cd = HistoryBuilder.this.cd(pd, lts, opId);
                ops[i] = op(cd, opId, opStep.opType);
            }

            return HistoryBuilder.batch(m, ops);
        }
    }

    private class OperationStep extends Step
    {
        private final OpSelectors.OperationKind opType;

        protected OperationStep(OpSelectors.OperationKind opType)
        {
            this.opType = opType;
        }

        public ReplayingVisitor.Batch toBatch(long pd, long lts, long m, LongSupplier opIdSupplier)
        {
            long opId = opIdSupplier.getAsLong();
            long cd = HistoryBuilder.this.cd(pd, lts, opId);
            return HistoryBuilder.batch(m,
                                        HistoryBuilder.op(cd, opIdSupplier.getAsLong(), opType));
        }
    }

    public PartitionBuilder nextPartition()
    {
        long pd = pd(partitions++);
        return new PartitionBuilder(pd);
    }

    // Ideally, we'd like to make these more generic
    private long pd(long position)
    {
        long pd = run.schemaSpec.adjustPdEntropy(run.rng.prev(position, PARTITION_DESCRIPTOR_STREAM_ID));
        pds.add(pd);
        return pd;
    }

    private long position(long pd)
    {
        return run.rng.next(pd, PARTITION_DESCRIPTOR_STREAM_ID);
    }

    protected long cd(long pd, long lts, long opId)
    {
        return run.descriptorSelector.cd(pd, lts, opId, run.schemaSpec);
    }

    public class PartitionBuilder implements OperationBuilder<PartitionBuilder>
    {

        final List<Step> steps = new ArrayList<>();
        final long pd;

        boolean strictOrder = true;
        boolean sequentially = true;

        boolean finished = false;

        public PartitionBuilder(long pd)
        {
            this.pd = pd;
        }

        public BatchBuilder<PartitionBuilder> batch()
        {
            return new BatchBuilder<>(this, steps::add);
        }

        /**
         * Execute operations listed by users of this PartitionBuilder with same logical timestamp.
         */
        public PartitionBuilder simultaneously()
        {
            this.sequentially = false;
            return this;
        }

        /**
         * Execute operations listed by users of this PartitionBuilder with monotonically increasing timestamps,
         * giving each operation its own timestamp. Timestamp order can be determined by `#randomOrder` / `#strictOrder`.
         */
        public PartitionBuilder sequentially()
        {
            this.sequentially = true;
            return this;
        }

        /**
         * Execute operations listed by users of this PartitionBuilder in random order
         */
        public PartitionBuilder randomOrder()
        {
            strictOrder = false;
            return this;
        }

        /**
         * Execute operations listed by users of this PartitionBuilder in the order given by the user
         */
        public PartitionBuilder strictOrder()
        {
            strictOrder = true;
            return this;
        }

        public PartitionBuilder partitionDelete()
        {
            return step(OpSelectors.OperationKind.DELETE_PARTITION);
        }

        public PartitionBuilder partitionDeletions(int n)
        {
            for (int i = 0; i < n; i++)
                partitionDelete();
            return this;
        }

        public PartitionBuilder update()
        {
            return step(OpSelectors.OperationKind.UPDATE);
        }

        public PartitionBuilder updates(int n)
        {
            for (int i = 0; i < n; i++)
                update();
            return this;
        }

        public PartitionBuilder insert()
        {
            return step(OpSelectors.OperationKind.INSERT);
        }

        public PartitionBuilder inserts(int n)
        {
            for (int i = 0; i < n; i++)
                insert();
            return this;
        }

        public PartitionBuilder delete()
        {
            return step(OpSelectors.OperationKind.DELETE_ROW);
        }

        public PartitionBuilder deletes(int n)
        {
            for (int i = 0; i < n; i++)
                delete();
            return this;
        }

        public PartitionBuilder columnDelete()
        {
            return step(OpSelectors.OperationKind.DELETE_COLUMN_WITH_STATICS);
        }

        public PartitionBuilder columnDeletes(int n)
        {
            for (int i = 0; i < n; i++)
                columnDelete();

            return this;
        }

        public PartitionBuilder rangeDelete()
        {
            return step(OpSelectors.OperationKind.DELETE_RANGE);
        }

        public PartitionBuilder rangeDeletes(int n)
        {
            for (int i = 0; i < n; i++)
                rangeDelete();

            return this;
        }

        public PartitionBuilder sliceDelete()
        {
            return step(OpSelectors.OperationKind.DELETE_SLICE);
        }

        public PartitionBuilder sliceDeletes(int n)
        {
            for (int i = 0; i < n; i++)
                sliceDelete();

            return this;
        }

        public PartitionBuilder partitionBuilder()
        {
            return this;
        }

        public HistoryBuilder finish()
        {
            assert !finished;
            finished = true;

            if (!strictOrder)
                // TODO: In the future/for large sets we could avoid generating the values and just generate them on the fly:
                // https://lemire.me/blog/2017/09/18/visiting-all-values-in-an-array-exactly-once-in-random-order/
                // we could just save the rules for generation, for example
                Collections.shuffle(steps);

            addSteps(steps);
            steps.clear();
            return HistoryBuilder.this;
        }

        void addSteps(List<Step> steps)
        {
            List<ReplayingVisitor.Batch> batches = new ArrayList<>();
            Counter m = new Counter();
            Counter opId = new Counter();
            for (Step step : steps)
            {
                batches.add(step.toBatch(pd, lts, m.get(), opId::getAndIncrement));

                if (sequentially)
                {
                    assert lts == log.size();
                    addToLog(pd, batches);
                    m.reset();
                }
                else
                {
                    m.increment();
                }

                opId.reset();
            }

            // If we were generating steps for the partition with same LTS, add remaining steps
            if (!batches.isEmpty())
            {
                assert !sequentially;
                addToLog(pd, batches);
            }
        }

        PartitionBuilder step(OpSelectors.OperationKind opType)
        {
            steps.add(new OperationStep(opType));
            return this;
        }
    }

    private void addToLog(long pd, List<ReplayingVisitor.Batch> batches)
    {
        pdToLtsMap.compute(pd, (ignore, ltss) -> {
            if (null == ltss)
                ltss = new TreeSet<>();
            ltss.add(lts);
            return ltss;
        });

        log.add(visit(lts++, pd, batches.toArray(new ReplayingVisitor.Batch[0])));
        batches.clear();
    }

    private static class Counter
    {
        long i;
        void reset()
        {
            i = 0;
        }

        long increment()
        {
            return i++;
        }

        long getAndIncrement()
        {
            return i++;
        }

        long get()
        {
            return i;
        }
    }

    public class BatchBuilder<T extends OperationBuilder<?>> implements OperationBuilder<BatchBuilder<T>>
    {
        final T operationBuilder;
        final List<OperationStep> steps = new ArrayList<>();
        final Consumer<Step> addStep;
        boolean strictOrder;

        boolean finished = false;
        public BatchBuilder(T operationBuilder,
                            Consumer<Step> addStep)
        {
            this.operationBuilder = operationBuilder;
            this.addStep = addStep;
        }

        public BatchBuilder<T> randomOrder()
        {
            this.strictOrder = false;
            return this;
        }

        public BatchBuilder<T> strictOrder()
        {
            this.strictOrder = true;
            return this;
        }

        public T finish()
        {
            assert !finished;
            finished = true;
            if (!strictOrder)
                // TODO
                Collections.shuffle(steps);

            addStep.accept(new BatchStep(steps));
            return operationBuilder;
        }

        public BatchBuilder<T> partitionDelete()
        {
            return step(OpSelectors.OperationKind.DELETE_PARTITION);
        }

        public BatchBuilder<T> partitionDeletions(int n)
        {
            for (int i = 0; i < n; i++)
                partitionDelete();
            return this;
        }

        public BatchBuilder<T> update()
        {
            return step(OpSelectors.OperationKind.UPDATE_WITH_STATICS);
        }

        public BatchBuilder<T> updates(int n)
        {
            for (int i = 0; i < n; i++)
                update();
            return this;
        }

        public BatchBuilder<T> insert()
        {
            return step(OpSelectors.OperationKind.INSERT_WITH_STATICS);
        }

        BatchBuilder<T> step(OpSelectors.OperationKind opType)
        {
            steps.add(new OperationStep(opType));
            return this;
        }

        public BatchBuilder<T> inserts(int n)
        {
            for (int i = 0; i < n; i++)
                insert();
            return this;
        }

        public BatchBuilder<T> delete()
        {
            return step(OpSelectors.OperationKind.DELETE_ROW);
        }

        public BatchBuilder<T> deletes(int n)
        {
            for (int i = 0; i < n; i++)
                delete();
            return this;
        }

        public BatchBuilder<T> columnDelete()
        {
            return step(OpSelectors.OperationKind.DELETE_COLUMN_WITH_STATICS);
        }

        public BatchBuilder<T> columnDeletes(int n)
        {
            for (int i = 0; i < n; i++)
                columnDelete();

            return this;
        }

        public BatchBuilder<T> rangeDelete()
        {
            return step(OpSelectors.OperationKind.DELETE_RANGE);
        }

        public BatchBuilder<T> rangeDeletes(int n)
        {
            for (int i = 0; i < n; i++)
                rangeDelete();
            return this;
        }

        public BatchBuilder<T> sliceDelete()
        {
            return step(OpSelectors.OperationKind.DELETE_SLICE);
        }

        public BatchBuilder<T> sliceDeletes(int n)
        {
            for (int i = 0; i < n; i++)
                sliceDelete();
            return this;
        }

        public PartitionBuilder partitionBuilder()
        {
            return operationBuilder.partitionBuilder();
        }
    }

    public interface OperationBuilder<T extends OperationBuilder<?>>
    {
        T randomOrder();
        T strictOrder();
        T partitionDelete();
        T partitionDeletions(int n);
        T update();
        T updates(int n);
        T insert();
        T inserts(int n);
        T delete();
        T deletes(int n);
        T columnDelete();
        T columnDeletes(int n);
        T rangeDelete();
        T rangeDeletes(int n);
        T sliceDelete();
        T sliceDeletes(int n);
        PartitionBuilder partitionBuilder();
    }

    public static ReplayingVisitor.Visit visit(long lts, long pd, ReplayingVisitor.Batch... ops)
    {
        return new ReplayingVisitor.Visit(lts, pd, ops);
    }

    public static ReplayingVisitor.Batch batch(long m, ReplayingVisitor.Operation... ops)
    {
        return new ReplayingVisitor.Batch(m, ops);
    }

    public static ReplayingVisitor.Operation op(long cd, long opId, OpSelectors.OperationKind opType)
    {
        return new ReplayingVisitor.Operation(cd, opId, opType);
    }

    public ReplayingVisitor visitor(VisitExecutor executor)
    {
        return new ReplayingVisitor(executor)
        {
            public Visit getVisit(long lts)
            {
                assert log.size() > lts : String.format("Log: %s, lts: %d", log, lts);
                return log.get((int) lts);
            }

            public void replayAll(Run run)
            {
                long maxLts = HistoryBuilder.this.lts;
                while (true)
                {
                    long lts = run.clock.currentLts();
                    if (lts >= maxLts)
                        return;
                    visit(lts);
                    run.clock.nextLts();
                }
            }
        };
    }
}
