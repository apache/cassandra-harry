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

import java.util.EnumMap;
import java.util.Map;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import harry.core.Configuration;
import harry.ddl.SchemaSpec;
import harry.generators.PCGFastPure;
import harry.generators.RngUtils;
import harry.generators.Surjections;
import harry.generators.distribution.Distribution;
import harry.util.BitSet;

import static harry.generators.DataGenerators.UNSET_DESCR;

/**
 * Row (uninflated) data selectors. Not calling them generators, since their output is entirely
 * deterministic, and for each input they are able to produce a single output.
 * <p>
 * This is more or less a direct translation of the formalization.
 * <p>
 * All functions implemented by this interface have to _always_ produce same outputs for same inputs.
 * Most of the functions, with the exception of real-time clock translations, shouold be pure.
 * <p>
 * Functions that are reverse of their coutnerparts are prefixed with "un"
 */
public interface OpSelectors
{
    public static interface Rng
    {
        long randomNumber(long i, long stream);

        long sequenceNumber(long r, long stream);

        default long next(long r)
        {
            return next(r, 0);
        }

        long next(long r, long stream);

        long prev(long r, long stream);

        default long prev(long r)
        {
            return next(r, 0);
        }
    }

    /**
     * Clock is a component responsible for mapping _logical_ timestamps to _real-time_ ones.
     * When reproducing test failures, and for validation purposes, a snapshot of such clock can
     * be taken to map a real-time timestamp from the value retrieved from the database in order
     * to map it back to the logical timestamp of the operation that wrote this value.
     */
    public static interface MonotonicClock
    {
        long rts(long lts);

        long lts(long rts);

        long nextLts();

        long maxLts();

        public Configuration.ClockConfiguration toConfig();
    }

    public static interface MonotonicClockFactory
    {
        public MonotonicClock make();
    }

    // TODO: move to DescriptorSelector, makes no sense to split them

    /**
     * *Partition descriptor selector* controls how partitions is selected based on the current logical
     * timestamp. Default implementation is a sliding window of partition descriptors that will visit
     * one partition after the other in the window `slide_after_repeats` times. After that will
     * retire one partition descriptor, and pick one instead of it.
     */
    public abstract class PdSelector
    {
        @VisibleForTesting
        protected abstract long pd(long lts);

        public long pd(long lts, SchemaSpec schema)
        {
            return schema.adjustPdEntropy(pd(lts));
        }

        // previous and next LTS with that will yield same pd
        public abstract long nextLts(long lts);

        public abstract long prevLts(long lts);

        public abstract long maxLts(long lts);

        public abstract long minLtsAt(long position);

        public abstract long minLtsFor(long pd);

        public abstract long positionFor(long lts);
    }

    public static interface PdSelectorFactory
    {
        public PdSelector make(Rng rng);
    }

    public static interface DescriptorSelectorFactory
    {
        public DescriptorSelector make(OpSelectors.Rng rng, SchemaSpec schemaSpec);
    }

    /**
     * DescriptorSelector controls how clustering descriptors are picked within the partition:
     * how many rows there can be in a partition, how many rows will be visited for a logical timestamp,
     * how many operations there will be in batch, what kind of operations there will and how often
     * each kind of operation is going to occur.
     */
    public abstract class DescriptorSelector
    {
        public abstract int numberOfModifications(long lts);

        public abstract int opsPerModification(long lts);

        public abstract int maxPartitionSize();

        public abstract boolean isCdVisitedBy(long pd, long lts, long cd);

        // clustering descriptor is calculated using operation id and not modification id, since
        // value descriptors are calculated using modification ids.
        public long cd(long pd, long lts, long opId, SchemaSpec schema)
        {
            return schema.adjustCdEntropy(cd(pd, lts, opId));
        }

        /**
         * Currently, we do not allow visiting the same row more than once per lts, which means that:
         * <p>
         * * `max(opId)` returned `cds` have to be unique for any `lts/pd` pair
         * * {@code max(opId) < maxPartitionSize}
         */
        @VisibleForTesting
        protected abstract long cd(long pd, long lts, long opId);

        public long randomCd(long pd, long entropy, SchemaSpec schema)
        {
            return schema.adjustCdEntropy(randomCd(pd, entropy));
        }

        public abstract long randomCd(long pd, long entropy);

        @VisibleForTesting
        protected abstract long vd(long pd, long cd, long lts, long opId, int col);

        public long[] vds(long pd, long cd, long lts, long opId, SchemaSpec schema)
        {
            long[] vds = new long[schema.regularColumns.size()];
            BitSet mask = columnMask(pd, cd, opId);

            for (int col = 0; col < vds.length; col++)
            {
                if (mask.isSet(col))
                {
                    long vd = vd(pd, cd, lts, opId, col);
                    vds[col] = schema.regularColumns.get(col).adjustEntropyDomain(vd);
                }
                else
                {
                    vds[col] = UNSET_DESCR;
                }
            }
            return vds;
        }

        public abstract OperationKind operationType(long pd, long lts, long opId);

        public abstract BitSet columnMask(long pd, long lts, long opId);

        // TODO: why is this one unused?
        public abstract long rowId(long pd, long lts, long cd);

        public abstract long modificationId(long pd, long cd, long lts, long vd, int col);
    }

    public static class PCGFast implements OpSelectors.Rng
    {
        private final long seed;

        public PCGFast(long seed)
        {
            this.seed = seed;
        }

        public long randomNumber(long i, long stream)
        {
            return PCGFastPure.shuffle(PCGFastPure.advanceState(seed, i, stream));
        }

        public long sequenceNumber(long r, long stream)
        {
            return PCGFastPure.distance(seed, PCGFastPure.unshuffle(r), stream);
        }

        public long next(long r, long stream)
        {
            return PCGFastPure.next(r, stream);
        }

        public long prev(long r, long stream)
        {
            return PCGFastPure.previous(r, stream);
        }
    }

    /**
     * Generates partition descriptors, based on LTS as if we had a sliding window.
     * <p>
     * Each {@code windowSize * switchAfter} steps, we move the window by one, effectively
     * expiring one partition descriptor, and adding one partition descriptor to the window.
     * <p>
     * For any LTS, we can calculate previous and next LTS on which it will visit the same
     * partition
     */
    public static class DefaultPdSelector extends OpSelectors.PdSelector
    {
        public final static long PARTITION_DESCRIPTOR_STREAM_ID = 0x706b;

        private final OpSelectors.Rng rng;
        private final long slideAfterRepeats;
        private final long switchAfter;
        private final long windowSize;

        public DefaultPdSelector(OpSelectors.Rng rng, long windowSize, long slideAfterRepeats)
        {
            this.rng = rng;
            this.slideAfterRepeats = slideAfterRepeats;
            this.windowSize = windowSize;
            this.switchAfter = windowSize * slideAfterRepeats;
        }

        protected long pd(long lts)
        {
            return rng.randomNumber(positionFor(lts), PARTITION_DESCRIPTOR_STREAM_ID);
        }

        public long minLtsAt(long position)
        {
            if (position < windowSize)
                return position;

            long windowStart = (position - (windowSize - 1)) * slideAfterRepeats * windowSize;
            return windowStart + windowSize - 1;
        }

        public long minLtsFor(long pd)
        {
            return minLtsAt(rng.sequenceNumber(pd, PARTITION_DESCRIPTOR_STREAM_ID));
        }

        public long positionFor(long lts)
        {
            long windowStart = lts / switchAfter;
            return windowStart + lts % windowSize;
        }

        public long nextLts(long lts)
        {
            long slideCount = lts / switchAfter;
            long positionInCycle = lts - slideCount * switchAfter;
            long nextRepeat = positionInCycle / windowSize + 1;

            if (nextRepeat > slideAfterRepeats ||
                (nextRepeat == slideAfterRepeats && (positionInCycle % windowSize) == 0))
                return -1;

            // last cycle before window slides; next window will have shifted by one
            if (nextRepeat == slideAfterRepeats)
                positionInCycle -= 1;

            return slideCount * switchAfter + windowSize + positionInCycle;
        }

        public long prevLts(long lts)
        {
            long slideCount = lts / switchAfter;
            long positionInCycle = lts - slideCount * switchAfter;
            long prevRepeat = positionInCycle / windowSize - 1;

            if (lts < windowSize ||
                prevRepeat < -1 ||
                (prevRepeat == -1 && (positionInCycle % windowSize) == (windowSize - 1)))
                return -1;

            // last cycle before window slides; next window will have shifted by one
            if (prevRepeat == -1)
                positionInCycle += 1;

            return slideCount * switchAfter - windowSize + positionInCycle;
        }

        public long maxLts(long lts)
        {
            long windowStart = lts / switchAfter;
            long position = windowStart + lts % windowSize;

            return position * switchAfter + (slideAfterRepeats - 1) * windowSize;
        }

        public String toString()
        {
            return "DefaultPdSelector{" +
                   "slideAfterRepeats=" + slideAfterRepeats +
                   ", windowSize=" + windowSize +
                   '}';
        }
    }

    public static ColumnSelectorBuilder columnSelectorBuilder()
    {
        return new ColumnSelectorBuilder();
    }

    // TODO: add weights/probabilities to this
    // TODO: this looks like a hierarchical surjection
    public static class ColumnSelectorBuilder
    {
        private Map<OperationKind, Surjections.Surjection<BitSet>> m;

        public ColumnSelectorBuilder()
        {
            this.m = new EnumMap<>(OperationKind.class);
        }

        public ColumnSelectorBuilder forAll(int regularColumnsCount)
        {
            return forAll(BitSet.surjection(regularColumnsCount));
        }

        public ColumnSelectorBuilder forAll(Surjections.Surjection<BitSet> orig)
        {
            for (OperationKind type : OperationKind.values())
            {
                Surjections.Surjection<BitSet> gen = orig;
                if (type == OperationKind.DELETE_COLUMN)
                {
                    gen = (descriptor) -> {
                        while (true)
                        {
                            BitSet bitSet = orig.inflate(descriptor);
                            if (!bitSet.allUnset())
                                return bitSet;

                            descriptor = RngUtils.next(descriptor);
                        }
                    };
                }
                this.m.put(type, gen);
            }
            return this;
        }

        public ColumnSelectorBuilder forAll(BitSet... pickFrom)
        {
            return forAll(Surjections.pick(pickFrom));
        }

        public ColumnSelectorBuilder forWrite(Surjections.Surjection<BitSet> gen)
        {
            m.put(OperationKind.WRITE, gen);
            return this;
        }

        public ColumnSelectorBuilder forWrite(BitSet pickFrom)
        {
            return forWrite(Surjections.pick(pickFrom));
        }

        public ColumnSelectorBuilder forDelete(Surjections.Surjection<BitSet> gen)
        {
            m.put(OperationKind.DELETE_ROW, gen);
            return this;
        }

        public ColumnSelectorBuilder forDelete(BitSet pickFrom)
        {
            return forDelete(Surjections.pick(pickFrom));
        }

        public ColumnSelectorBuilder forColumnDelete(Surjections.Surjection<BitSet> gen)
        {
            m.put(OperationKind.DELETE_COLUMN, gen);
            return this;
        }

        public ColumnSelectorBuilder forColumnDelete(BitSet pickFrom)
        {
            return forColumnDelete(Surjections.pick(pickFrom));
        }

        public Function<OperationKind, Surjections.Surjection<BitSet>> build()
        {
            return m::get;
        }
    }

    // TODO: this can actually be further improved upon. Maybe not generation-wise, this part seems to be ok,
    //       but in the way it is hooked up with the rest of the system
    public static class HierarchicalDescriptorSelector extends DefaultDescriptorSelector
    {
        private final int[] fractions;

        public HierarchicalDescriptorSelector(Rng rng,
                                              // how many parts (at most) each subsequent "level" should contain
                                              int[] fractions,
                                              Function<OperationKind, Surjections.Surjection<BitSet>> columnMaskSelector,
                                              Surjections.Surjection<OperationKind> operationTypeSelector,
                                              Distribution modificationsPerLtsDistribution,
                                              Distribution rowsPerModificationsDistribution,
                                              int maxPartitionSize)
        {
            super(rng, columnMaskSelector, operationTypeSelector, modificationsPerLtsDistribution, rowsPerModificationsDistribution, maxPartitionSize);
            this.fractions = fractions;
        }

        @Override
        public long cd(long pd, long lts, long opId, SchemaSpec schema)
        {
            if (schema.clusteringKeys.size() <= 1)
                return schema.adjustCdEntropy(super.cd(pd, lts, opId));

            int partitionSize = maxPartitionSize();
            int clusteringOffset = clusteringOffset(lts);
            long res;
            if (clusteringOffset == 0)
            {
                res = rng.prev(opId, pd);
            }
            else
            {
                int positionInPartition = (int) ((clusteringOffset + opId) % partitionSize);
                res = cd(positionInPartition, fractions, schema, rng, pd);
            }
            return schema.adjustCdEntropy(res);
        }

        @VisibleForTesting
        public static long cd(int positionInPartition, int[] fractions, SchemaSpec schema, Rng rng, long pd)
        {
            long[] slices = new long[schema.clusteringKeys.size()];
            for (int i = 0; i < slices.length; i++)
            {
                int idx = i < fractions.length ? (positionInPartition % (fractions[i] - 1)) : positionInPartition;
                slices[i] = rng.prev(idx, rng.next(pd, i + 1));
            }

            return schema.ckGenerator.stitch(slices);
        }

        protected long cd(long pd, long lts, long opId)
        {
            throw new RuntimeException("Shouldn't be called");
        }
    }

    // TODO: add a way to limit partition size alltogether; current "number of rows" notion is a bit misleading
    public static class DefaultDescriptorSelector extends DescriptorSelector
    {
        protected final static long NUMBER_OF_MODIFICATIONS_STREAM = 0xf490c5272baL;
        protected final static long ROWS_PER_OPERATION_STREAM = 0x5e03812e293L;
        protected final static long BITSET_IDX_STREAM = 0x92eb607bef1L;

        public static Surjections.Surjection<OperationKind> DEFAULT_OP_TYPE_SELECTOR = Surjections.enumValues(OperationKind.class);

        protected final OpSelectors.Rng rng;
        protected final Surjections.Surjection<OperationKind> operationTypeSelector;
        protected final Function<OperationKind, Surjections.Surjection<BitSet>> columnMaskSelector;
        protected final Distribution modificationsPerLtsDistribution;
        protected final Distribution rowsPerModificationsDistribution;
        protected final int maxPartitionSize;

        public DefaultDescriptorSelector(OpSelectors.Rng rng,
                                         Function<OperationKind, Surjections.Surjection<BitSet>> columnMaskSelector,
                                         Surjections.Surjection<OperationKind> operationTypeSelector,
                                         Distribution modificationsPerLtsDistribution,
                                         Distribution rowsPerModificationsDistribution,
                                         int maxPartitionSize)
        {
            this.rng = rng;

            this.operationTypeSelector = operationTypeSelector;
            this.columnMaskSelector = columnMaskSelector;

            this.modificationsPerLtsDistribution = modificationsPerLtsDistribution;
            this.rowsPerModificationsDistribution = rowsPerModificationsDistribution;
            this.maxPartitionSize = maxPartitionSize;
        }

        public int numberOfModifications(long lts)
        {
            return (int) modificationsPerLtsDistribution.skew(rng.randomNumber(lts, NUMBER_OF_MODIFICATIONS_STREAM));
        }

        public int opsPerModification(long lts)
        {
            return (int) rowsPerModificationsDistribution.skew(rng.randomNumber(lts, ROWS_PER_OPERATION_STREAM));
        }

        // TODO: this is not the best way to calculate a clustering offset; potentially we'd like to use
        // some sort of expiration mechanism slimilar to PDs.
        public int maxPartitionSize()
        {
            return maxPartitionSize;
        }

        protected int clusteringOffset(long lts)
        {
            return RngUtils.asInt(lts, 0, maxPartitionSize() - 1);
        }

        // TODO: this won't work for entropy-adjusted CDs, at least the way they're implemented now
        public boolean isCdVisitedBy(long pd, long lts, long cd)
        {
            return rowId(pd, lts, cd) < (numberOfModifications(lts) * opsPerModification(lts));
        }

        public long randomCd(long pd, long entropy)
        {
            long positionInPartition = Math.abs(rng.prev(entropy)) % maxPartitionSize();
            return rng.prev(positionInPartition, pd);
        }

        protected long cd(long pd, long lts, long opId)
        {
            assert opId <= maxPartitionSize;
            int partitionSize = maxPartitionSize();
            int clusteringOffset = clusteringOffset(lts);
            if (clusteringOffset == 0)
                return rng.prev(opId, pd);

            // TODO: partition size can't be larger than cardinality of the value.
            // So if we have 10 modifications per lts and 10 rows per modification,
            // we'll visit the same row twice per lts.
            int positionInPartition = (int) ((clusteringOffset + opId) % partitionSize);

            return rng.prev(positionInPartition, pd);
        }

        public long rowId(long pd, long lts, long cd)
        {
            int partitionSize = maxPartitionSize();
            int clusteringOffset = clusteringOffset(lts);
            int positionInPartition = (int) rng.next(cd, pd);

            if (clusteringOffset == 0)
                return positionInPartition;

            if (positionInPartition == 0)
                return partitionSize - clusteringOffset;
            if (positionInPartition == clusteringOffset)
                return 0;
            else if (positionInPartition < clusteringOffset)
                return partitionSize - clusteringOffset + positionInPartition;
            else
                return positionInPartition - clusteringOffset;
        }

        public OperationKind operationType(long pd, long lts, long opId)
        {
            return operationTypeSelector.inflate(pd ^ lts ^ opId);
        }

        public BitSet columnMask(long pd, long lts, long opId)
        {
            Surjections.Surjection<BitSet> gen = columnMaskSelector.apply(operationType(pd, lts, opId));
            if (gen == null)
                throw new IllegalArgumentException("Can't find a selector for " + gen);
            return gen.inflate(rng.randomNumber(pd ^ lts ^ opId, BITSET_IDX_STREAM));
        }

        public long vd(long pd, long cd, long lts, long opId, int col)
        {
            // change randomNumber / sequenceNumber to prev/Next
            return rng.randomNumber(opId + 1, pd ^ cd ^ lts ^ col);
        }

        public long modificationId(long pd, long cd, long lts, long vd, int col)
        {
            return rng.sequenceNumber(vd, pd ^ cd ^ lts ^ col);
        }
    }

    public enum OperationKind
    {
        WRITE,
        DELETE_ROW,
        DELETE_COLUMN,
        DELETE_RANGE
    }
}
