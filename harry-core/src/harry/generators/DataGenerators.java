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

package harry.generators;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import harry.ddl.ColumnSpec;

public class DataGenerators
{
    public static final Object UNSET_VALUE = new Object();
    public static long UNSET_DESCR = 0;
    public static long NIL_DESCR = -1;

    public static Object[] inflateData(List<ColumnSpec<?>> columns, long[] descriptors)
    {
        // This can be not true depending on how we implement subselections
        assert columns.size() == descriptors.length;
        Object[] data = new Object[descriptors.length];
        for (int i = 0; i < descriptors.length; i++)
        {
            ColumnSpec columnSpec = columns.get(i);
            if (descriptors[i] == UNSET_DESCR)
                data[i] = UNSET_VALUE;
            else
                data[i] = columnSpec.inflate(descriptors[i]);
        }
        return data;
    }

    public static long[] deflateData(List<ColumnSpec<?>> columns, Object[] data)
    {
        // This can be not true depending on how we implement subselections
        assert columns.size() == data.length;
        long[] descriptors = new long[data.length];
        for (int i = 0; i < descriptors.length; i++)
        {
            ColumnSpec columnSpec = columns.get(i);
            if (data[i] == null)
                descriptors[i] = NIL_DESCR;
            else
                descriptors[i] = columnSpec.deflate(data[i]);
        }
        return descriptors;
    }

    public static int[] requiredBytes(List<ColumnSpec<?>> columns)
    {
        switch (columns.size())
        {
            case 0:
                throw new RuntimeException("Can't inflate empty data column set as it is not inversible");
            case 1:
                return new int[]{ Math.min(columns.get(0).type.maxSize(), Long.SIZE) };
            default:
                class Pair
                {
                    final int idx, maxSize;

                    Pair(int idx, int maxSize)
                    {
                        this.idx = idx;
                        this.maxSize = maxSize;
                    }
                }
                int[] bytes = new int[Math.min(4, columns.size())];
                Pair[] sorted = new Pair[bytes.length];
                for (int i = 0; i < sorted.length; i++)
                    sorted[i] = new Pair(i, columns.get(i).type.maxSize());

                int remainingBytes = Long.BYTES;
                int slotSize = remainingBytes / bytes.length;
                // first pass: give it at most a slot number of bytes
                for (int i = 0; i < sorted.length; i++)
                {
                    int size = sorted[i].maxSize;
                    int allotedSize = Math.min(size, slotSize);
                    remainingBytes -= allotedSize;
                    bytes[sorted[i].idx] = allotedSize;
                }

                // sliced evenly
                if (remainingBytes == 0)
                    return bytes;

                // second pass: try to occupy remaining bytes
                // it is possible to improve the second pass and separate additional bytes evenly, but it is
                // questionable how much it'll bring since it does not change the total amount of entropy.
                for (int i = 0; i < sorted.length; i++)
                {
                    if (remainingBytes == 0)
                        break;
                    Pair p = sorted[i];
                    if (bytes[p.idx] < p.maxSize)
                    {
                        int allotedSize = Math.min(p.maxSize - bytes[p.idx], remainingBytes);
                        remainingBytes -= allotedSize;
                        bytes[p.idx] += allotedSize;
                    }
                }

                return bytes;
        }
    }

    public static Object[] inflateKey(List<ColumnSpec<?>> columns, long descriptor, long[] slices)
    {
        assert columns.size() >= slices.length : String.format("Columns: %s. Slices: %s", columns, Arrays.toString(slices));
        assert columns.size() > 0 : "Can't deflate from empty columnset";

        Object[] res = new Object[columns.size()];
        for (int i = 0; i < slices.length; i++)
        {
            ColumnSpec spec = columns.get(i);
            res[i] = spec.inflate(slices[i]);
        }

        // The rest can be random, since prefix is always fixed
        long current = descriptor;
        for (int i = slices.length; i < columns.size(); i++)
        {
            current = RngUtils.next(current);
            res[i] = columns.get(i).inflate(current);
        }

        return res;
    }

    public static long[] deflateKey(List<ColumnSpec<?>> columns, Object[] values)
    {
        assert columns.size() == values.length : String.format("%s != %s", columns.size(), values.length);
        assert columns.size() > 0 : "Can't deflate from empty columnset";

        int fixedPart = Math.min(4, columns.size());

        long[] slices = new long[fixedPart];
        for (int i = 0; i < fixedPart; i++)
        {
            ColumnSpec spec = columns.get(i);
            slices[i] = spec.deflate(values[i]);
        }
        return slices;
    }

    public static KeyGenerator createKeyGenerator(List<ColumnSpec<?>> columns)
    {
        switch (columns.size())
        {
            case 0:
                return EMPTY_KEY_GEN;
            case 1:
                return new SinglePartKeyGenerator(columns);
            default:
                return new MultiPartKeyGenerator(columns);
        }
    }

    private static final KeyGenerator EMPTY_KEY_GEN = new KeyGenerator(Collections.emptyList())
    {
        private final long[] EMPTY_SLICED = new long[0];
        private final Object[] EMPTY_INFLATED = new Object[0];

        public long[] slice(long descriptor)
        {
            return EMPTY_SLICED;
        }

        public long stitch(long[] parts)
        {
            return 0;
        }

        protected long minValueInternal(int idx)
        {
            return 0;
        }

        protected long maxValueInternal(int idx)
        {
            return 0;
        }

        @Override
        public Object[] inflate(long descriptor)
        {
            return EMPTY_INFLATED;
        }

        @Override
        public long deflate(Object[] value)
        {
            return 0;
        }

        public long adjustEntropyDomain(long descriptor)
        {
            return 0;
        }

        public int byteSize()
        {
            return 0;
        }

        public int compare(long l, long r)
        {
            return 0;
        }
    };

    public static abstract class KeyGenerator implements Bijections.Bijection<Object[]>
    {
        @VisibleForTesting
        final List<ColumnSpec<?>> columns;

        KeyGenerator(List<ColumnSpec<?>> columns)
        {
            this.columns = columns;
        }

        public abstract long[] slice(long descriptor);

        public abstract long stitch(long[] parts);

        public long minValue()
        {
            return columns.get(0).isReversed() ? maxForSize(byteSize()) : minForSize(byteSize());
        }

        public long maxValue()
        {
            return columns.get(0).isReversed() ? minForSize(byteSize()) : maxForSize(byteSize());
        }

        protected static long minForSize(int size)
        {
            long min = 1L << (size * Byte.SIZE - 1);

            if (size < Long.BYTES)
                min ^= Bytes.signMaskFor(size);

            return min;
        }

        protected long maxForSize(int size)
        {
            long max = Bytes.bytePatternFor(size) >>> 1;

            if (size < Long.BYTES)
                max ^= Bytes.signMaskFor(size);

            return max;
        }

        /**
         * Min value for a segment: 0, possibly with an inverted 0 sign for stitching.
         * Similar thing can be achieved by
         */
        public long minValue(int idx)
        {
            return columns.get(idx).isReversed() ? maxValueInternal(idx) : minValueInternal(idx);
        }

        public long maxValue(int idx)
        {
            return columns.get(idx).isReversed() ? minValueInternal(idx) : maxValueInternal(idx);
        }

        protected abstract long minValueInternal(int idx);

        protected abstract long maxValueInternal(int idx);
    }

    static class SinglePartKeyGenerator extends KeyGenerator
    {
        private final Bijections.Bijection keyGen;
        private final int totalSize;

        SinglePartKeyGenerator(List<ColumnSpec<?>> columns)
        {
            super(columns);
            assert columns.size() == 1;
            this.keyGen = columns.get(0).generator();
            this.totalSize = keyGen.byteSize();
        }

        public long[] slice(long descriptor)
        {
            long adjusted = adjustEntropyDomain(descriptor);
            long[] res = new long[]{ adjusted };
            assert adjusted == stitch(res);
            return res;
        }

        public long stitch(long[] parts)
        {
            return parts[0];
        }

        public long minValueInternal(int idx)
        {
            return minForSize(totalSize);
        }

        public long maxValueInternal(int idx)
        {
            return maxForSize(totalSize);
        }

        public Object[] inflate(long descriptor)
        {
            return new Object[]{ keyGen.inflate(descriptor) };
        }

        public long deflate(Object[] value)
        {
            long descriptor = keyGen.deflate(value[0]);
            descriptor &= Bytes.bytePatternFor(totalSize);
            return descriptor;
        }

        public int byteSize()
        {
            return totalSize;
        }

        public int compare(long l, long r)
        {
            return Long.compare(l, r);
        }

        public long adjustEntropyDomain(long descriptor)
        {
            descriptor &= (Bytes.bytePatternFor(totalSize) >> 1);
            descriptor = keyGen.adjustEntropyDomain(descriptor);
            return descriptor;
        }
    }

    public static class MultiPartKeyGenerator extends KeyGenerator
    {
        @VisibleForTesting
        public final int[] sizes;
        protected final int totalSize;

        MultiPartKeyGenerator(List<ColumnSpec<?>> columns)
        {
            super(columns);
            assert columns.size() > 1 : "It makes sense to use a multipart generator if you have more than one column, but you have " + columns.size();

            this.sizes = requiredBytes(columns);
            int total = 0;
            for (int size : sizes)
                total += size;

            this.totalSize = total;
        }

        public long deflate(Object[] values)
        {
            return stitch(DataGenerators.deflateKey(columns, values));
        }

        public Object[] inflate(long descriptor)
        {
            return DataGenerators.inflateKey(columns, descriptor, slice(descriptor));
        }

        public long adjustEntropyDomain(long descriptor)
        {
            // We can't simply trim the value here, mostly because of the values like
            // long and double that can change the value during normalization in addition
            // to trimming it.
            return stitch(slice(descriptor));
        }

        // Checks whether we need to invert a slice sign to preserve order of the sliced descriptor
        public boolean shouldInvertSign(int idx)
        {
            int maxSliceSize = columns.get(idx).generator().byteSize();
            int actualSliceSize = sizes[idx];

            if (idx == 0)
            {
                // Signed representation of the first value would follow the sorting of the
                // long value itself, which means that we have invert sign of the first piece if:
                //    * not all entropy bytes are consumed
                //    * we do not have enough entropy bytes to set the sign bit of the value
                // TODO: I think maxPieceSize != pieceSize is not right here; it should be <= ???
                return totalSize != Long.BYTES || maxSliceSize != actualSliceSize;
            }

            // We invert sign of all subsequent chunks if their signs match
            return maxSliceSize == actualSliceSize;
        }

        public long[] slice(long descriptor)
        {
            long[] pieces = new long[sizes.length];
            long pos = totalSize;
            for (int i = 0; i < sizes.length; i++)
            {
                final int size = sizes[i];
                long piece = descriptor >> ((pos - size) * Byte.SIZE);

                piece &= Bytes.bytePatternFor(size);

                if (shouldInvertSign(i))
                    piece ^= Bytes.signMaskFor(size);

                piece = columns.get(i).adjustEntropyDomain(piece);

                pieces[i] = piece;
                pos -= size;
            }
            return pieces;
        }

        public long stitch(long[] parts)
        {
            long stitched = 0;
            int consumed = 0;
            for (int i = sizes.length - 1; i >= 0; i--)
            {
                int size = sizes[i];
                long piece = parts[i];

                if (shouldInvertSign(i))
                    piece ^= Bytes.signMaskFor(size);

                piece &= Bytes.bytePatternFor(size);
                stitched |= piece << (consumed * Byte.SIZE);
                consumed += size;
            }
            return stitched;
        }

        protected long minValueInternal(int idx)
        {
            int size = sizes[idx];
            long res = 0;
            if (shouldInvertSign(idx))
                res ^= Bytes.signMaskFor(size);
            return res;
        }

        protected long maxValueInternal(int idx)
        {
            int size = sizes[idx];
            long res = Bytes.bytePatternFor(size);
            if (shouldInvertSign(idx))
                res ^= Bytes.signMaskFor(size);
            return res;
        }

        public int byteSize()
        {
            return totalSize;
        }

        public int compare(long l, long r)
        {
            return Long.compare(l, r);
        }
    }
}