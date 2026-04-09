/*
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package org.apache.cassandra.harry.gen;

import org.apache.cassandra.harry.gen.Bijections.IndexedBijection;

/**
 * A bijection that generates a fixed population of unique, ordered long values
 * without materializing them. Given a population of M values in a domain of
 * 2^(byteSize*8), divides the domain into M equal-width buckets and places
 * exactly one deterministic value in each bucket using a PRF (SplitMix64
 * finalizer). Different seeds produce different values; the same (seed,
 * descriptor) always produces the same value.
 * <p>
 * The descriptor is treated as a bucket index. Because buckets are non-overlapping
 * and laid out in ascending order, inflate(d1) < inflate(d2) whenever d1 < d2,
 * satisfying the Bijection ordering contract. The mapping is trivially invertible:
 * given a value, integer-divide by stride to recover the descriptor.
 * <p>
 * Values are signed longs in the range appropriate for the given byteSize:
 * <ul>
 *   <li>byteSize=8: [Long.MIN_VALUE, Long.MAX_VALUE]</li>
 *   <li>byteSize=4: [Integer.MIN_VALUE, Integer.MAX_VALUE]</li>
 *   <li>byteSize=2: [Short.MIN_VALUE, Short.MAX_VALUE]</li>
 *   <li>byteSize=1: [Byte.MIN_VALUE, Byte.MAX_VALUE]</li>
 * </ul>
 * To obtain a narrower numeric type, simply cast the inflated Long.
 *
 * <h3>Composing into an IndexedBijection via {@link #toIndexed}</h3>
 *
 * Any order-preserving Bijection can be lifted into an IndexedBijection by
 * composing it with a StridedBijection. This works because both the stride
 * mapping and the inner bijection are order-preserving bijections, and the
 * composition of two order-preserving bijections is itself an order-preserving
 * bijection:
 * <pre>
 *   idx --[strided.inflate]--> descriptor --[inner.inflate]--> value
 * </pre>
 * StridedBijection maps indices to descriptors such that
 * {@code i < j => strided.inflate(i) < strided.inflate(j)}.
 * The inner Bijection maps descriptors to values such that
 * {@code d1 < d2 => inner.inflate(d1) < inner.inflate(d2)}.
 * Composing them: {@code i < j => value(i) < value(j)}.
 * Both steps are invertible, so the full chain is invertible.
 * <p>
 * This means any type that Harry already has a Bijection for (int, long, UUID,
 * timestamp, etc.) can be turned into an IndexedBijection with O(1) seek and
 * O(1) inversion, zero materialization, and seed-dependent output -- simply by
 * inserting a StridedBijection as the descriptor-selection layer.
 */
public class StridedBijection implements Bijections.Bijection<Long>
{
    private final long seed;
    private final long pop;
    private final int bytes;
    private final long stride;
    private final long origin;
    private final boolean fullRange;
    private final boolean unsigned;

    public StridedBijection(long seed, long population, int byteSize)
    {
        this(seed, population, byteSize, false);
    }

    public StridedBijection(long seed, long population, int byteSize, boolean unsigned)
    {
        if (population <= 0)
            throw new IllegalArgumentException("Population must be positive, got: " + population);
        if (byteSize < 1 || byteSize > 8)
            throw new IllegalArgumentException("byteSize must be in [1, 8], got: " + byteSize);

        this.seed = seed;
        this.pop = population;
        this.bytes = byteSize;
        this.unsigned = unsigned;
        this.fullRange = (byteSize == Long.BYTES && !unsigned);

        if (fullRange)
        {
            // 2^64 does not fit in a long; approximate with unsigned division of (2^64 - 1)
            this.stride = Long.divideUnsigned(-1L, population);
            this.origin = Long.MIN_VALUE;
        }
        else
        {
            long rangeSize = 1L << (byteSize * 8);
            this.stride = rangeSize / population;
            this.origin = unsigned ? 0 : -(rangeSize / 2);
        }

        if (stride == 0)
            throw new IllegalArgumentException(
                "Population " + population + " too large for " + byteSize + " byte(s)");
    }

    /**
     * SplitMix64 finalizer used as a PRF: deterministic, fast, good avalanche.
     */
    private static long prf(long seed, long descriptor)
    {
        long z = seed + descriptor * 0x9e3779b97f4a7c15L;
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }

    @Override
    public Long inflate(long descriptor)
    {
        long offset;
        if (fullRange)
            offset = Long.remainderUnsigned(prf(seed, descriptor) >>> 1, stride);
        else
            offset = (prf(seed, descriptor) & Long.MAX_VALUE) % stride;

        return origin + descriptor * stride + offset;
    }

    @Override
    public long deflate(Long value)
    {
        if (fullRange)
            return Long.divideUnsigned(value - origin, stride);
        else
            return (value - origin) / stride;
    }

    @Override
    public int compare(long l, long r)
    {
        return Long.compare(l, r);
    }

    @Override
    public int byteSize()
    {
        return bytes;
    }

    @Override
    public long population()
    {
        return pop;
    }

    @Override
    public long adjustEntropyDomain(long descriptor)
    {
        return descriptor;
    }

    /**
     * Lifts any order-preserving {@link Bijections.Bijection} into an
     * {@link IndexedBijection} by composing it with a StridedBijection.
     * <p>
     * This is possible because both mappings in the chain are order-preserving
     * bijections, and the composition of order-preserving bijections is itself
     * an order-preserving bijection. Concretely:
     * <ol>
     *   <li>StridedBijection is an order-preserving bijection from indices to
     *       descriptors: {@code i < j => descriptor(i) < descriptor(j)}.</li>
     *   <li>The inner Bijection is an order-preserving bijection from descriptors
     *       to values: {@code d1 < d2 => value(d1) < value(d2)}.</li>
     *   <li>Composing them preserves both properties: {@code i < j => value(i) < value(j)},
     *       and both steps are invertible, so the full chain is invertible.</li>
     * </ol>
     * The result is an IndexedBijection that supports O(1) {@code descriptorAt}
     * and O(1) {@code idxFor} with zero materialization.
     *
     * @param inner      the order-preserving bijection to lift
     * @param seed       controls which values are selected within each bucket
     * @param population the number of indexed values
     * @return an IndexedBijection backed by the composition of a StridedBijection and {@code inner}
     */
    public static <T> IndexedBijection<T> toIndexed(Bijections.Bijection<T> inner, long seed, long population)
    {
        boolean isUnsigned = inner.unsigned();
        StridedBijection strided = new StridedBijection(seed, population, inner.byteSize(), isUnsigned);
        int byteSize = inner.byteSize();

        return new IndexedBijection<T>()
        {
            @Override
            public long descriptorAt(long idx)
            {
                return strided.inflate(idx);
            }

            @Override
            public long idxFor(long descriptor)
            {
                // For unsigned types, descriptors are already in [0, range) -- no
                // sign extension needed. For signed types, inner.deflate may return
                // an unsigned-encoded long that must be sign-extended back.
                return strided.deflate(isUnsigned ? descriptor : signExtend(descriptor, byteSize));
            }

            @Override
            public T inflate(long descriptor)
            {
                return inner.inflate(descriptor);
            }

            @Override
            public long deflate(T value)
            {
                return isUnsigned ? inner.deflate(value) : signExtend(inner.deflate(value), byteSize);
            }

            @Override
            public int compare(long l, long r)
            {
                return inner.compare(l, r);
            }

            @Override
            public int byteSize()
            {
                return byteSize;
            }

            @Override
            public long population()
            {
                return population;
            }

            @Override
            public long adjustEntropyDomain(long descriptor)
            {
                return inner.adjustEntropyDomain(descriptor);
            }
        };
    }

    /**
     * Sign-extends a descriptor from the given byte width to a full 64-bit long.
     * This bridges the gap between bijections that store descriptors in unsigned
     * form (e.g. Int32Bijection uses {@code value & 0xffffffffL}) and
     * StridedBijection which works in signed space.
     */
    static long signExtend(long descriptor, int byteSize)
    {
        if (byteSize >= 8)
            return descriptor;
        // Shift left to place the sign bit at bit 63, then arithmetic right-shift
        // back. This sign-extends from any bit width in one step.
        int shift = (8 - byteSize) * 8;
        return (descriptor << shift) >> shift;
    }
}
