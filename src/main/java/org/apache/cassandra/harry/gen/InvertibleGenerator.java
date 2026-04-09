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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.agrona.collections.IntHashSet;

import accord.utils.Invariants;

import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.gen.rng.SeedableEntropySource;
import org.apache.cassandra.utils.ArrayUtils;

/**
 * Invertible generator allows you to provide _any_ data type. Harry is based on the idea that descriptors
 * can be inflated into values, and values can be turned back into descriptors. Descriptors follow the sorting
 * order of the values they were generated from. This makes _writing_ these generators a bit more complex.
 * There is a library of lightweight generators available for simple cases.
 *
 * InvertibleGenerator decouples descriptor order from value order, and allows descriptor to be used simply as
 * a seed for generating values. Since it tracks all descriptors it generated values from in a sorted order,
 * it can always turn the given value back into a descriptor by inflating log(population) values and comparing them
 * to the searched value. In other words, it trades memory required for storing map of values to CPU required
 * to re-compute the value order.
 *
 * TODO (expected): custom invertible generator for bool, u8, u16, u32, etc, for efficiency.
 * TODO (expected): implement support for tuple/vector/udt, and other multi-cell types.
 */
public class InvertibleGenerator<T> implements Bijections.IndexedBijection<T>
{
    public static long MAX_ENTROPY = 1L << 63;

    private static final boolean PARANOIA = false;

    private final long[] allocatedDescriptors;

    private final Generator<T> gen;
    private final Comparator<T> comparator;
    private final MidpointCache<T> cache;

    // To avoid <?> erased types
    public static <T> InvertibleGenerator<T> fromType(EntropySource rng, int population, ColumnSpec<T> spec)
    {
        int effectivePopulation = spec.population > 0 ? spec.population : population;
        return new InvertibleGenerator<>(rng, spec.type.typeEntropy(), effectivePopulation, spec.gen, spec.type.comparator());
    }

    public InvertibleGenerator(EntropySource rng,
                               /* unsigned */ long typeEntropy,
                               int population,
                               Generator<T> gen,
                               Comparator<T> comparator)
    {
        Invariants.require(population > 0,
                              "Population should be strictly positive %d", population);
        Invariants.require(Long.compareUnsigned(typeEntropy, 0) > 0,
                              "Type entropy should be strictly positive, but was %d: %s", typeEntropy, gen);

        // We can / will generate at most that many values
        if (Long.compareUnsigned(typeEntropy, Integer.MAX_VALUE) > 0)
            typeEntropy = Integer.MAX_VALUE;

        population = (int) Math.min(typeEntropy, population);

        this.gen = gen;
        this.comparator = comparator;

        // Generate a population of _unique_ values. We do not want to store all values, only their hashes.
        long[] tmp = new long[population];
        IntHashSet hashes = new IntHashSet(population);
        int length = 0;
        while (length < population)
        {
            long candidate = rng.next();

            // Should never allocate these, however improbable that is
            if (MagicConstants.MAGIC_DESCRIPTOR_VALS.contains(candidate))
                continue;

            Object inflated = inflate(candidate);
            int hash = ArrayUtils.hashCode(inflated);
            Invariants.require(hash != System.identityHashCode(inflated), "hashCode was not overridden for type %s", inflated.getClass());

            if (hashes.add(hash))
                tmp[length++] = candidate;
        }
        hashes.clear();

        new InMemorySorter().sort(tmp, length, this::compare);

        this.allocatedDescriptors = Arrays.copyOf(tmp, length);
        this.cache = new MidpointCache<>(this.allocatedDescriptors, this::inflate);

        // Check there are no duplicates, and items are properly sorted.
        if (PARANOIA)
        {
            T prev = inflate(allocatedDescriptors[0]);
            for (int i = 1; i < allocatedDescriptors.length; i++)
            {
                T current = inflate(allocatedDescriptors[i]);
                Invariants.require(comparator.compare(current, prev) > 0, "%s should be strictly after %s", prev, current);
            }
        }
    }

    @Override
    public long idxFor(long descriptor)
    {
        return binarySearch(inflate(descriptor));
    }

    @Override
    public long descriptorAt(long idx)
    {
        return allocatedDescriptors[(int) idx];
    }

    @Override
    public T inflate(long descriptor)
    {
        Invariants.require(!MagicConstants.MAGIC_DESCRIPTOR_VALS.contains(descriptor), "Should not be able to inflate %d, as it's magic value", descriptor);
        return SeedableEntropySource.computeWithSeed(descriptor, gen::generate);
    }

    @Override
    public long deflate(T value)
    {
        final int idx = binarySearch(value);
        if (PARANOIA)
        {
            if (idx < 0)
            {
                for (long descriptor : allocatedDescriptors)
                {
                    Object expected = inflate(descriptor);
                    if (value.getClass().isArray())
                    {
                        Object[] valueArr = (Object[]) value;
                        Object[] expectedArr = (Object[]) expected;
                        Invariants.require(comparator.compare((T) expected, value) != 0,
                                           "%s was found: %s", expectedArr, valueArr);

                    }
                    else
                    {
                        Invariants.require(comparator.compare((T) expected, value) != 0,
                                           "%s was found: %s", expected, value);
                    }

                }
            }
            else
            {
                long res = allocatedDescriptors[idx];
                Object expected = inflate(res);
                if (value.getClass().isArray())
                {
                    Object[] valueArr = (Object[]) value;
                    Object[] expectedArr = (Object[]) expected;

                    Invariants.require(comparator.compare((T) expected, value) == 0,
                                       "%s != %s", expectedArr, valueArr);

                }
                else
                {
                    Invariants.require(comparator.compare((T) expected, value) == 0,
                                       "%s != %s", expected, value);
                }

                return res;
            }
        }

        if (idx < 0)
        {
            int start = Math.max(0, idx - 2);
            List<Object> nearby = new ArrayList<>();
            for (int i = start; i < start + 2; i++)
                nearby.add(inflate(allocatedDescriptors[i]));
            throw new IllegalStateException(String.format("Could not find: %s\nNearby objects: %s",
                                                          ArrayUtils.toString(value), nearby.stream().map(ArrayUtils::toString).collect(Collectors.toList())));
        }

        return allocatedDescriptors[idx];
    }


    @Override
    public int byteSize()
    {
        return Long.BYTES;
    }

    private int binarySearch(T key)
    {
        int low = 0, mid = allocatedDescriptors.length, high = mid - 1, result = -1;
        while (low <= high)
        {
            mid = (low + high) >>> 1;
            T cached = cache.get(mid);
            T inflated = cached != null ? cached : inflate(allocatedDescriptors[mid]);
            result = comparator.compare(key, inflated);
            if (result > 0)
                low = mid + 1;
            else if (result == 0)
                return mid;
            else
                high = mid - 1;
        }
        return -mid - (result < 0 ? 1 : 2);
    }


    @Override
    public int compare(long d1, long d2)
    {
        if (d1 == d2)
            return 0;
        T v1 = inflate(d1);
        T v2 = inflate(d2);
        return comparator.compare(v1, v2);
    }

    /**
     * Returns a number of allocated descriptors
     */
    @Override
    public long population()
    {
        return allocatedDescriptors.length;
    }

    public Comparator<Long> descriptorsComparator()
    {
        // TODO: this can be cached
        Map<Long, Integer> descriptorToIdx = new HashMap<>();
        for (int i = 0; i < allocatedDescriptors.length; i++)
            descriptorToIdx.put(allocatedDescriptors[i], i);
        return Comparator.comparingInt(descriptorToIdx::get);
    }
}
