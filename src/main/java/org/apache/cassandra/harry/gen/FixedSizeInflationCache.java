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

import java.util.HashMap;
import java.util.Map;

import org.agrona.collections.Long2ObjectHashMap;

import org.apache.cassandra.harry.gen.rng.SeedableEntropySource;
import org.apache.cassandra.harry.util.LongRingBuffer;

/**
 * Fixed-capacity FIFO bimap cache for inflated values.
 *
 * The only way to add an entry is via {@link #get(long)}: on a miss the descriptor is inflated
 * using the provided generator, the result is stored, and the oldest entry is evicted if the
 * cache is at capacity.
 *
 * Both directions of the bimap are exposed: descriptor → value via {@link #get(long)}, and
 * value → descriptor via {@link #getDescriptor(Object)}.  The reverse lookup returns {@code null}
 * for values that are not currently cached.
 *
 * Array-valued generators (e.g. tuple generators backed by {@code Object[]}) will not work
 * correctly with the reverse lookup because {@code Object[].equals} is identity-based.
 */
public class FixedSizeInflationCache<T>
{
    private final Generator<T> generator;
    private final LongRingBuffer eviction;
    private final Long2ObjectHashMap<T> descriptorToValue;
    private final Map<T, Long> valueToDescriptor;

    public FixedSizeInflationCache(int capacity, Generator<T> generator)
    {
        this.generator = generator;
        this.eviction = new LongRingBuffer(capacity);
        this.descriptorToValue = new Long2ObjectHashMap<>();
        this.valueToDescriptor = new HashMap<>();
    }

    /**
     * Returns the inflated value for {@code descriptor}, inflating and caching it on a miss.
     * If the cache is full the oldest entry is evicted before the new one is inserted.
     */
    public T get(long descriptor)
    {
        T cached = descriptorToValue.get(descriptor);
        if (cached != null)
            return cached;

        T value = SeedableEntropySource.computeWithSeed(descriptor, generator::generate);

        if (eviction.isFull())
        {
            long evicted = eviction.poll();
            T evictedValue = descriptorToValue.remove(evicted);
            valueToDescriptor.remove(evictedValue);
        }

        eviction.offer(descriptor);
        descriptorToValue.put(descriptor, value);
        valueToDescriptor.put(value, descriptor);

        return value;
    }

    /**
     * Returns the descriptor for {@code value} if it is currently cached, {@code null} otherwise.
     * Does not add anything to the cache.
     */
    public Long getDescriptor(T value)
    {
        return valueToDescriptor.get(value);
    }

    /** Removes all entries from the cache and resets the eviction queue. */
    public void clear()
    {
        descriptorToValue.clear();
        valueToDescriptor.clear();
        while (!eviction.isEmpty())
            eviction.poll();
    }
}
