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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class FixedSizeInflationCacheTest
{
    // Generator that appends rng.next() to "v" — deterministic per descriptor, distinct across descriptors
    private static org.apache.cassandra.harry.gen.Generator<String> countingGen(AtomicInteger counter)
    {
        return rng -> {
            counter.incrementAndGet();
            return "v" + rng.next();
        };
    }

    @Test
    public void testGetInflatesOnMiss()
    {
        AtomicInteger count = new AtomicInteger();
        FixedSizeInflationCache<String> cache = new FixedSizeInflationCache<>(4, countingGen(count));

        cache.get(42L);
        assertEquals(1, count.get());
    }

    @Test
    public void testGetReturnsCachedValueOnHit()
    {
        AtomicInteger count = new AtomicInteger();
        FixedSizeInflationCache<String> cache = new FixedSizeInflationCache<>(4, countingGen(count));

        String first = cache.get(42L);
        String second = cache.get(42L);
        assertSame(first, second);
        assertEquals("inflated only once", 1, count.get());
    }

    @Test
    public void testGetDescriptorForCachedValue()
    {
        FixedSizeInflationCache<String> cache = new FixedSizeInflationCache<>(4, countingGen(new AtomicInteger()));

        String value = cache.get(42L);
        assertEquals(Long.valueOf(42L), cache.getDescriptor(value));
    }

    @Test
    public void testGetDescriptorReturnsNullForUncachedValue()
    {
        FixedSizeInflationCache<String> cache = new FixedSizeInflationCache<>(4, countingGen(new AtomicInteger()));

        assertNull(cache.getDescriptor("not-in-cache"));
    }

    @Test
    public void testFifoEvictionRemovesOldestEntry()
    {
        AtomicInteger count = new AtomicInteger();
        FixedSizeInflationCache<String> cache = new FixedSizeInflationCache<>(3, countingGen(count));

        String v1 = cache.get(1L);
        cache.get(2L);
        cache.get(3L);
        // cache full: [1, 2, 3]

        count.set(0);
        cache.get(4L); // evicts 1
        assertEquals("only 4L inflated", 1, count.get());
        assertNull("evicted value has no reverse mapping", cache.getDescriptor(v1));

        // re-accessing 1L must re-inflate
        cache.get(1L);
        assertEquals(2, count.get());
    }

    @Test
    public void testGetDescriptorReturnsNullAfterEviction()
    {
        FixedSizeInflationCache<String> cache = new FixedSizeInflationCache<>(2, countingGen(new AtomicInteger()));

        String v1 = cache.get(1L);
        cache.get(2L);
        cache.get(3L); // evicts 1

        assertNull(cache.getDescriptor(v1));
    }

    @Test
    public void testClearEmptiesCache()
    {
        AtomicInteger count = new AtomicInteger();
        FixedSizeInflationCache<String> cache = new FixedSizeInflationCache<>(4, countingGen(count));

        cache.get(1L);
        cache.get(2L);
        count.set(0);

        cache.clear();

        cache.get(1L);
        cache.get(2L);
        assertEquals("both re-inflated after clear", 2, count.get());
    }

    @Test
    public void testClearAllowsReuseUpToCapacity()
    {
        AtomicInteger count = new AtomicInteger();
        FixedSizeInflationCache<String> cache = new FixedSizeInflationCache<>(2, countingGen(count));

        cache.get(1L);
        cache.get(2L);
        cache.clear();
        count.set(0);

        // after clear the ring is empty, so we can fill to capacity again without eviction
        cache.get(3L);
        cache.get(4L);
        String v3 = cache.get(3L); // should be a hit
        assertSame(v3, cache.get(3L));
        assertEquals("3 and 4 inflated once each", 2, count.get());
    }
}
