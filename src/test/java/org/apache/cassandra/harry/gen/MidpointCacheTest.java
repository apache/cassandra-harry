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


import static org.junit.Assert.*;

public class MidpointCacheTest
{
    private static long[] descriptors(int count)
    {
        long[] d = new long[count];
        for (int i = 0; i < count; i++)
            d[i] = i;
        return d;
    }

    @Test
    public void testFullCoverageAllPositionsCached()
    {
        long[] d = descriptors(10);
        MidpointCache<String> cache = new MidpointCache<>(d, desc -> "v" + desc);
        for (int i = 0; i < d.length; i++)
            assertNotNull("position " + i + " should be cached", cache.get(i));
    }

    @Test
    public void testFullCoverageValueMatchesInflate()
    {
        long[] d = descriptors(10);
        MidpointCache<String> cache = new MidpointCache<>(d, desc -> "v" + desc);
        for (int i = 0; i < d.length; i++)
            assertEquals("v" + i, cache.get(i));
    }

    @Test
    public void testFullCoverageInflateCalledOncePerPosition()
    {
        AtomicInteger count = new AtomicInteger();
        long[] d = descriptors(10);
        new MidpointCache<String>(d, desc -> { count.incrementAndGet(); return "v" + desc; });
        assertEquals(10, count.get());
    }

    @Test
    public void testPartialCoverageExactly1024PositionsCached()
    {
        int population = 4096;
        long[] d = descriptors(population);
        MidpointCache<String> cache = new MidpointCache<>(d, desc -> "v" + desc);
        int cached = 0;
        for (int i = 0; i < population; i++)
            if (cache.get(i) != null) cached++;
        assertEquals(1024, cached);
    }

    @Test
    public void testPartialCoverageRootMidIsCached()
    {
        int population = 4096;
        long[] d = descriptors(population);
        MidpointCache<String> cache = new MidpointCache<>(d, desc -> "v" + desc);
        int rootMid = (population - 1) >>> 1;
        assertEquals("v" + rootMid, cache.get(rootMid));
    }

    @Test
    public void testPartialCoverageUncachedPositionReturnsNull()
    {
        int population = 4096;
        long[] d = descriptors(population);
        MidpointCache<String> cache = new MidpointCache<>(d, desc -> "v" + desc);
        boolean foundNull = false;
        for (int i = 0; i < population; i++)
        {
            if (cache.get(i) == null)
            {
                foundNull = true;
                break;
            }
        }
        assertTrue("some positions should not be cached for large population", foundNull);
    }

    @Test
    public void testPartialCoverageInflateCalledExactly1024Times()
    {
        AtomicInteger count = new AtomicInteger();
        long[] d = descriptors(4096);
        new MidpointCache<String>(d, desc -> { count.incrementAndGet(); return "v" + desc; });
        assertEquals(1024, count.get());
    }

    @Test
    public void testPartialCoverageCachedValueMatchesInflate()
    {
        int population = 4096;
        long[] d = descriptors(population);
        MidpointCache<String> cache = new MidpointCache<>(d, desc -> "v" + desc);
        for (int i = 0; i < population; i++)
        {
            String v = cache.get(i);
            if (v != null)
                assertEquals("v" + i, v);
        }
    }

    @Test
    public void testBfsLevelOnePositionsCached()
    {
        // For population=4096: root mid = 2047, left child = 1023, right child = 3071
        int population = 4096;
        long[] d = descriptors(population);
        MidpointCache<String> cache = new MidpointCache<>(d, desc -> "v" + desc);
        assertEquals("v2047", cache.get(2047));
        assertEquals("v1023", cache.get(1023));
        assertEquals("v3071", cache.get(3071));
    }
}
