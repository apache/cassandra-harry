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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InMemorySorterTest
{
    // Natural order on longs as a LongComparator
    private static final InMemorySorter.LongComparator NAT = Long::compare;

    @Test
    public void testSortsByComparatorOrder()
    {
        long[] arr = { 7L, 3L, 99L, 1L, 42L };
        new InMemorySorter().sort(arr, arr.length, NAT);

        for (int i = 1; i < arr.length; i++)
            assertTrue("ascending at i=" + i, arr[i - 1] < arr[i]);
    }

    @Test
    public void testSortSubrangeOnly()
    {
        long[] arr = { 7L, 3L, 99L, 1L, 42L };
        long tail = arr[4];
        new InMemorySorter().sort(arr, 4, NAT);

        assertEquals("element beyond length is untouched", tail, arr[4]);
        for (int i = 1; i < 4; i++)
            assertTrue("ascending at i=" + i, arr[i - 1] < arr[i]);
    }

    @Test
    public void testSingleElement()
    {
        long[] arr = { 42L };
        new InMemorySorter().sort(arr, 1, NAT);
        assertEquals(42L, arr[0]);
    }

    @Test
    public void testReverseComparator()
    {
        long[] arr = { 1L, 5L, 3L, 9L, 2L };
        new InMemorySorter().sort(arr, arr.length, (a, b) -> Long.compare(b, a));

        for (int i = 1; i < arr.length; i++)
            assertTrue("descending at i=" + i, arr[i - 1] > arr[i]);
    }

    @Test
    public void testAlreadySorted()
    {
        long[] arr = { 1L, 2L, 3L, 4L, 5L };
        long[] expected = arr.clone();
        new InMemorySorter().sort(arr, arr.length, NAT);
        for (int i = 0; i < arr.length; i++)
            assertEquals("position " + i, expected[i], arr[i]);
    }
}
