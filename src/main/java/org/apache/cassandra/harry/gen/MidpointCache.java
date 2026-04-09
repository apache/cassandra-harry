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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.function.LongFunction;

/**
 * Fixed-capacity cache of inflated values at BFS-order binary-search midpoint positions.
 *
 * Reduces redundant {@code inflate()} calls inside {@code InvertibleGenerator.binarySearch()}
 * by pre-computing inflated values at the positions a binary search visits as it descends the
 * implicit search tree. Constructed once; immutable after construction.
 */
class MidpointCache<T>
{
    private static final int CAPACITY = 1024;

    private final int[] cachedPositions; // null when fullyCovered
    private final Object[] cachedValues;
    private final boolean fullyCovered;

    public MidpointCache(long[] sortedDescriptors, LongFunction<T> inflate)
    {
        int population = sortedDescriptors.length;

        if (population <= CAPACITY)
        {
            fullyCovered = true;
            cachedPositions = null;
            cachedValues = new Object[population];
            for (int i = 0; i < population; i++)
                cachedValues[i] = inflate.apply(sortedDescriptors[i]);
        }
        else
        {
            fullyCovered = false;
            int[] positions = new int[CAPACITY];
            int count = 0;

            ArrayDeque<int[]> queue = new ArrayDeque<>(2 * CAPACITY);
            queue.add(new int[]{ 0, population - 1 });

            while (!queue.isEmpty() && count < CAPACITY)
            {
                int[] range = queue.poll();
                int lo = range[0], hi = range[1];
                if (lo > hi)
                    continue;
                int mid = (lo + hi) >>> 1;
                positions[count++] = mid;
                if (count < CAPACITY)
                {
                    queue.add(new int[]{ lo, mid - 1 });
                    queue.add(new int[]{ mid + 1, hi });
                }
            }

            Arrays.sort(positions);
            cachedPositions = positions;
            cachedValues = new Object[CAPACITY];
            for (int i = 0; i < CAPACITY; i++)
                cachedValues[i] = inflate.apply(sortedDescriptors[cachedPositions[i]]);
        }
    }

    /**
     * Returns the cached inflated value for array position {@code idx},
     * or {@code null} if not cached.
     * Behaviour is undefined for {@code idx} outside [0, population).
     */
    @SuppressWarnings("unchecked")
    public T get(int idx)
    {
        if (fullyCovered)
            return (T) cachedValues[idx];

        int pos = Arrays.binarySearch(cachedPositions, idx);
        return pos >= 0 ? (T) cachedValues[pos] : null;
    }
}
