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

package org.apache.cassandra.harry.gen.rng;

import org.apache.cassandra.harry.gen.Distribution;
import org.apache.cassandra.harry.gen.EntropySource;

/**
 * Maintains a sliding window of active primary-key positions over a
 * pseudo-random permutation produced by {@link Cycler}.
 * <p>
 * On each {@link #pick} call, one active key is selected (via a pluggable
 * {@link Distribution}) and returned.  With probability {@code evictionRate},
 * the lowest-position active key is evicted and replaced by either the next
 * unvisited key (sliding the window forward) or, with probability
 * {@code revisitRate}, an already-evicted key.  Revisited keys naturally
 * become the new lowest and are evicted first on the next replacement.
 *
 * <!-- TODO: consider whether revisited keys should sometimes survive longer,
 *      e.g. by not always evicting the absolute lowest but using a distribution
 *      over the bottom-K elements. Discuss with Alex. -->
 */
public class KeyPicker
{
    private final Cycler cycler;
    private final int windowSize;
    private final double evictionRate;
    private final double revisitRate;
    private final Distribution pickDistribution;

    private final long[] active;
    private int lowestSlot;
    private long hi;

    public KeyPicker(long seed, long population, int windowSize,
                     double evictionRate, double revisitRate,
                     Distribution pickDistribution)
    {
        if (windowSize < 1)
            throw new IllegalArgumentException("windowSize must be >= 1");
        if (windowSize > population)
            throw new IllegalArgumentException("windowSize (" + windowSize + ") must be <= population (" + population + ")");
        if (evictionRate < 0 || evictionRate > 1)
            throw new IllegalArgumentException("evictionRate must be in [0.0, 1.0]");
        if (revisitRate < 0 || revisitRate > 1)
            throw new IllegalArgumentException("revisitRate must be in [0.0, 1.0]");

        this.cycler = new Cycler(seed, 0, population - 1);
        this.windowSize = windowSize;
        this.evictionRate = evictionRate;
        this.revisitRate = revisitRate;
        this.pickDistribution = pickDistribution;

        this.active = new long[windowSize];
        for (int i = 0; i < windowSize; i++)
            active[i] = i;

        this.lowestSlot = 0;
        this.hi = windowSize;
    }

    /**
     * Pick an active key, possibly triggering a replacement.
     * Returns a key ID in {@code [0, population)}.
     */
    public long pick(EntropySource rng)
    {
        int slot = (int) pickDistribution.next(rng);
        long position = active[slot];
        long key = cycler.get(position);

        if (rng.nextDouble() < evictionRate)
            replace(rng);

        return key;
    }

    private void replace(EntropySource rng)
    {
        long evictedPos = active[lowestSlot];

        boolean doRevisit = evictedPos > 0
                            && rng.nextDouble() < revisitRate;

        if (doRevisit)
        {
            long revisitPos = rng.nextLong(0, evictedPos);
            active[lowestSlot] = revisitPos;
        }
        else if (hi < cycler.getCycle())
        {
            active[lowestSlot] = hi;
            hi++;
        }
        else
        {
            // Population exhausted and revisit not possible or not chosen.
            // Fall back to revisit if there are evicted positions.
            if (evictedPos > 0)
            {
                long revisitPos = rng.nextLong(0, evictedPos);
                active[lowestSlot] = revisitPos;
            }
            // else: window == population, nothing to do
        }

        updateLowestSlot();
    }

    private void updateLowestSlot()
    {
        int minSlot = 0;
        long minPos = active[0];
        for (int i = 1; i < windowSize; i++)
        {
            if (active[i] < minPos)
            {
                minPos = active[i];
                minSlot = i;
            }
        }
        this.lowestSlot = minSlot;
    }

    /** Return a copy of the current active positions. */
    public long[] activePositions()
    {
        long[] copy = new long[windowSize];
        System.arraycopy(active, 0, copy, 0, windowSize);
        return copy;
    }

    /** Return the current active key IDs (Cycler outputs). */
    public long[] activeKeys()
    {
        long[] keys = new long[windowSize];
        for (int i = 0; i < windowSize; i++)
            keys[i] = cycler.get(active[i]);
        return keys;
    }

    /** Number of distinct positions ever activated (hi watermark). */
    public long visited()
    {
        return hi;
    }

    /** The lowest position currently in the active set. */
    public long lowestActive()
    {
        return active[lowestSlot];
    }

    public int windowSize()
    {
        return windowSize;
    }
}
