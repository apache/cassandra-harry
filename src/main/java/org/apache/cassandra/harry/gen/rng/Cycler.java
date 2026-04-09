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

/**
 * A Feistel-cipher based cycler that enumerates numbers in [min, max]
 * in a pseudo-random order without repetition, cycling after exactly
 * (max - min + 1) calls to next().
 * <p>
 * Unlike {@link PcgCycle}, which requires power-of-two ranges, this
 * class supports arbitrary ranges by using a Feistel network with
 * cycle-walking to reject values outside the target range.
 */
public class Cycler
{
    private final long seed;
    private final long min;
    private final long max;
    private final long cycle;
    private final int halfBits;
    private final int rounds;
    private long counter;

    public Cycler(long seed, long min, long max)
    {
        if (max < min) throw new IllegalArgumentException("max must be >= min");
        this.seed = seed;
        this.min = min;
        this.max = max;
        this.cycle = max - min + 1;
        this.rounds = 8;

        if (cycle <= 1)
        {
            this.halfBits = 0;
        }
        else
        {
            int rawBits = 64 - Long.numberOfLeadingZeros(cycle - 1);
            this.halfBits = (rawBits + 1) / 2;
        }
        this.counter = 0;
    }

    public long getCycle()
    {
        return cycle;
    }

    /**
     * Return the permuted value for the given position without advancing
     * the internal counter.  This gives random-access into the permutation.
     */
    public long get(long position)
    {
        return permute(position);
    }

    public long next()
    {
        long result = permute(counter);
        counter = (counter + 1) % cycle;
        return result;
    }

    private long permute(long index)
    {
        if (cycle <= 1) return min;
        long result = encrypt(index);
        while (result >= cycle)
            result = encrypt(result);
        return result + min;
    }

    private long encrypt(long value)
    {
        long mask = (1L << halfBits) - 1;
        long left = (value >>> halfBits) & mask;
        long right = value & mask;

        for (int i = 0; i < rounds; i++)
        {
            long temp = right;
            right = left ^ (roundFunction(right, i) & mask);
            left = temp;
        }
        return (left << halfBits) | right;
    }

    private long roundFunction(long value, int round)
    {
        long h = value;
        h *= 0x9E3779B97F4A7C15L;
        h += seed;
        h += round * 0x6C62272E07BB0142L;
        h ^= h >>> 32;
        h *= 0xD6E8FEB86659FD93L;
        h ^= h >>> 32;
        h *= 0xD6E8FEB86659FD93L;
        h ^= h >>> 32;
        return h;
    }
}
