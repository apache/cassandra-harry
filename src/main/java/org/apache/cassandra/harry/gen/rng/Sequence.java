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

import org.apache.cassandra.harry.gen.EntropySource;

/**
 * An addressable, navigable deterministic sequence of longs.
 * <p>
 * Wraps PCG with a bound (seed, stream) pair so callers never have to
 * pass stream ids or call raw {@link PCGFastPure} methods directly.
 * <p>
 * Uses the same convention as {@link PureRng.PCGFast}: the seed is the
 * raw PCG initial state, and the stream is passed directly to
 * {@link PCGFastPure#advanceState} (which applies
 * {@link PCGFastPure#streamIncrement} internally).
 */
public class Sequence
{
    private final long seed;
    private final long stream;

    public Sequence(long seed)
    {
        this(seed, 0);
    }

    public Sequence(long seed, long stream)
    {
        this.seed = seed;
        this.stream = stream;
    }

    /**
     * Get the i-th value in this sequence.
     */
    public long valueAt(long i)
    {
        return PCGFastPure.shuffle(PCGFastPure.advanceState(seed, i, stream));
    }

    /**
     * Reverse lookup: given a value produced by this sequence, return its index.
     */
    public long indexOf(long value)
    {
        return PCGFastPure.distance(seed, PCGFastPure.unshuffle(value), stream);
    }

    /**
     * Given a value from this sequence, return the next value in the sequence.
     */
    public long next(long value)
    {
        return PCGFastPure.next(value, stream);
    }

    /**
     * Given a value from this sequence, return the previous value in the sequence.
     */
    public long prev(long value)
    {
        return PCGFastPure.previous(value, stream);
    }

    /**
     * Compute the distance (number of steps) between two values in this sequence.
     */
    public long distance(long a, long b)
    {
        return PCGFastPure.distance(PCGFastPure.unshuffle(a), PCGFastPure.unshuffle(b), stream);
    }

    /**
     * Derive a sub-sequence rooted at position i.
     * The sub-sequence uses the internal state at position i as its seed,
     * and an incremented stream to ensure independence.
     */
    public Sequence fork(long i)
    {
        long forkedState = PCGFastPure.advanceState(seed, i, stream);
        return new Sequence(forkedState, stream + 1);
    }

    /**
     * Create a stateful {@link EntropySource} that walks this sequence.
     * The first call to {@link EntropySource#next()} returns {@code valueAt(1)},
     * matching PCG convention where next() advances then reads.
     */
    public EntropySource toEntropySource()
    {
        return new SequenceEntropySource(seed, stream);
    }

    public long seed()
    {
        return seed;
    }

    public long stream()
    {
        return stream;
    }

    /**
     * A stateful EntropySource backed by the same PCG state as this Sequence.
     * Unlike {@link PcgRSUFast}, this does not apply xorshift64star to the seed,
     * so the generated values match {@link Sequence#valueAt}.
     */
    private static class SequenceEntropySource implements EntropySource
    {
        private long state;
        private final long stream;

        SequenceEntropySource(long state, long stream)
        {
            this.state = state;
            this.stream = stream;
        }

        @Override
        public long next()
        {
            state = PCGFastPure.nextState(state, stream);
            return PCGFastPure.shuffle(state);
        }

        @Override
        public void seed(long seed)
        {
            this.state = seed;
        }

        @Override
        public EntropySource derive()
        {
            return new SequenceEntropySource(PCGFastPure.nextState(state, stream), stream);
        }

        @Override
        public int nextInt()
        {
            return RngUtils.asInt(next());
        }

        @Override
        public int nextInt(int max)
        {
            return RngUtils.asInt(next(), max);
        }

        @Override
        public int nextInt(int min, int max)
        {
            return RngUtils.asInt(next(), min, max);
        }

        @Override
        public float nextFloat()
        {
            return RngUtils.asFloat(next());
        }

        @Override
        public double nextDouble()
        {
            return RngUtils.asDouble(next());
        }

        @Override
        public boolean nextBoolean()
        {
            return RngUtils.asBoolean(next());
        }
    }
}
