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
 * A permutation generator based on PCG (Permuted Congruential Generator) that
 * produces a full-cycle permutation of all values in a given bit width.
 * <p>
 * Given a bit width N (must be a power of two, 1 to 64), the generator visits
 * every value in [0, 2^N) exactly once before cycling. This is achieved by
 * combining a full-period LCG (Linear Congruential Generator) with the
 * invertible RXS-M-XS output permutation from the PCG family. Because both
 * the LCG state transition and the output permutation are bijections, the
 * composed function is also a bijection over the state space, guaranteeing
 * that every output appears exactly once per period.
 * <p>
 * The period is exactly 2^bitWidth. Different seeds produce different starting
 * points in the same permutation; different streams select entirely different
 * permutations (by changing the LCG increment).
 * <p>
 * Typical uses:
 * <ul>
 *   <li>Shuffling a range of IDs without storing the full permutation</li>
 *   <li>Generating unique test keys in a deterministic, reproducible order</li>
 *   <li>Iterating over a value space in pseudorandom order for fuzz testing</li>
 * </ul>
 *
 * @see <a href="https://www.pcg-random.org/paper.html">PCG: A Family of
 *      Simple Fast Space-Efficient Statistically Good Algorithms for Random
 *      Number Generation</a>
 */
public class PcgCycle
{
    private final int bitWidth;
    private final long mask;
    private final long multiplier;
    private final long mcgMultiplier;
    private final long increment;
    private final int opbits;
    private long state;

    // LCG multipliers (from PCG C++ reference implementation)
    private static final long MULT_8  = 141L;
    private static final long MULT_16 = 12829L;
    private static final long MULT_32 = 747796405L;
    private static final long MULT_64 = 6364136223846793005L;

    // Default increments (from PCG C++ reference implementation)
    private static final long INC_8  = 77L;
    private static final long INC_16 = 47989L;
    private static final long INC_32 = 2891336453L;
    private static final long INC_64 = 1442695040888963407L;

    // MCG multipliers for the RXS-M-XS output permutation
    private static final long MCG_8  = 217L;
    private static final long MCG_16 = 62169L;
    private static final long MCG_32 = 277803737L;
    private static final long MCG_64 = -5840758589994634535L; // unsigned: 12605985483714917081

    private PcgCycle(int bitWidth, long seed, long stream)
    {
        if (bitWidth < 1 || bitWidth > 64 || (bitWidth & (bitWidth - 1)) != 0)
        {
            throw new IllegalArgumentException(
                "bitWidth must be a power of 2 in [1, 64], got: " + bitWidth);
        }
        this.bitWidth = bitWidth;
        this.mask = bitWidth == 64 ? -1L : (1L << bitWidth) - 1;
        this.multiplier = lookupMultiplier(bitWidth);
        this.mcgMultiplier = lookupMcgMultiplier(bitWidth);
        this.increment = ((stream << 1) | 1L) & mask;
        this.opbits = bitWidth >= 64 ? 5 : bitWidth >= 32 ? 4 : bitWidth >= 16 ? 3 : 2;

        // PCG seeding protocol: two bumps with the seed mixed in between
        this.state = 0L;
        this.state = bump(this.state);
        this.state = (this.state + seed) & mask;
        this.state = bump(this.state);
    }

    private long bump(long s)
    {
        return (s * multiplier + increment) & mask;
    }

    private static long lookupMultiplier(int bits)
    {
        if (bits <= 8) return MULT_8;
        if (bits <= 16) return MULT_16;
        if (bits <= 32) return MULT_32;
        return MULT_64;
    }

    private static long lookupMcgMultiplier(int bits)
    {
        if (bits <= 8) return MCG_8;
        if (bits <= 16) return MCG_16;
        if (bits <= 32) return MCG_32;
        return MCG_64;
    }

    private static long lookupDefaultIncrement(int bits)
    {
        if (bits <= 8) return INC_8;
        if (bits <= 16) return INC_16;
        if (bits <= 32) return INC_32;
        return INC_64;
    }

    private static long defaultStream(int bits)
    {
        return lookupDefaultIncrement(bits) >> 1;
    }

    // -- Output permutation: RXS-M-XS --

    private long output(long internal)
    {
        int bits = this.bitWidth;
        long m = this.mask;
        int op = this.opbits;

        // 1. Random xorshift: shift amount depends on high bits of the state
        int rshift = op > 0 ? (int) ((internal >>> (bits - op)) & ((1 << op) - 1)) : 0;
        internal = (internal ^ (internal >>> (op + rshift))) & m;

        // 2. MCG multiply
        internal = (internal * mcgMultiplier) & m;

        // 3. Fixed xorshift
        int fshift = (2 * bits + 2) / 3;
        internal = (internal ^ (internal >>> fshift)) & m;

        return internal;
    }

    // -- Public API --

    /**
     * Returns the next value in the permutation cycle, in the range [0, 2^bitWidth).
     */
    public long next()
    {
        long oldState = state;
        state = bump(state);
        return output(oldState);
    }

    public byte nextByte()
    {
        if (bitWidth != 8) throw new IllegalStateException("bitWidth is " + bitWidth + ", not 8");
        return (byte) next();
    }

    public short nextShort()
    {
        if (bitWidth != 16) throw new IllegalStateException("bitWidth is " + bitWidth + ", not 16");
        return (short) next();
    }

    public int nextInt()
    {
        if (bitWidth != 32) throw new IllegalStateException("bitWidth is " + bitWidth + ", not 32");
        return (int) next();
    }

    public long nextLong()
    {
        if (bitWidth != 64) throw new IllegalStateException("bitWidth is " + bitWidth + ", not 64");
        return next();
    }

    public int getBitWidth()
    {
        return bitWidth;
    }

    // -- Factory methods --

    public static PcgCycle ofBits(int bitWidth)
    {
        return new PcgCycle(bitWidth, 0L, defaultStream(bitWidth));
    }

    public static PcgCycle ofBits(int bitWidth, long seed)
    {
        return new PcgCycle(bitWidth, seed, defaultStream(bitWidth));
    }

    public static PcgCycle ofBits(int bitWidth, long seed, long stream)
    {
        return new PcgCycle(bitWidth, seed, stream);
    }

    public static PcgCycle ofByte()                          { return ofBits(8); }
    public static PcgCycle ofByte(long seed)                 { return ofBits(8, seed); }
    public static PcgCycle ofByte(long seed, long stream)    { return ofBits(8, seed, stream); }

    public static PcgCycle ofShort()                         { return ofBits(16); }
    public static PcgCycle ofShort(long seed)                { return ofBits(16, seed); }
    public static PcgCycle ofShort(long seed, long stream)   { return ofBits(16, seed, stream); }

    public static PcgCycle ofInt()                           { return ofBits(32); }
    public static PcgCycle ofInt(long seed)                  { return ofBits(32, seed); }
    public static PcgCycle ofInt(long seed, long stream)     { return ofBits(32, seed, stream); }

    public static PcgCycle ofLong()                          { return ofBits(64); }
    public static PcgCycle ofLong(long seed)                 { return ofBits(64, seed); }
    public static PcgCycle ofLong(long seed, long stream)    { return ofBits(64, seed, stream); }
}
