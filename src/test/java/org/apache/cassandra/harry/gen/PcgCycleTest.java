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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.harry.checker.PropertyChecker;
import org.apache.cassandra.harry.gen.rng.PcgCycle;

public class PcgCycleTest
{
    private static final Generator<Integer> ALL_WIDTHS =
        Generators.pick(Arrays.asList(1, 2, 4, 8, 16, 32, 64));
    private static final Generator<Integer> EXHAUSTIVE_WIDTHS =
        Generators.pick(Arrays.asList(1, 2, 4, 8, 16));
    private static final Generator<Integer> SMALL_WIDTHS =
        Generators.pick(Arrays.asList(1, 2, 4, 8));

    // -----------------------------------------------------------------------
    // Full-cycle permutation: every value appears exactly once
    // -----------------------------------------------------------------------

    @Test
    public void fullCycleIsPermutation() throws Throwable
    {
        // For each exhaustive width and random seed, the first 2^width outputs
        // must be a permutation of [0, 2^width).
        PropertyChecker.forAll(EXHAUSTIVE_WIDTHS, Generators.int64())
                       .withRuns(50)
                       .withSeed(42L)
                       .check((width, seed) -> {
                           long size = 1L << width;
                           long mask = size - 1;
                           PcgCycle gen = PcgCycle.ofBits(width, seed);
                           Set<Long> seen = new HashSet<>((int) size * 2);
                           for (long i = 0; i < size; i++)
                           {
                               long v = gen.next();
                               Assert.assertTrue("value " + v + " out of range for " + width + "-bit",
                                                 v >= 0 && v <= mask);
                               Assert.assertTrue("duplicate at step " + i + " for " + width + "-bit, seed=" + seed,
                                                 seen.add(v));
                           }
                           Assert.assertEquals(size, seen.size());
                       });
    }

    // -----------------------------------------------------------------------
    // Cycle length: after exactly 2^width steps the sequence repeats
    // -----------------------------------------------------------------------

    @Test
    public void cycleLengthIsExact() throws Throwable
    {
        PropertyChecker.forAll(EXHAUSTIVE_WIDTHS, Generators.int64())
                       .withRuns(30)
                       .withSeed(7L)
                       .check((width, seed) -> {
                           long size = 1L << width;
                           PcgCycle gen = PcgCycle.ofBits(width, seed);

                           // Consume the full cycle
                           long first = gen.next();
                           for (long i = 1; i < size; i++)
                               gen.next();

                           // The next value should equal the first (cycle wrapped)
                           Assert.assertEquals("cycle did not wrap for " + width + "-bit, seed=" + seed,
                                               first, gen.next());
                       });
    }

    // -----------------------------------------------------------------------
    // Determinism: same (seed, stream) always produces the same sequence
    // -----------------------------------------------------------------------

    @Test
    public void deterministic() throws Throwable
    {
        PropertyChecker.forAll(ALL_WIDTHS, Generators.int64(), Generators.int64())
                       .withRuns(100)
                       .withSeed(42L)
                       .check((width, seed, stream) -> {
                           PcgCycle g1 = PcgCycle.ofBits(width, seed, stream);
                           PcgCycle g2 = PcgCycle.ofBits(width, seed, stream);
                           for (int i = 0; i < 200; i++)
                           {
                               Assert.assertEquals("diverged at step " + i +
                                                   " for " + width + "-bit, seed=" + seed + ", stream=" + stream,
                                                   g1.next(), g2.next());
                           }
                       });
    }

    // -----------------------------------------------------------------------
    // Different seeds produce different sequences
    // -----------------------------------------------------------------------

    @Test
    public void differentSeedsDiverge() throws Throwable
    {
        // Use widths >= 8 so that 20 output samples are enough to detect divergence.
        // Very small widths (1, 2, 4) have so few distinct outputs that collisions
        // over a short prefix are expected by chance.
        Generator<Integer> wideEnough = Generators.pick(Arrays.asList(8, 16, 32, 64));
        PropertyChecker.forAll(wideEnough, Generators.int64(), Generators.int64())
                       .withRuns(100)
                       .withSeed(42L)
                       .check((width, seedA, seedB) -> {
                           if (seedA.equals(seedB)) return;
                           PcgCycle g1 = PcgCycle.ofBits(width, seedA);
                           PcgCycle g2 = PcgCycle.ofBits(width, seedB);
                           boolean diverged = false;
                           for (int i = 0; i < 20; i++)
                           {
                               if (g1.next() != g2.next()) { diverged = true; break; }
                           }
                           Assert.assertTrue("seeds " + seedA + " and " + seedB +
                                             " produced identical sequences for " + width + "-bit",
                                             diverged);
                       });
    }

    // -----------------------------------------------------------------------
    // Different streams produce different permutations
    // -----------------------------------------------------------------------

    @Test
    public void differentStreamsDiverge() throws Throwable
    {
        Generator<Integer> wideEnough = Generators.pick(Arrays.asList(8, 16, 32, 64));
        PropertyChecker.forAll(wideEnough, Generators.int64(), Generators.int64())
                       .withRuns(100)
                       .withSeed(42L)
                       .check((width, seed, streamOffset) -> {
                           long s1 = 0;
                           long s2 = (streamOffset == 0) ? 1 : streamOffset;
                           PcgCycle g1 = PcgCycle.ofBits(width, seed, s1);
                           PcgCycle g2 = PcgCycle.ofBits(width, seed, s2);
                           boolean diverged = false;
                           for (int i = 0; i < 20; i++)
                           {
                               if (g1.next() != g2.next()) { diverged = true; break; }
                           }
                           Assert.assertTrue("streams 0 and " + s2 +
                                             " produced identical sequences for " + width + "-bit, seed=" + seed,
                                             diverged);
                       });
    }

    // -----------------------------------------------------------------------
    // Output values are always within the bit-width mask
    // -----------------------------------------------------------------------

    @Test
    public void outputWithinMask() throws Throwable
    {
        PropertyChecker.forAll(ALL_WIDTHS, Generators.int64())
                       .withRuns(100)
                       .withSeed(42L)
                       .check((width, seed) -> {
                           PcgCycle gen = PcgCycle.ofBits(width, seed);
                           long mask = width == 64 ? -1L : (1L << width) - 1;
                           for (int i = 0; i < 1000; i++)
                           {
                               long v = gen.next();
                               Assert.assertEquals("value " + v + " has bits outside " + width + "-bit mask",
                                                   0L, v & ~mask);
                           }
                       });
    }

    // -----------------------------------------------------------------------
    // Large widths (32, 64): statistical uniqueness over a sample
    // -----------------------------------------------------------------------

    @Test
    public void largeWidthSampleUniqueness() throws Throwable
    {
        PropertyChecker.forAll(Generators.pick(Arrays.asList(32, 64)), Generators.int64())
                       .withRuns(10)
                       .withSeed(42L)
                       .check((width, seed) -> {
                           PcgCycle gen = PcgCycle.ofBits(width, seed);
                           int sampleSize = 500_000;
                           Set<Long> seen = new HashSet<>(sampleSize * 2);
                           for (int i = 0; i < sampleSize; i++)
                               seen.add(gen.next());
                           Assert.assertEquals("duplicates in " + sampleSize + " samples for " +
                                               width + "-bit, seed=" + seed,
                                               sampleSize, seen.size());
                       });
    }

    // -----------------------------------------------------------------------
    // 32-bit nextInt covers both positive and negative values
    // -----------------------------------------------------------------------

    @Test
    public void nextIntCoversSignedRange() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> {
                           PcgCycle gen = PcgCycle.ofInt(seed);
                           boolean hasNeg = false, hasPos = false;
                           for (int i = 0; i < 100_000 && !(hasNeg && hasPos); i++)
                           {
                               int v = gen.nextInt();
                               if (v < 0) hasNeg = true;
                               if (v > 0) hasPos = true;
                           }
                           Assert.assertTrue("no negative values for seed=" + seed, hasNeg);
                           Assert.assertTrue("no positive values for seed=" + seed, hasPos);
                       });
    }

    // -----------------------------------------------------------------------
    // Typed accessors reject mismatched bit widths
    // -----------------------------------------------------------------------

    @Test
    public void typedAccessorsRejectWrongWidth() throws Throwable
    {
        PropertyChecker.forAll(ALL_WIDTHS)
                       .withRuns(7)
                       .withSeed(42L)
                       .check(width -> {
                           PcgCycle gen = PcgCycle.ofBits(width);
                           if (width != 8)  assertThrows(IllegalStateException.class, gen::nextByte);
                           if (width != 16) assertThrows(IllegalStateException.class, gen::nextShort);
                           if (width != 32) assertThrows(IllegalStateException.class, gen::nextInt);
                           if (width != 64) assertThrows(IllegalStateException.class, gen::nextLong);
                       });
    }

    // -----------------------------------------------------------------------
    // Invalid bit widths are rejected
    // -----------------------------------------------------------------------

    @Test
    public void invalidBitWidthsRejected() throws Throwable
    {
        PropertyChecker.forAll(Generators.pick(Arrays.asList(0, 3, 5, 6, 7, 9, 15, 33, 65, 128)))
                       .withRuns(10)
                       .withSeed(42L)
                       .check(width -> assertThrows(IllegalArgumentException.class,
                                                    () -> PcgCycle.ofBits(width)));
    }

    // -----------------------------------------------------------------------
    // getBitWidth returns the configured width
    // -----------------------------------------------------------------------

    @Test
    public void getBitWidthReturnsConfiguredWidth() throws Throwable
    {
        PropertyChecker.forAll(ALL_WIDTHS)
                       .withRuns(7)
                       .withSeed(42L)
                       .check(width -> Assert.assertEquals((int) width, PcgCycle.ofBits(width).getBitWidth()));
    }

    // -----------------------------------------------------------------------
    // Full permutation holds across multiple streams (exhaustive widths)
    // -----------------------------------------------------------------------

    @Test
    public void fullPermutationAcrossStreams() throws Throwable
    {
        PropertyChecker.forAll(SMALL_WIDTHS, Generators.int64())
                       .withRuns(30)
                       .withSeed(42L)
                       .check((width, stream) -> {
                           long size = 1L << width;
                           PcgCycle gen = PcgCycle.ofBits(width, 0L, stream);
                           Set<Long> seen = new HashSet<>((int) size * 2);
                           for (long i = 0; i < size; i++)
                               seen.add(gen.next());
                           Assert.assertEquals("not a full permutation for " + width + "-bit, stream=" + stream,
                                               size, seen.size());
                       });
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static void assertThrows(Class<? extends Throwable> expected, Runnable block)
    {
        try
        {
            block.run();
            Assert.fail("Expected " + expected.getSimpleName() + " but nothing was thrown");
        }
        catch (Throwable t)
        {
            Assert.assertTrue("Expected " + expected.getSimpleName() + " but got " + t.getClass().getSimpleName(),
                              expected.isInstance(t));
        }
    }
}
