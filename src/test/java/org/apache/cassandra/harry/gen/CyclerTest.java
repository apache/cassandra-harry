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

import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.harry.checker.PropertyChecker;
import org.apache.cassandra.harry.gen.rng.Cycler;

public class CyclerTest
{
    private static final Generator<Long> SEEDS = Generators.int64();
    private static final Generator<Integer> SMALL_SIZES = Generators.int32(1, 500);
    private static final Generator<Integer> OFFSETS = Generators.int32(0, 1000);

    // -----------------------------------------------------------------------
    // Full-cycle permutation: every value appears exactly once
    // -----------------------------------------------------------------------

    @Test
    public void fullCycleIsPermutation() throws Throwable
    {
        PropertyChecker.forAll(SEEDS, SMALL_SIZES, OFFSETS)
                       .withRuns(100)
                       .withSeed(42L)
                       .check((seed, size, offset) -> {
                           long min = offset;
                           long max = min + size - 1;
                           Cycler c = new Cycler(seed, min, max);
                           Set<Long> seen = new HashSet<>(size * 2);
                           for (long i = 0; i < c.getCycle(); i++)
                           {
                               long val = c.next();
                               Assert.assertTrue("value " + val + " out of range [" + min + "," + max + "]",
                                                 val >= min && val <= max);
                               Assert.assertTrue("duplicate " + val + " at step " + i,
                                                 seen.add(val));
                           }
                           Assert.assertEquals(c.getCycle(), seen.size());
                       });
    }

    // -----------------------------------------------------------------------
    // Bijection: output set == {min, min+1, ..., max}
    // -----------------------------------------------------------------------

    @Test
    public void outputSetIsComplete() throws Throwable
    {
        PropertyChecker.forAll(SEEDS, Generators.int32(1, 300), OFFSETS)
                       .withRuns(100)
                       .withSeed(42L)
                       .check((seed, size, offset) -> {
                           long min = offset;
                           long max = min + size - 1;
                           Cycler c = new Cycler(seed, min, max);
                           Set<Long> outputs = new HashSet<>(size * 2);
                           for (long i = 0; i < c.getCycle(); i++)
                               outputs.add(c.next());
                           for (long v = min; v <= max; v++)
                               Assert.assertTrue("missing " + v, outputs.contains(v));
                       });
    }

    // -----------------------------------------------------------------------
    // Cycle length: sequence repeats exactly after one full cycle
    // -----------------------------------------------------------------------

    @Test
    public void cycleLengthIsExact() throws Throwable
    {
        PropertyChecker.forAll(SEEDS, Generators.int32(1, 200), OFFSETS)
                       .withRuns(100)
                       .withSeed(7L)
                       .check((seed, size, offset) -> {
                           long min = offset;
                           long max = min + size - 1;
                           Cycler c = new Cycler(seed, min, max);
                           long[] first = new long[(int) c.getCycle()];
                           for (int i = 0; i < first.length; i++)
                               first[i] = c.next();
                           for (int i = 0; i < first.length; i++)
                               Assert.assertEquals("cycle broken at step " + i, first[i], c.next());
                       });
    }

    // -----------------------------------------------------------------------
    // Determinism: same params always produce the same sequence
    // -----------------------------------------------------------------------

    @Test
    public void deterministic() throws Throwable
    {
        PropertyChecker.forAll(SEEDS, Generators.int32(1, 300), OFFSETS)
                       .withRuns(100)
                       .withSeed(42L)
                       .check((seed, size, offset) -> {
                           long min = offset;
                           long max = min + size - 1;
                           Cycler c1 = new Cycler(seed, min, max);
                           Cycler c2 = new Cycler(seed, min, max);
                           for (int i = 0; i < c1.getCycle(); i++)
                               Assert.assertEquals("mismatch at step " + i, c1.next(), c2.next());
                       });
    }

    // -----------------------------------------------------------------------
    // Different seeds produce different orderings
    // -----------------------------------------------------------------------

    @Test
    public void differentSeedsDiverge() throws Throwable
    {
        PropertyChecker.forAll(SEEDS, SEEDS)
                       .withRuns(100)
                       .withSeed(42L)
                       .check((seedA, seedB) -> {
                           if (seedA.equals(seedB)) return;
                           Cycler c1 = new Cycler(seedA, 0, 63);
                           Cycler c2 = new Cycler(seedB, 0, 63);
                           boolean diverged = false;
                           for (int i = 0; i < 64; i++)
                           {
                               if (c1.next() != c2.next()) { diverged = true; break; }
                           }
                           Assert.assertTrue("seeds " + seedA + " and " + seedB + " produced identical sequences",
                                             diverged);
                       });
    }

    // -----------------------------------------------------------------------
    // Single-element range always returns that element
    // -----------------------------------------------------------------------

    @Test
    public void singleElementRange() throws Throwable
    {
        PropertyChecker.forAll(SEEDS, Generators.int32(0, 10000))
                       .withRuns(100)
                       .withSeed(42L)
                       .check((seed, val) -> {
                           long v = val;
                           Cycler c = new Cycler(seed, v, v);
                           Assert.assertEquals(1, c.getCycle());
                           Assert.assertEquals(v, c.next());
                           Assert.assertEquals(v, c.next());
                       });
    }

    // -----------------------------------------------------------------------
    // Not identity: output order differs from input order
    // -----------------------------------------------------------------------

    @Test
    public void outputIsShuffled() throws Throwable
    {
        PropertyChecker.forAll(SEEDS)
                       .withRuns(100)
                       .withSeed(42L)
                       .check(seed -> {
                           Cycler c = new Cycler(seed, 0, 99);
                           boolean isIdentity = true;
                           for (int i = 0; i < 100; i++)
                           {
                               if (c.next() != i) { isIdentity = false; break; }
                           }
                           Assert.assertFalse("output was identity permutation for seed=" + seed, isIdentity);
                       });
    }

    // -----------------------------------------------------------------------
    // getCycle returns (max - min + 1)
    // -----------------------------------------------------------------------

    @Test
    public void getCycleMatchesRange() throws Throwable
    {
        PropertyChecker.forAll(Generators.int32(0, 10000), SMALL_SIZES)
                       .withRuns(100)
                       .withSeed(42L)
                       .check((offset, size) -> {
                           long min = offset;
                           long max = min + size - 1;
                           Cycler c = new Cycler(0L, min, max);
                           Assert.assertEquals((long) size, c.getCycle());
                       });
    }

    // -----------------------------------------------------------------------
    // Invalid range is rejected
    // -----------------------------------------------------------------------

    @Test
    public void invalidRangeRejected() throws Throwable
    {
        PropertyChecker.forAll(Generators.int32(1, 1000))
                       .withRuns(50)
                       .withSeed(42L)
                       .check(gap -> {
                           try
                           {
                               new Cycler(0L, (long) gap, 0L);
                               Assert.fail("Expected IllegalArgumentException for max < min");
                           }
                           catch (IllegalArgumentException ignored)
                           {
                           }
                       });
    }
}
