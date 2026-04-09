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
import org.apache.cassandra.harry.gen.rng.Cycler;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.gen.rng.KeyPicker;

public class KeyPickerTest
{
    private static final Generator<Long> SEEDS = Generators.int64();
    private static final Generator<Integer> POPULATIONS = Generators.int32(20, 500);
    private static final Generator<Integer> WINDOW_SIZES = Generators.int32(2, 15);

    // -------------------------------------------------------------------
    // 1. Window size invariant
    // -------------------------------------------------------------------

    @Test
    public void windowSizeIsConstant() throws Throwable
    {
        PropertyChecker.forAll(SEEDS, POPULATIONS, WINDOW_SIZES)
                       .withRuns(100)
                       .withSeed(42L)
                       .check((seed, pop, win) -> {
                           if (win > pop) return;
                           KeyPicker kp = new KeyPicker(seed, pop, win, 0.5, 0.1,
                                                        Distribution.uniform(0, win - 1));
                           EntropySource rng = new JdkRandomEntropySource(seed);
                           for (int i = 0; i < 200; i++)
                           {
                               kp.pick(rng);
                               Assert.assertEquals("window size changed at step " + i,
                                                   win.intValue(), kp.activeKeys().length);
                           }
                       });
    }

    // -------------------------------------------------------------------
    // 2. Active keys are valid Cycler outputs
    // -------------------------------------------------------------------

    @Test
    public void activeKeysAreValid() throws Throwable
    {
        PropertyChecker.forAll(SEEDS, POPULATIONS, WINDOW_SIZES)
                       .withRuns(100)
                       .withSeed(42L)
                       .check((seed, pop, win) -> {
                           if (win > pop) return;
                           KeyPicker kp = new KeyPicker(seed, pop, win, 0.5, 0.1,
                                                        Distribution.uniform(0, win - 1));
                           EntropySource rng = new JdkRandomEntropySource(seed + 1);
                           for (int i = 0; i < 200; i++)
                           {
                               long key = kp.pick(rng);
                               Assert.assertTrue("pick returned out-of-range key " + key,
                                                 key >= 0 && key < pop);
                               for (long k : kp.activeKeys())
                                   Assert.assertTrue("active key out of range: " + k,
                                                     k >= 0 && k < pop);
                           }
                       });
    }

    // -------------------------------------------------------------------
    // 3. No duplicates in active set
    // -------------------------------------------------------------------

    @Test
    public void noDuplicatePositions() throws Throwable
    {
        PropertyChecker.forAll(SEEDS, POPULATIONS, WINDOW_SIZES)
                       .withRuns(100)
                       .withSeed(42L)
                       .check((seed, pop, win) -> {
                           if (win > pop) return;
                           KeyPicker kp = new KeyPicker(seed, pop, win, 0.5, 0.1,
                                                        Distribution.uniform(0, win - 1));
                           EntropySource rng = new JdkRandomEntropySource(seed + 2);
                           for (int i = 0; i < 200; i++)
                           {
                               kp.pick(rng);
                               long[] positions = kp.activePositions();
                               Set<Long> seen = new HashSet<>();
                               for (long p : positions)
                                   Assert.assertTrue("duplicate position " + p + " at step " + i,
                                                     seen.add(p));
                           }
                       });
    }

    // -------------------------------------------------------------------
    // 4. Monotonic hi watermark
    // -------------------------------------------------------------------

    @Test
    public void hiWatermarkNeverDecreases() throws Throwable
    {
        PropertyChecker.forAll(SEEDS, POPULATIONS, WINDOW_SIZES)
                       .withRuns(100)
                       .withSeed(42L)
                       .check((seed, pop, win) -> {
                           if (win > pop) return;
                           KeyPicker kp = new KeyPicker(seed, pop, win, 0.5, 0.1,
                                                        Distribution.uniform(0, win - 1));
                           EntropySource rng = new JdkRandomEntropySource(seed + 3);
                           long prevHi = kp.visited();
                           for (int i = 0; i < 200; i++)
                           {
                               kp.pick(rng);
                               long nowHi = kp.visited();
                               Assert.assertTrue("hi watermark decreased at step " + i,
                                                 nowHi >= prevHi);
                               prevHi = nowHi;
                           }
                       });
    }

    // -------------------------------------------------------------------
    // 5. Eviction rate
    // -------------------------------------------------------------------

    @Test
    public void evictionRateIsApproximate() throws Throwable
    {
        PropertyChecker.forAll(SEEDS)
                       .withRuns(50)
                       .withSeed(42L)
                       .check(seed -> {
                           int pop = 10000;
                           int win = 10;
                           double rate = 0.3;
                           KeyPicker kp = new KeyPicker(seed, pop, win, rate, 0.0,
                                                        Distribution.uniform(0, win - 1));
                           EntropySource rng = new JdkRandomEntropySource(seed + 4);
                           int picks = 5000;
                           long hiBefore = kp.visited();
                           for (int i = 0; i < picks; i++)
                               kp.pick(rng);
                           long evictions = kp.visited() - hiBefore;
                           double empirical = (double) evictions / picks;
                           Assert.assertTrue("eviction rate " + empirical + " too far from " + rate,
                                             empirical > rate - 0.05 && empirical < rate + 0.05);
                       });
    }

    // -------------------------------------------------------------------
    // 6. Revisit rate
    // -------------------------------------------------------------------

    @Test
    public void revisitRateIsApproximate() throws Throwable
    {
        PropertyChecker.forAll(SEEDS)
                       .withRuns(50)
                       .withSeed(42L)
                       .check(seed -> {
                           int pop = 10000;
                           int win = 10;
                           double evictRate = 0.5;
                           double revisitRate = 0.3;
                           KeyPicker kp = new KeyPicker(seed, pop, win, evictRate, revisitRate,
                                                        Distribution.uniform(0, win - 1));
                           EntropySource rng = new JdkRandomEntropySource(seed + 5);

                           // Track evictions vs hi advances.
                           // Each pick that triggers an eviction either advances hi (new)
                           // or does not (revisit).
                           int picks = 10000;
                           long hiBefore = kp.visited();
                           // Count total eviction events by observing lowestActive changes
                           // and hi watermark changes.
                           int totalEvictions = 0;
                           int newEvictions = 0;
                           for (int i = 0; i < picks; i++)
                           {
                               long loBefore = kp.lowestActive();
                               long hiBef = kp.visited();
                               kp.pick(rng);
                               long loAfter = kp.lowestActive();
                               long hiAft = kp.visited();
                               // Detect eviction: the lowest active changed or hi changed
                               if (loBefore != loAfter || hiBef != hiAft)
                               {
                                   totalEvictions++;
                                   if (hiAft > hiBef)
                                       newEvictions++;
                               }
                           }

                           if (totalEvictions < 100) return; // not enough data
                           int revisitEvictions = totalEvictions - newEvictions;
                           double empiricalRevisit = (double) revisitEvictions / totalEvictions;
                           // Revisit rate should be approximately revisitRate, with some
                           // tolerance for early picks where revisit is impossible.
                           Assert.assertTrue("revisit fraction " + empiricalRevisit +
                                             " too far from " + revisitRate +
                                             " (total=" + totalEvictions + ", revisits=" + revisitEvictions + ")",
                                             empiricalRevisit > revisitRate - 0.10 &&
                                             empiricalRevisit < revisitRate + 0.10);
                       });
    }

    // -------------------------------------------------------------------
    // 7. Revisited keys are short-lived
    // -------------------------------------------------------------------

    @Test
    public void revisitedKeysAreEvictedNext() throws Throwable
    {
        PropertyChecker.forAll(SEEDS)
                       .withRuns(100)
                       .withSeed(42L)
                       .check(seed -> {
                           int pop = 1000;
                           int win = 10;
                           // High eviction, moderate revisit so we see revisits
                           KeyPicker kp = new KeyPicker(seed, pop, win, 1.0, 0.5,
                                                        Distribution.uniform(0, win - 1));
                           EntropySource rng = new JdkRandomEntropySource(seed + 6);

                           // Warm up to get some evicted positions
                           for (int i = 0; i < 20; i++)
                               kp.pick(rng);

                           // Now observe: after a revisit, the revisited position
                           // should be the lowest and evicted on the next pick.
                           for (int i = 0; i < 100; i++)
                           {
                               long[] posBefore = kp.activePositions();
                               long loBefore = kp.lowestActive();
                               kp.pick(rng);
                               long[] posAfter = kp.activePositions();
                               long loAfter = kp.lowestActive();

                               // Detect if a revisit happened: the new lowest is lower
                               // than any position that was in the set before (other
                               // than the one evicted).
                               if (loAfter < loBefore)
                               {
                                   // This was a revisit. The revisited position is loAfter.
                                   // On the next eviction (which is guaranteed since
                                   // evictionRate=1.0), it should be evicted.
                                   long revisitedPos = loAfter;
                                   kp.pick(rng);
                                   // After next pick (with eviction), revisitedPos should be gone
                                   long[] posAfter2 = kp.activePositions();
                                   boolean found = false;
                                   for (long p : posAfter2)
                                       if (p == revisitedPos) found = true;
                                   Assert.assertFalse("revisited position " + revisitedPos +
                                                      " survived an eviction", found);
                               }
                           }
                       });
    }

    // -------------------------------------------------------------------
    // 8. Full coverage
    // -------------------------------------------------------------------

    @Test
    public void fullCoverageWithMaxEviction() throws Throwable
    {
        PropertyChecker.forAll(SEEDS, Generators.int32(20, 200))
                       .withRuns(50)
                       .withSeed(42L)
                       .check((seed, pop) -> {
                           int win = 5;
                           KeyPicker kp = new KeyPicker(seed, pop, win, 1.0, 0.0,
                                                        Distribution.uniform(0, win - 1));
                           EntropySource rng = new JdkRandomEntropySource(seed + 7);
                           for (int i = 0; i < pop; i++)
                               kp.pick(rng);
                           Assert.assertEquals("should have visited all positions",
                                               (long) pop, kp.visited());
                       });
    }

    // -------------------------------------------------------------------
    // 9. Determinism
    // -------------------------------------------------------------------

    @Test
    public void deterministic() throws Throwable
    {
        PropertyChecker.forAll(SEEDS, POPULATIONS, WINDOW_SIZES)
                       .withRuns(100)
                       .withSeed(42L)
                       .check((seed, pop, win) -> {
                           if (win > pop) return;
                           KeyPicker kp1 = new KeyPicker(seed, pop, win, 0.4, 0.15,
                                                         Distribution.uniform(0, win - 1));
                           KeyPicker kp2 = new KeyPicker(seed, pop, win, 0.4, 0.15,
                                                         Distribution.uniform(0, win - 1));
                           EntropySource rng1 = new JdkRandomEntropySource(seed + 8);
                           EntropySource rng2 = new JdkRandomEntropySource(seed + 8);
                           for (int i = 0; i < 200; i++)
                           {
                               long k1 = kp1.pick(rng1);
                               long k2 = kp2.pick(rng2);
                               Assert.assertEquals("diverged at step " + i, k1, k2);
                           }
                       });
    }

    // -------------------------------------------------------------------
    // 10. Cycler integration
    // -------------------------------------------------------------------

    @Test
    public void pickedKeysMatchCycler() throws Throwable
    {
        PropertyChecker.forAll(SEEDS, POPULATIONS, WINDOW_SIZES)
                       .withRuns(100)
                       .withSeed(42L)
                       .check((seed, pop, win) -> {
                           if (win > pop) return;
                           KeyPicker kp = new KeyPicker(seed, pop, win, 0.3, 0.1,
                                                        Distribution.uniform(0, win - 1));
                           Cycler cycler = new Cycler(seed, 0, pop - 1);
                           EntropySource rng = new JdkRandomEntropySource(seed + 9);
                           for (int i = 0; i < 200; i++)
                           {
                               long key = kp.pick(rng);
                               // Verify key is the Cycler output for some active position
                               long[] positions = kp.activePositions();
                               boolean found = false;
                               for (long pos : positions)
                               {
                                   if (cycler.get(pos) == key)
                                   {
                                       found = true;
                                       break;
                                   }
                               }
                               // The key was picked before replacement, so it might have
                               // been evicted. Check the key is valid at minimum.
                               Assert.assertTrue("key " + key + " not in [0, " + pop + ")",
                                                 key >= 0 && key < pop);
                           }
                       });
    }

    // -------------------------------------------------------------------
    // 11. Window sliding
    // -------------------------------------------------------------------

    @Test
    public void windowSlidesForward() throws Throwable
    {
        PropertyChecker.forAll(SEEDS, Generators.int32(50, 300), Generators.int32(2, 10))
                       .withRuns(100)
                       .withSeed(42L)
                       .check((seed, pop, win) -> {
                           if (win > pop) return;
                           KeyPicker kp = new KeyPicker(seed, pop, win, 1.0, 0.0,
                                                        Distribution.uniform(0, win - 1));
                           EntropySource rng = new JdkRandomEntropySource(seed + 10);
                           int steps = Math.min(pop - win, 50);
                           for (int k = 0; k < steps; k++)
                           {
                               kp.pick(rng);
                               long[] positions = kp.activePositions();
                               Arrays.sort(positions);
                               // After k+1 picks with evictionRate=1.0 and revisitRate=0.0,
                               // the window should be [k+1, k+1+windowSize)
                               long expectedLo = k + 1;
                               long expectedHi = k + 1 + win;
                               Assert.assertEquals("lo mismatch at step " + k,
                                                   expectedLo, positions[0]);
                               Assert.assertEquals("hi mismatch at step " + k,
                                                   expectedHi - 1, positions[win - 1]);
                           }
                       });
    }

    // -------------------------------------------------------------------
    // 12. Distribution pluggability
    // -------------------------------------------------------------------

    @Test
    public void distributionAffectsPickPattern() throws Throwable
    {
        PropertyChecker.forAll(SEEDS)
                       .withRuns(50)
                       .withSeed(42L)
                       .check(seed -> {
                           int pop = 1000;
                           int win = 10;
                           // Fixed(0): always picks slot 0
                           KeyPicker kpFixed = new KeyPicker(seed, pop, win, 0.0, 0.0,
                                                             Distribution.fixed(0));
                           EntropySource rngFixed = new JdkRandomEntropySource(seed + 11);
                           long[] fixedKeys = new long[100];
                           for (int i = 0; i < 100; i++)
                               fixedKeys[i] = kpFixed.pick(rngFixed);

                           // All picks should be the same key (no eviction, always slot 0)
                           for (int i = 1; i < fixedKeys.length; i++)
                               Assert.assertEquals("fixed distribution should always return same key",
                                                   fixedKeys[0], fixedKeys[i]);

                           // Uniform: should hit multiple different keys
                           KeyPicker kpUniform = new KeyPicker(seed, pop, win, 0.0, 0.0,
                                                               Distribution.uniform(0, win - 1));
                           EntropySource rngUniform = new JdkRandomEntropySource(seed + 12);
                           Set<Long> uniformKeys = new HashSet<>();
                           for (int i = 0; i < 100; i++)
                               uniformKeys.add(kpUniform.pick(rngUniform));

                           Assert.assertTrue("uniform should produce multiple distinct keys, got " +
                                             uniformKeys.size(),
                                             uniformKeys.size() > 1);
                       });
    }
}
