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

package org.apache.cassandra.harry.checker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.harry.gen.Generators;

import static org.apache.cassandra.harry.checker.Properties.*;

public class PropertyCheckerTest
{
    // -----------------------------------------------------------------------
    // Quick
    // -----------------------------------------------------------------------

    @Test
    public void quickRunsCorrectNumberOfTimes() throws Throwable
    {
        AtomicInteger count = new AtomicInteger();
        PropertyChecker.quick()
                       .withRuns(50)
                       .withSeed(42L)
                       .check(rng -> count.incrementAndGet());
        Assert.assertEquals(50, count.get());
    }

    @Test
    public void quickSeedIsReproducible() throws Throwable
    {
        List<Long> run1 = new ArrayList<>();
        List<Long> run2 = new ArrayList<>();

        PropertyChecker.quick()
                       .withRuns(10)
                       .withSeed(123L)
                       .check(rng -> run1.add(rng.next()));

        PropertyChecker.quick()
                       .withRuns(10)
                       .withSeed(123L)
                       .check(rng -> run2.add(rng.next()));

        Assert.assertEquals(run1, run2);
    }

    @Test
    public void quickReportsSeedOnFailure() throws Throwable
    {
        long seed = 999L;
        try
        {
            PropertyChecker.quick()
                           .withRuns(10)
                           .withSeed(seed)
                           .check(rng -> { throw new RuntimeException("boom"); });
            Assert.fail("Should have thrown");
        }
        catch (AssertionError e)
        {
            Assert.assertTrue("Message should contain seed, got: " + e.getMessage(),
                              e.getMessage().contains("seed:" + seed + "L"));
            Assert.assertTrue("Message should contain run number",
                              e.getMessage().contains("run 1 of 10"));
        }
    }

    // -----------------------------------------------------------------------
    // forAll (single generator)
    // -----------------------------------------------------------------------

    @Test
    public void forAllSingleGenerator() throws Throwable
    {
        PropertyChecker.forAll(Generators.int32())
                       .withRuns(200)
                       .withSeed(42L)
                       .check(v -> {
                           // int identity
                       });
    }

    @Test
    public void forAllReportsSeedAndValueOnFailure() throws Throwable
    {
        long seed = 777L;
        try
        {
            PropertyChecker.forAll(Generators.int32())
                           .withRuns(100)
                           .withSeed(seed)
                           .check(v -> {
                               throw new RuntimeException("fail");
                           });
            Assert.fail("Should have thrown");
        }
        catch (AssertionError e)
        {
            Assert.assertTrue("Message should contain seed", e.getMessage().contains("seed:" + seed + "L"));
            Assert.assertTrue("Message should contain 'value'", e.getMessage().contains("value:"));
        }
    }

    // -----------------------------------------------------------------------
    // forAll (two generators)
    // -----------------------------------------------------------------------

    @Test
    public void forAllTwoGenerators() throws Throwable
    {
        PropertyChecker.forAll(Generators.int32(), Generators.int32())
                       .withRuns(200)
                       .withSeed(42L)
                       .check((a, b) -> Assert.assertEquals((long) (a + b), (long) (b + a)));
    }

    // -----------------------------------------------------------------------
    // forAll (three generators)
    // -----------------------------------------------------------------------

    @Test
    public void forAllThreeGenerators() throws Throwable
    {
        PropertyChecker.forAll(Generators.int32(), Generators.int32(), Generators.int32())
                       .withRuns(200)
                       .withSeed(42L)
                       .check((a, b, c) -> Assert.assertEquals((long) ((a + b) + c), (long) (a + (b + c))));
    }

    // -----------------------------------------------------------------------
    // Stateful (state-only)
    // -----------------------------------------------------------------------

    @Test
    public void statefulStateOnly() throws Throwable
    {
        AtomicInteger state = new AtomicInteger(0);
        PropertyChecker.stateful(state)
                       .withRuns(100)
                       .withSeed(42L)
                       .step(s -> { s.incrementAndGet(); return s; })
                       .invariant(s -> s.get() >= 0)
                       .check();
    }

    // -----------------------------------------------------------------------
    // Stateful (model + SUT)
    // -----------------------------------------------------------------------

    @Test
    public void statefulWithSut() throws Throwable
    {
        PropertyChecker.stateful(new ArrayList<Integer>(), new ArrayList<Integer>())
                       .withRuns(100)
                       .withSeed(42L)
                       .step((model, sut, rng) -> {
                           int val = rng.nextInt(1000);
                           model.add(val);
                           sut.add(val);
                           return new ModelChecker.Pair<>(model, sut);
                       })
                       .invariant((model, sut) -> {
                           Assert.assertEquals(model.size(), sut.size());
                           return true;
                       })
                       .check();
    }

    // -----------------------------------------------------------------------
    // Properties: roundtrip
    // -----------------------------------------------------------------------

    @Test
    public void testRoundtrip() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(1000)
                       .withSeed(42L)
                       .check(roundtrip(v -> Long.toHexString(v), v -> Long.parseUnsignedLong(v, 16)));
    }

    @Test
    public void testRoundtripFailure() throws Throwable
    {
        try
        {
            PropertyChecker.forAll(Generators.int32())
                           .withRuns(100)
                           .withSeed(42L)
                           .check(roundtrip(v -> v + 1, v -> v));  // deliberately broken
            Assert.fail("Should have thrown");
        }
        catch (AssertionError e)
        {
            Assert.assertTrue("Should mention roundtrip", e.getCause().getMessage().contains("roundtrip"));
        }
    }

    // -----------------------------------------------------------------------
    // Properties: invariant
    // -----------------------------------------------------------------------

    @Test
    public void testInvariant() throws Throwable
    {
        PropertyChecker.forAll(Generators.int32().map(v -> Math.abs(v)))
                       .withRuns(500)
                       .withSeed(42L)
                       .check(invariant(v -> v >= 0, "abs is non-negative"));
    }

    // -----------------------------------------------------------------------
    // Properties: idempotent
    // -----------------------------------------------------------------------

    @Test
    public void testIdempotent() throws Throwable
    {
        PropertyChecker.forAll(Generators.int32())
                       .withRuns(500)
                       .withSeed(42L)
                       .check(idempotent(v -> Math.abs(Math.abs(v))));
    }

    // -----------------------------------------------------------------------
    // Properties: commutative
    // -----------------------------------------------------------------------

    @Test
    public void testCommutative() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64(), Generators.int64())
                       .withRuns(500)
                       .withSeed(42L)
                       .check(commutative((a, b) -> a + b));
    }

    // -----------------------------------------------------------------------
    // Properties: sorted
    // -----------------------------------------------------------------------

    @Test
    public void testSorted() throws Throwable
    {
        PropertyChecker.forAll(Generators.int32().map(v -> {
                           List<Integer> list = new ArrayList<>();
                           for (int i = 0; i < 10; i++) list.add(i);
                           return list;
                       }))
                       .withRuns(100)
                       .withSeed(42L)
                       .check(sorted(Integer::compareTo));
    }

    // -----------------------------------------------------------------------
    // skipToRun
    // -----------------------------------------------------------------------

    @Test
    public void skipToRunProducesSameRngState() throws Throwable
    {
        long seed = 42L;
        int failRun = 50;

        // Collect the value seen at run 50 by running all 50 iterations
        List<Long> fullRun = new ArrayList<>();
        PropertyChecker.quick()
                       .withRuns(failRun)
                       .withSeed(seed)
                       .check(rng -> fullRun.add(rng.next()));

        // Now skip to run 50 and collect just that one value
        List<Long> skippedRun = new ArrayList<>();
        PropertyChecker.quick()
                       .withRuns(failRun)
                       .withSeed(seed)
                       .skipToRun(failRun)
                       .check(rng -> skippedRun.add(rng.next()));

        Assert.assertEquals(1, skippedRun.size());
        Assert.assertEquals("Value at run 50 should match",
                            fullRun.get(fullRun.size() - 1), skippedRun.get(0));
    }

    @Test
    public void skipToRunWithGeneratorProducesSameValue() throws Throwable
    {
        long seed = 42L;
        int failRun = 30;

        List<Integer> fullRun = new ArrayList<>();
        PropertyChecker.forAll(Generators.int32())
                       .withRuns(failRun)
                       .withSeed(seed)
                       .check(v -> fullRun.add(v));

        List<Integer> skippedRun = new ArrayList<>();
        PropertyChecker.forAll(Generators.int32())
                       .withRuns(failRun)
                       .withSeed(seed)
                       .skipToRun(failRun)
                       .check(v -> skippedRun.add(v));

        Assert.assertEquals(1, skippedRun.size());
        Assert.assertEquals("Value at run 30 should match",
                            fullRun.get(fullRun.size() - 1), skippedRun.get(0));
    }

    @Test
    public void skipToRunOnlyExecutesRemainingRuns() throws Throwable
    {
        AtomicInteger count = new AtomicInteger();
        PropertyChecker.quick()
                       .withRuns(100)
                       .withSeed(42L)
                       .skipToRun(91)
                       .check(rng -> count.incrementAndGet());
        Assert.assertEquals(10, count.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void skipToRunRejectsZero() throws Throwable
    {
        PropertyChecker.quick().skipToRun(0);
    }

    // -----------------------------------------------------------------------
    // Default runs
    // -----------------------------------------------------------------------

    @Test
    public void defaultRunsIs100() throws Throwable
    {
        AtomicInteger count = new AtomicInteger();
        PropertyChecker.quick()
                       .withSeed(42L)
                       .check(rng -> count.incrementAndGet());
        Assert.assertEquals(100, count.get());
    }
}
