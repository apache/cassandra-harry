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

import org.junit.Test;

import org.apache.cassandra.harry.checker.PropertyChecker;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;

import static org.junit.Assert.*;

public class DistributionTest
{
    private static final int SAMPLE_COUNT = 1_000_000;

    // -- Harness --

    static void assertDistribution(Distribution dist, int sampleCount, long seed, Expectation... expectations)
    {
        EntropySource rng = new JdkRandomEntropySource(seed);
        long range = dist.max() - dist.min() + 1;
        long[] histogram = new long[(int) range];

        for (int i = 0; i < sampleCount; i++)
        {
            long value = dist.next(rng);
            assertTrue("Value " + value + " below min " + dist.min(), value >= dist.min());
            assertTrue("Value " + value + " above max " + dist.max(), value <= dist.max());
            histogram[(int) (value - dist.min())]++;
        }

        for (Expectation exp : expectations)
            exp.check(histogram, dist.min(), dist.max(), sampleCount);
    }

    // -- Expectation predicates --

    interface Expectation
    {
        void check(long[] histogram, long min, long max, int sampleCount);
    }

    static Expectation meanInRange(double lowFrac, double highFrac)
    {
        return (histogram, min, max, sampleCount) -> {
            long range = max - min + 1;
            double mean = empiricalMean(histogram, min, sampleCount);
            double frac = (mean - min) / range;
            assertTrue(String.format("meanInRange: expected [%.4f, %.4f] but got %.4f (mean=%.1f)",
                                     lowFrac, highFrac, frac, mean),
                       frac >= lowFrac && frac <= highFrac);
        };
    }

    static Expectation medianInRange(double lowFrac, double highFrac)
    {
        return (histogram, min, max, sampleCount) -> {
            long range = max - min + 1;
            double median = empiricalMedian(histogram, sampleCount);
            double frac = median / range;
            assertTrue(String.format("medianInRange: expected [%.4f, %.4f] but got %.4f (median=%.1f)",
                                     lowFrac, highFrac, frac, median),
                       frac >= lowFrac && frac <= highFrac);
        };
    }

    static Expectation stddevInRange(double lowFrac, double highFrac)
    {
        return (histogram, min, max, sampleCount) -> {
            long range = max - min + 1;
            double stddev = empiricalStddev(histogram, min, sampleCount);
            double frac = stddev / range;
            assertTrue(String.format("stddevInRange: expected [%.4f, %.4f] but got %.4f (stddev=%.1f)",
                                     lowFrac, highFrac, frac, stddev),
                       frac >= lowFrac && frac <= highFrac);
        };
    }

    static Expectation massInRange(double rangeLo, double rangeHi, double minMass, double maxMass)
    {
        return (histogram, min, max, sampleCount) -> {
            long range = max - min + 1;
            int lo = (int) (rangeLo * range);
            int hi = (int) (rangeHi * range);
            long count = 0;
            for (int i = lo; i < hi && i < histogram.length; i++)
                count += histogram[i];
            double mass = (double) count / sampleCount;
            assertTrue(String.format("massInRange([%.2f,%.2f)): expected mass [%.4f, %.4f] but got %.4f",
                                     rangeLo, rangeHi, minMass, maxMass, mass),
                       mass >= minMass && mass <= maxMass);
        };
    }

    static Expectation topKMass(int k, double minMass)
    {
        return (histogram, min, max, sampleCount) -> {
            long[] sorted = histogram.clone();
            Arrays.sort(sorted);
            long topK = 0;
            for (int i = sorted.length - 1; i >= Math.max(0, sorted.length - k); i--)
                topK += sorted[i];
            double mass = (double) topK / sampleCount;
            assertTrue(String.format("topKMass(k=%d): expected >= %.4f but got %.4f", k, minMass, mass),
                       mass >= minMass);
        };
    }

    static Expectation allValuesHit()
    {
        return (histogram, min, max, sampleCount) -> {
            for (int i = 0; i < histogram.length; i++)
                assertTrue("allValuesHit: value " + (min + i) + " was never sampled", histogram[i] > 0);
        };
    }

    static Expectation monotoneDecreasing(int buckets)
    {
        return (histogram, min, max, sampleCount) -> {
            long range = max - min + 1;
            long[] binCounts = new long[buckets];
            for (int i = 0; i < histogram.length; i++)
            {
                int bin = (int) ((long) i * buckets / range);
                if (bin >= buckets) bin = buckets - 1;
                binCounts[bin] += histogram[i];
            }
            for (int i = 1; i < buckets; i++)
            {
                // 5% tolerance
                double tolerance = 0.05 * sampleCount;
                assertTrue(String.format("monotoneDecreasing: bin[%d]=%d > bin[%d]=%d + %.0f tolerance",
                                         i, binCounts[i], i - 1, binCounts[i - 1], tolerance),
                           binCounts[i] <= binCounts[i - 1] + tolerance);
            }
        };
    }

    // -- Statistics helpers --

    private static double empiricalMean(long[] histogram, long min, int sampleCount)
    {
        double sum = 0;
        for (int i = 0; i < histogram.length; i++)
            sum += (double)(min + i) * histogram[i];
        return sum / sampleCount;
    }

    private static double empiricalMedian(long[] histogram, int sampleCount)
    {
        long cumulative = 0;
        long target = sampleCount / 2;
        for (int i = 0; i < histogram.length; i++)
        {
            cumulative += histogram[i];
            if (cumulative >= target)
                return i;
        }
        return histogram.length - 1;
    }

    private static double empiricalStddev(long[] histogram, long min, int sampleCount)
    {
        double mean = empiricalMean(histogram, min, sampleCount);
        double sumSq = 0;
        for (int i = 0; i < histogram.length; i++)
        {
            double diff = (min + i) - mean;
            sumSq += diff * diff * histogram[i];
        }
        return Math.sqrt(sumSq / sampleCount);
    }

    // -- Distribution tests --

    @Test
    public void testUniform() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> assertDistribution(Distribution.uniform(0, 9999), SAMPLE_COUNT, seed,
                                                         meanInRange(0.49, 0.51),
                                                         stddevInRange(0.27, 0.30),
                                                         massInRange(0.0, 0.5, 0.49, 0.51),
                                                         massInRange(0.0, 0.1, 0.09, 0.11)));
    }

    @Test
    public void testFixed() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(50)
                       .withSeed(42L)
                       .check(seed -> {
                           Distribution dist = Distribution.fixed(5000);
                           EntropySource rng = new JdkRandomEntropySource(seed);
                           for (int i = 0; i < 1000; i++)
                               assertEquals(5000, dist.next(rng));
                       });
    }

    @Test
    public void testGaussianDefault() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> assertDistribution(Distribution.gaussian(0, 9999), SAMPLE_COUNT, seed,
                                                         meanInRange(0.49, 0.51),
                                                         stddevInRange(0.14, 0.19),
                                                         massInRange(0.33, 0.67, 0.67, 0.70),
                                                         massInRange(0.0, 0.1, 0.00, 0.03),
                                                         massInRange(0.9, 1.0, 0.00, 0.03)));
    }

    @Test
    public void testGaussianTight() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> assertDistribution(Distribution.gaussian(0, 9999, 6), SAMPLE_COUNT, seed,
                                                         meanInRange(0.49, 0.51),
                                                         massInRange(0.4, 0.6, 0.75, 0.80),
                                                         massInRange(0.0, 0.1, 0.00, 0.005)));
    }

    @Test
    public void testExponential() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> assertDistribution(Distribution.exponential(0, 9999), SAMPLE_COUNT, seed,
                                                         meanInRange(0.05, 0.20),
                                                         medianInRange(0.01, 0.12),
                                                         massInRange(0.0, 0.1, 0.40, 0.75),
                                                         massInRange(0.5, 1.0, 0.005, 0.18),
                                                         monotoneDecreasing(20)));
    }

    @Test
    public void testExtremeHeavySkew() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> assertDistribution(Distribution.extreme(0, 9999, 0.5), SAMPLE_COUNT, seed,
                                                         meanInRange(0.01, 0.05),
                                                         massInRange(0.0, 0.01, 0.55, 0.65),
                                                         monotoneDecreasing(20)));
    }

    @Test
    public void testExtremeModerateSkew() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> assertDistribution(Distribution.extreme(0, 9999, 2), SAMPLE_COUNT, seed,
                                                         meanInRange(0.25, 0.35),
                                                         massInRange(0.0, 0.1, 0.07, 0.11),
                                                         massInRange(0.0, 0.5, 0.88, 0.92)));
    }

    @Test
    public void testExtremeNearlySymmetric() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> assertDistribution(Distribution.extreme(0, 9999, 5), SAMPLE_COUNT, seed,
                                                         meanInRange(0.55, 0.63),
                                                         massInRange(0.0, 0.5, 0.22, 0.28)));
    }

    @Test
    public void testQuantizedExtreme() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> assertDistribution(Distribution.quantizedExtreme(0, 9999, 2, 10), SAMPLE_COUNT, seed,
                                                         meanInRange(0.25, 0.35),
                                                         massInRange(0.0, 0.1, 0.07, 0.11)));
    }

    @Test
    public void testZipfianClassic() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(10)
                       .withSeed(42L)
                       .check(seed -> assertDistribution(Distribution.zipfian(0, 9999, 1.0), 2_000_000, seed,
                                                         meanInRange(0.05, 0.15),
                                                         topKMass(10, 0.25),
                                                         topKMass(100, 0.50),
                                                         massInRange(0.5, 1.0, 0.00, 0.10),
                                                         monotoneDecreasing(20)));
    }

    @Test
    public void testZipfianMild() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(10)
                       .withSeed(42L)
                       .check(seed -> assertDistribution(Distribution.zipfian(0, 9999, 0.5), 2_000_000, seed,
                                                         meanInRange(0.25, 0.40),
                                                         topKMass(100, 0.08),
                                                         massInRange(0.0, 0.1, 0.25, 0.40)));
    }

    @Test
    public void testHotspot() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> assertDistribution(Distribution.hotspot(0, 9999, 0.2, 0.8), SAMPLE_COUNT, seed,
                                                         massInRange(0.0, 0.2, 0.79, 0.81),
                                                         massInRange(0.2, 1.0, 0.19, 0.21),
                                                         meanInRange(0.15, 0.30)));
    }

    @Test
    public void testHotspotExtreme() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> assertDistribution(Distribution.hotspot(0, 9999, 0.1, 0.95), SAMPLE_COUNT, seed,
                                                         massInRange(0.0, 0.1, 0.94, 0.96),
                                                         massInRange(0.1, 1.0, 0.04, 0.06)));
    }

    @Test
    public void testWeighted() throws Throwable
    {
        // weights: 1, 2, 10, 20, 100, 1000, 10000
        // Total = 11133. Value 6 gets 10000/11133 ~ 89.8% of samples.
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> {
                           Distribution dist = Distribution.weighted(0, 6, 1, 2, 10, 20, 100, 1000, 10000);
                           EntropySource rng = new JdkRandomEntropySource(seed);
                           long[] counts = new long[7];
                           int n = SAMPLE_COUNT;
                           for (int i = 0; i < n; i++)
                           {
                               long v = dist.next(rng);
                               assertTrue(v >= 0 && v <= 6);
                               counts[(int) v]++;
                           }
                           // Value 6 should get ~89.8% of traffic
                           double frac6 = (double) counts[6] / n;
                           assertTrue(String.format("Value 6 should get ~89%% but got %.1f%%", frac6 * 100),
                                      frac6 > 0.88 && frac6 < 0.92);
                           // Value 0 should get ~0.009%
                           double frac0 = (double) counts[0] / n;
                           assertTrue(String.format("Value 0 should get <0.1%% but got %.4f%%", frac0 * 100),
                                      frac0 < 0.001);
                           // Ratios should roughly match weights
                           // count[6]/count[5] ~ 10000/1000 = 10
                           if (counts[5] > 0)
                           {
                               double ratio = (double) counts[6] / counts[5];
                               assertTrue(String.format("Ratio 6/5 should be ~10 but got %.1f", ratio),
                                          ratio > 8 && ratio < 12);
                           }
                       });
    }

    @Test
    public void testWeightedWithRange() throws Throwable
    {
        // 3 weights spread over [0, 9999]: equal weights = uniform
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> assertDistribution(Distribution.weighted(0, 9999, 1, 1, 1), SAMPLE_COUNT, seed,
                                                         meanInRange(0.49, 0.51),
                                                         massInRange(0.0, 0.33, 0.32, 0.35),
                                                         massInRange(0.33, 0.67, 0.32, 0.35)));
    }

    @Test
    public void testSequential() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> {
                           Distribution dist = Distribution.sequential(0, 99);
                           EntropySource rng = new JdkRandomEntropySource(seed);

                           // First pass: every value exactly once, in order
                           boolean[] seen = new boolean[100];
                           for (int i = 0; i < 100; i++)
                           {
                               long v = dist.next(rng);
                               seen[(int) v] = true;
                           }
                           for (int i = 0; i < 100; i++)
                               assertTrue("Value " + i + " was not produced", seen[i]);
                       });
    }

    @Test
    public void testSequentialWrapsAround() throws Throwable
    {
        // Sequential is stateful (AtomicLong counter), so wrap-around needs a
        // fresh instance per run. The seed does not affect sequential output,
        // but we still exercise it through PropertyChecker for consistency.
        PropertyChecker.forAll(Generators.int32(10, 500))
                       .withRuns(30)
                       .withSeed(42L)
                       .check(rangeSize -> {
                           Distribution dist = Distribution.sequential(0, rangeSize - 1);
                           EntropySource rng = new JdkRandomEntropySource(0L);

                           // Consume one full cycle
                           for (int i = 0; i < rangeSize; i++)
                               assertEquals("Sequential should return values in order", (long) i, dist.next(rng));

                           // Second cycle wraps
                           for (int i = 0; i < rangeSize; i++)
                               assertEquals("Sequential should wrap around", (long) i, dist.next(rng));
                       });
    }

    @Test
    public void testInvertedExponential() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> {
                           long min = 0, max = 9999;
                           Distribution exp = Distribution.exponential(min, max);
                           Distribution invExp = Distribution.invert(exp);

                           EntropySource rng1 = new JdkRandomEntropySource(seed);
                           EntropySource rng2 = new JdkRandomEntropySource(seed);

                           long range = max - min + 1;
                           double sumOrig = 0, sumInv = 0;
                           int n = SAMPLE_COUNT;
                           for (int i = 0; i < n; i++)
                           {
                               sumOrig += exp.next(rng1);
                               sumInv += invExp.next(rng2);
                           }
                           double meanOrigFrac = (sumOrig / n - min) / range;
                           double meanInvFrac = (sumInv / n - min) / range;

                           double diff = Math.abs(meanInvFrac - (1.0 - meanOrigFrac));
                           assertTrue(String.format("Inverted mean %.4f should be ~ 1 - original mean %.4f, diff=%.4f",
                                                    meanInvFrac, meanOrigFrac, diff),
                                      diff < 0.02);
                       });
    }

    @Test
    public void testInvertedGaussian() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> {
                           Distribution invGauss = Distribution.invert(Distribution.gaussian(0, 9999));
                           assertDistribution(invGauss, SAMPLE_COUNT, seed,
                                              meanInRange(0.49, 0.51));
                       });
    }

    @Test
    public void testDoubleInvertUnwraps()
    {
        Distribution exp = Distribution.exponential(0, 9999);
        Distribution doubleInv = Distribution.invert(Distribution.invert(exp));
        assertSame("Double invert should unwrap", exp, doubleInv);
    }

    // -- Determinism --

    @Test
    public void testDeterminism() throws Throwable
    {
        Distribution[] dists = {
            Distribution.uniform(0, 9999),
            Distribution.fixed(5000),
            Distribution.gaussian(0, 9999),
            Distribution.gaussian(0, 9999, 6),
            Distribution.exponential(0, 9999),
            Distribution.extreme(0, 9999, 0.5),
            Distribution.extreme(0, 9999, 2),
            Distribution.quantizedExtreme(0, 9999, 2, 10),
            Distribution.zipfian(0, 9999, 1.0),
            Distribution.hotspot(0, 9999, 0.2, 0.8),
            Distribution.invert(Distribution.exponential(0, 9999)),
            Distribution.weighted(0, 6, 1, 2, 10, 20, 100, 1000, 10000)
        };

        PropertyChecker.forAll(Generators.int64())
                       .withRuns(50)
                       .withSeed(42L)
                       .check(seed -> {
                           for (Distribution dist : dists)
                           {
                               EntropySource rng1 = new JdkRandomEntropySource(seed);
                               EntropySource rng2 = new JdkRandomEntropySource(seed);
                               for (int i = 0; i < 10_000; i++)
                                   assertEquals("Determinism violated for " + dist.getClass().getSimpleName()
                                                + " at seed=" + seed,
                                                dist.next(rng1), dist.next(rng2));
                           }
                       });
    }

    // -- Boundary conditions --

    @Test
    public void testMinEqualsMax() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64(), Generators.int64())
                       .withRuns(50)
                       .withSeed(42L)
                       .check((value, seed) -> {
                           long v = value % 100_000; // keep it reasonable for fixed()
                           Distribution[] dists = {
                               Distribution.uniform(v, v),
                               Distribution.gaussian(v, v),
                               Distribution.gaussian(v, v, 3),
                               Distribution.exponential(v, v),
                               Distribution.extreme(v, v, 0.5),
                               Distribution.sequential(v, v),
                           };

                           EntropySource rng = new JdkRandomEntropySource(seed);
                           for (Distribution dist : dists)
                               for (int i = 0; i < 100; i++)
                                   assertEquals("min==max must return min for " + dist.getClass().getSimpleName(),
                                                v, dist.next(rng));
                       });
    }

    @Test
    public void testSmallRange() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(30)
                       .withSeed(42L)
                       .check(seed -> {
                           Distribution[] dists = {
                               Distribution.uniform(0, 1),
                               Distribution.gaussian(0, 1),
                               Distribution.exponential(0, 1),
                               Distribution.extreme(0, 1, 2),
                               Distribution.hotspot(0, 1, 0.5, 0.8),
                           };

                           EntropySource rng = new JdkRandomEntropySource(seed);
                           for (Distribution dist : dists)
                           {
                               boolean saw0 = false, saw1 = false;
                               for (int i = 0; i < 1000; i++)
                               {
                                   long val = dist.next(rng);
                                   assertTrue(val >= 0 && val <= 1);
                                   if (val == 0) saw0 = true;
                                   if (val == 1) saw1 = true;
                               }
                               assertTrue("Should produce both values for " + dist.getClass().getSimpleName(),
                                          saw0 && saw1);
                           }
                       });
    }

    @Test
    public void testZipfianExponentZeroIsUniform() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> assertDistribution(Distribution.zipfian(0, 9999, 0), SAMPLE_COUNT, seed,
                                                         meanInRange(0.49, 0.51),
                                                         massInRange(0.0, 0.5, 0.49, 0.51)));
    }

    @Test
    public void testHotspotDegenerateCases() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .withSeed(42L)
                       .check(seed -> {
                           // hotWeight = 1: all in hot zone
                           assertDistribution(Distribution.hotspot(0, 9999, 0.2, 1.0), SAMPLE_COUNT, seed,
                                              massInRange(0.0, 0.2, 0.99, 1.01));

                           // hotWeight = 0: all in cold zone
                           assertDistribution(Distribution.hotspot(0, 9999, 0.2, 0.0), SAMPLE_COUNT, seed,
                                              massInRange(0.2, 1.0, 0.99, 1.01));
                       });
    }
}
