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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Maps uniform entropy from an {@link EntropySource} into values drawn according
 * to a probability distribution over a configured {@code [min, max]} range.
 */
public abstract class Distribution
{
    protected final long min;
    protected final long max;

    protected Distribution(long min, long max)
    {
        assert min <= max : "min (" + min + ") must be <= max (" + max + ")";
        this.min = min;
        this.max = max;
    }

    /** Sample the next long value in [min, max]. */
    public abstract long next(EntropySource rng);

    /** Sample the next double value in [min, max]. */
    public abstract double nextDouble(EntropySource rng);

    public long min() { return min; }
    public long max() { return max; }

    // -- Built-in distributions --

    public static Distribution uniform(long min, long max)
    {
        return new Uniform(min, max);
    }

    public static Distribution fixed(long value)
    {
        return new Fixed(value);
    }

    public static Distribution gaussian(long min, long max)
    {
        double mean = (min + max) / 2.0;
        double stdev = (max - min) / 6.0;
        return new Gaussian(min, max, mean, stdev);
    }

    public static Distribution gaussian(long min, long max, double stddevRanges)
    {
        double mean = (min + max) / 2.0;
        double stdev = ((max - min) / 2.0) / stddevRanges;
        return new Gaussian(min, max, mean, stdev);
    }

    public static Distribution gaussian(long min, long max, double mean, double stdev)
    {
        return new Gaussian(min, max, mean, stdev);
    }

    public static Distribution exponential(long min, long max)
    {
        return new Exponential(min, max);
    }

    public static Distribution extreme(long min, long max, double shape)
    {
        return new Extreme(min, max, shape);
    }

    public static Distribution quantizedExtreme(long min, long max, double shape, int buckets)
    {
        return new QuantizedExtreme(min, max, shape, buckets);
    }

    public static Distribution zipfian(long min, long max, double exponent)
    {
        return new Zipfian(min, max, exponent);
    }

    public static Distribution hotspot(long min, long max, double hotFraction, double hotWeight)
    {
        return new Hotspot(min, max, hotFraction, hotWeight);
    }

    public static Distribution sequential(long min, long max)
    {
        return new Sequential(min, max);
    }

    /**
     * A distribution where each value's probability is proportional to its weight.
     * The weights array defines relative probabilities for bins spread across [min, max].
     * For example, {@code weighted(0, 6, 1, 2, 10, 20, 100, 1000, 10000)} returns 6 about
     * 10,000x more often than 0.
     *
     * If the range is larger than the number of weights, the range is divided into
     * equal-sized bins and values within each bin are selected uniformly.
     */
    public static Distribution weighted(long min, long max, double... weights)
    {
        return new Weighted(min, max, weights);
    }

    public static Distribution invert(Distribution delegate)
    {
        if (delegate instanceof Inverted)
            return ((Inverted) delegate).delegate;
        return new Inverted(delegate);
    }

    // -- Implementations --

    private static long clamp(long value, long min, long max)
    {
        return Math.max(min, Math.min(max, value));
    }

    private static double clampDouble(double value, double min, double max)
    {
        return Math.max(min, Math.min(max, value));
    }

    private static class Uniform extends Distribution
    {
        Uniform(long min, long max)
        {
            super(min, max);
        }

        @Override
        public long next(EntropySource rng)
        {
            if (min == max) return min;
            return rng.nextLong(min, max + 1);
        }

        @Override
        public double nextDouble(EntropySource rng)
        {
            if (min == max) return min;
            return min + rng.nextDouble() * (max - min);
        }
    }

    private static class Fixed extends Distribution
    {
        private final long value;

        Fixed(long value)
        {
            super(value, value);
            this.value = value;
        }

        @Override
        public long next(EntropySource rng)
        {
            return value;
        }

        @Override
        public double nextDouble(EntropySource rng)
        {
            return (double) value;
        }
    }

    private static class Gaussian extends Distribution
    {
        private final double mean;
        private final double stdev;

        Gaussian(long min, long max, double mean, double stdev)
        {
            super(min, max);
            this.mean = mean;
            this.stdev = stdev;
        }

        @Override
        public long next(EntropySource rng)
        {
            if (min == max) return min;
            return clamp(Math.round(nextGaussian(rng)), min, max);
        }

        @Override
        public double nextDouble(EntropySource rng)
        {
            if (min == max) return min;
            return clampDouble(nextGaussian(rng), min, max);
        }

        private double nextGaussian(EntropySource rng)
        {
            // Box-Muller transform
            double u1 = rng.nextDouble();
            double u2 = rng.nextDouble();
            // Avoid log(0)
            if (u1 < 1e-15) u1 = 1e-15;
            double z = Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
            return mean + z * stdev;
        }
    }

    private static class Exponential extends Distribution
    {
        private final double lambda;

        Exponential(long min, long max)
        {
            super(min, max);
            long range = max - min;
            if (range <= 1)
            {
                this.lambda = 1.0;
            }
            else
            {
                double epsilon = 1.0 / range;
                this.lambda = -Math.log(epsilon) / range;
            }
        }

        @Override
        public long next(EntropySource rng)
        {
            if (min == max) return min;
            return clamp(min + Math.round(sample(rng)), min, max);
        }

        @Override
        public double nextDouble(EntropySource rng)
        {
            if (min == max) return min;
            return clampDouble(min + sample(rng), min, max);
        }

        private double sample(EntropySource rng)
        {
            double u = rng.nextDouble();
            if (u >= 1.0) u = 1.0 - 1e-15;
            return -Math.log(1.0 - u) / lambda;
        }
    }

    private static class Extreme extends Distribution
    {
        protected final double shape;
        protected final double scale;

        Extreme(long min, long max, double shape)
        {
            super(min, max);
            this.shape = shape;
            long range = max - min;
            if (range <= 1)
            {
                this.scale = 1.0;
            }
            else
            {
                double epsilon = 1.0 / range;
                this.scale = range / Math.pow(-Math.log(epsilon), 1.0 / shape);
            }
        }

        @Override
        public long next(EntropySource rng)
        {
            if (min == max) return min;
            return clamp(min + Math.round(sample(rng)), min, max);
        }

        @Override
        public double nextDouble(EntropySource rng)
        {
            if (min == max) return min;
            return clampDouble(min + sample(rng), min, max);
        }

        protected double sample(EntropySource rng)
        {
            double u = rng.nextDouble();
            if (u >= 1.0) u = 1.0 - 1e-15;
            return scale * Math.pow(-Math.log(1.0 - u), 1.0 / shape);
        }
    }

    private static class QuantizedExtreme extends Distribution
    {
        private final double shape;
        private final double scale;
        private final int buckets;

        QuantizedExtreme(long min, long max, double shape, int buckets)
        {
            super(min, max);
            this.shape = shape;
            this.buckets = buckets;
            long range = max - min;
            if (range <= 1)
            {
                this.scale = 1.0;
            }
            else
            {
                double epsilon = 1.0 / range;
                this.scale = range / Math.pow(-Math.log(epsilon), 1.0 / shape);
            }
        }

        @Override
        public long next(EntropySource rng)
        {
            if (min == max) return min;
            long range = max - min;

            // Weibull draw to select bucket
            double u = rng.nextDouble();
            if (u >= 1.0) u = 1.0 - 1e-15;
            double raw = scale * Math.pow(-Math.log(1.0 - u), 1.0 / shape);
            int bucketIdx = (int) clamp((long) Math.floor(raw * buckets / range), 0, buckets - 1);

            // Uniform within bucket
            long lo = min + range * bucketIdx / buckets;
            long hi = min + range * (bucketIdx + 1) / buckets;
            return rng.nextLong(lo, hi + 1);
        }

        @Override
        public double nextDouble(EntropySource rng)
        {
            return (double) next(rng);
        }
    }

    private static class Zipfian extends Distribution
    {
        private final double exponent;
        private final long n;
        private final double[] cdf;

        Zipfian(long min, long max, double exponent)
        {
            super(min, max);
            this.exponent = exponent;
            this.n = max - min + 1;
            // Precompute CDF for binary search sampling.
            // For very large ranges, use a sampled CDF with interpolation.
            int cdfSize = (int) Math.min(n, 100_000);
            this.cdf = new double[cdfSize];
            double cumulative = 0;
            if (n <= 100_000)
            {
                for (int i = 0; i < cdfSize; i++)
                {
                    cumulative += 1.0 / Math.pow(i + 1, exponent);
                    cdf[i] = cumulative;
                }
            }
            else
            {
                // Sample: map cdf[i] to rank (i * n / cdfSize)
                double prev = 0;
                for (int i = 0; i < cdfSize; i++)
                {
                    long rank = (long) i * n / cdfSize;
                    long nextRank = (long) (i + 1) * n / cdfSize;
                    // Sum from rank+1 to nextRank
                    for (long r = rank + 1; r <= nextRank && r <= n; r++)
                        cumulative += 1.0 / Math.pow(r, exponent);
                    cdf[i] = cumulative;
                }
            }
            // Normalize to [0, 1]
            double total = cdf[cdfSize - 1];
            for (int i = 0; i < cdfSize; i++)
                cdf[i] /= total;
        }

        @Override
        public long next(EntropySource rng)
        {
            if (min == max) return min;
            if (exponent == 0) return rng.nextLong(min, max + 1);

            double u = rng.nextDouble();
            int idx = java.util.Arrays.binarySearch(cdf, u);
            if (idx < 0) idx = -idx - 1;
            idx = Math.min(idx, cdf.length - 1);

            if (n <= 100_000)
                return min + idx;

            // Map back from sampled index to actual rank
            return min + clamp((long) idx * n / cdf.length, 0, n - 1);
        }

        @Override
        public double nextDouble(EntropySource rng)
        {
            return (double) next(rng);
        }
    }

    private static class Hotspot extends Distribution
    {
        private final double hotWeight;
        private final long hotSize;

        Hotspot(long min, long max, double hotFraction, double hotWeight)
        {
            super(min, max);
            assert hotFraction > 0 && hotFraction < 1 : "hotFraction must be in (0, 1)";
            assert hotWeight >= 0 && hotWeight <= 1 : "hotWeight must be in [0, 1]";
            long range = max - min + 1;
            this.hotSize = (long) Math.ceil(range * hotFraction);
            this.hotWeight = hotWeight;
        }

        @Override
        public long next(EntropySource rng)
        {
            if (min == max) return min;
            long range = max - min + 1;
            if (rng.nextDouble() < hotWeight)
                return min + rng.nextLong(0, hotSize);
            else
                return min + hotSize + rng.nextLong(0, range - hotSize);
        }

        @Override
        public double nextDouble(EntropySource rng)
        {
            return (double) next(rng);
        }
    }

    private static class Sequential extends Distribution
    {
        private final AtomicLong counter = new AtomicLong(0);

        Sequential(long min, long max)
        {
            super(min, max);
        }

        @Override
        public long next(EntropySource rng)
        {
            long range = max - min + 1;
            return min + (counter.getAndIncrement() % range);
        }

        @Override
        public double nextDouble(EntropySource rng)
        {
            return (double) next(rng);
        }
    }

    private static class Weighted extends Distribution
    {
        private final double[] cdf;
        private final int numWeights;

        Weighted(long min, long max, double... weights)
        {
            super(min, max);
            assert weights.length > 0 : "weights must not be empty";
            for (double w : weights)
                assert w >= 0 : "weights must be non-negative";
            this.numWeights = weights.length;
            this.cdf = new double[weights.length];
            double cumulative = 0;
            for (int i = 0; i < weights.length; i++)
            {
                cumulative += weights[i];
                cdf[i] = cumulative;
            }
            assert cumulative > 0 : "total weight must be positive";
            // Normalize
            for (int i = 0; i < cdf.length; i++)
                cdf[i] /= cumulative;
        }

        @Override
        public long next(EntropySource rng)
        {
            if (min == max) return min;
            double u = rng.nextDouble();
            int idx = java.util.Arrays.binarySearch(cdf, u);
            if (idx < 0) idx = -idx - 1;
            idx = Math.min(idx, numWeights - 1);

            long range = max - min + 1;
            if (numWeights == range)
                return min + idx;

            // Map weight index to a bin in the range, then pick uniformly within bin
            long binStart = min + range * idx / numWeights;
            long binEnd = min + range * (idx + 1) / numWeights;
            if (binEnd > max + 1) binEnd = max + 1;
            return rng.nextLong(binStart, binEnd);
        }

        @Override
        public double nextDouble(EntropySource rng)
        {
            return (double) next(rng);
        }
    }

    private static class Inverted extends Distribution
    {
        private final Distribution delegate;

        Inverted(Distribution delegate)
        {
            super(delegate.min, delegate.max);
            this.delegate = delegate;
        }

        @Override
        public long next(EntropySource rng)
        {
            return max - (delegate.next(rng) - min);
        }

        @Override
        public double nextDouble(EntropySource rng)
        {
            return max - (delegate.nextDouble(rng) - min);
        }
    }
}
