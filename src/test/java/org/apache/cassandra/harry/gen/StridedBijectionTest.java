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
import org.apache.cassandra.harry.gen.Bijections.IndexedBijection;

import static org.apache.cassandra.harry.checker.Properties.roundtrip;

public class StridedBijectionTest
{
    private static final Generator<Integer> BYTE_SIZES = Generators.pick(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));

    private static int populationForByteSize(int byteSize)
    {
        return byteSize == 1 ? 100 : 10_000;
    }

    // -----------------------------------------------------------------------
    // StridedBijection (Bijection<Long>) properties
    // -----------------------------------------------------------------------

    @Test
    public void strictlyOrdered() throws Throwable
    {
        PropertyChecker.forAll(BYTE_SIZES, Generators.int64())
                       .withRuns(100)
                       .check((byteSize, seed) -> {
                           int pop = populationForByteSize(byteSize);
                           StridedBijection bij = new StridedBijection(seed, pop, byteSize);
                           long prev = bij.inflate(0);
                           for (int d = 1; d < pop; d++)
                           {
                               long curr = bij.inflate(d);
                               Assert.assertTrue(
                                   String.format("byteSize=%d seed=%d: inflate(%d)=%d >= inflate(%d)=%d",
                                                 byteSize, seed, d - 1, prev, d, curr),
                                   prev < curr);
                               prev = curr;
                           }
                       });
    }

    @Test
    public void allUnique() throws Throwable
    {
        PropertyChecker.forAll(BYTE_SIZES, Generators.int64())
                       .withRuns(100)
                       .check((byteSize, seed) -> {
                           int pop = populationForByteSize(byteSize);
                           StridedBijection bij = new StridedBijection(seed, pop, byteSize);
                           Set<Long> seen = new HashSet<>(pop * 2);
                           for (int d = 0; d < pop; d++)
                               Assert.assertTrue("Duplicate at descriptor " + d,
                                                 seen.add(bij.inflate(d)));
                       });
    }

    @Test
    public void deflateRoundTrip() throws Throwable
    {
        PropertyChecker.forAll(BYTE_SIZES, Generators.int64())
                       .withRuns(100)
                       .check((byteSize, seed) -> {
                           int pop = populationForByteSize(byteSize);
                           StridedBijection bij = new StridedBijection(seed, pop, byteSize);
                           PropertyChecker.forAll(Generators.int64(0, pop - 1))
                                          .withRuns(pop)
                                          .check(roundtrip(bij::inflate, bij::deflate));
                       });
    }

    @Test
    public void differentSeedsProduceDifferentValues() throws Throwable
    {
        PropertyChecker.forAll(BYTE_SIZES, Generators.int64(), Generators.int64())
                       .withRuns(100)
                       .check((byteSize, seedA, seedB) -> {
                           if (seedA.equals(seedB)) return;
                           int pop = byteSize == 1 ? 50 : 1000;
                           StridedBijection a = new StridedBijection(seedA, pop, byteSize);
                           StridedBijection b = new StridedBijection(seedB, pop, byteSize);
                           int diffCount = 0;
                           for (int d = 0; d < pop; d++)
                           {
                               if (a.inflate(d) != b.inflate(d))
                                   diffCount++;
                           }
                           Assert.assertTrue(
                               String.format("byteSize=%d: only %d/%d values differ between seeds %d and %d",
                                             byteSize, diffCount, pop, seedA, seedB),
                               diffCount > pop / 2);
                       });
    }

    @Test
    public void compareConsistentWithOrder() throws Throwable
    {
        PropertyChecker.forAll(BYTE_SIZES, Generators.int64())
                       .withRuns(100)
                       .check((byteSize, seed) -> {
                           int pop = populationForByteSize(byteSize);
                           StridedBijection bij = new StridedBijection(seed, pop, byteSize);
                           for (int d = 0; d < Math.min(pop - 1, 200); d++)
                           {
                               Assert.assertTrue(bij.compare(d, d + 1) < 0);
                               Assert.assertTrue(bij.compare(d + 1, d) > 0);
                               Assert.assertEquals(0, bij.compare(d, d));
                           }
                       });
    }

    @Test
    public void valuesWithinTypeRange() throws Throwable
    {
        PropertyChecker.forAll(BYTE_SIZES, Generators.int64())
                       .withRuns(100)
                       .check((byteSize, seed) -> {
                           int pop = populationForByteSize(byteSize);
                           StridedBijection bij = new StridedBijection(seed, pop, byteSize);
                           long min = minForByteSize(byteSize);
                           long max = maxForByteSize(byteSize);
                           for (int d = 0; d < pop; d++)
                           {
                               long v = bij.inflate(d);
                               Assert.assertTrue(
                                   String.format("byteSize=%d: value %d out of [%d, %d]",
                                                 byteSize, v, min, max),
                                   v >= min && v <= max);
                           }
                       });
    }

    @Test
    public void downcastLossless() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(50)
                       .check(seed -> {
                           StridedBijection byteBij = new StridedBijection(seed, 50, 1);
                           for (int d = 0; d < 50; d++)
                           {
                               long v = byteBij.inflate(d);
                               Assert.assertEquals("byte downcast at " + d, v, (byte) v);
                           }

                           StridedBijection shortBij = new StridedBijection(seed, 500, 2);
                           for (int d = 0; d < 500; d++)
                           {
                               long v = shortBij.inflate(d);
                               Assert.assertEquals("short downcast at " + d, v, (short) v);
                           }

                           StridedBijection intBij = new StridedBijection(seed, 1000, 4);
                           for (int d = 0; d < 1000; d++)
                           {
                               long v = intBij.inflate(d);
                               Assert.assertEquals("int downcast at " + d, v, (int) v);
                           }
                       });
    }

    @Test
    public void populationReported() throws Throwable
    {
        PropertyChecker.forAll(BYTE_SIZES, Generators.int64(1, 10_000))
                       .withRuns(50)
                       .check((byteSize, popLong) -> {
                           int pop = Math.min(popLong.intValue(),
                                              byteSize == 1 ? 200 : 10_000);
                           if (pop <= 0) return;
                           StridedBijection bij = new StridedBijection(0, pop, byteSize);
                           Assert.assertEquals(pop, bij.population());
                       });
    }

    @Test(expected = IllegalArgumentException.class)
    public void zeroPopulationRejected()
    {
        new StridedBijection(0, 0, 8);
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidByteSizeRejected()
    {
        new StridedBijection(0, 100, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void populationTooLargeForByteSize()
    {
        new StridedBijection(0, 257, 1);
    }

    // -----------------------------------------------------------------------
    // toIndexed properties -- all bijection types
    // -----------------------------------------------------------------------

    // All bijections whose inflate/deflate forms a standard order-preserving
    // mapping over [0, 2^(byteSize*8)) or its signed equivalent.  Boolean is
    // excluded: its adjustEntropyDomain remaps to {1,2}, so bucket-stride
    // descriptors fall outside the valid domain.
    @SuppressWarnings("unchecked")
    private static final Generator<Bijections.Bijection<?>> ALL_BIJECTIONS = Generators.pick(Arrays.asList(Bijections.INT8_GENERATOR,
                                                                                                           Bijections.INT16_GENERATOR,
                                                                                                           Bijections.INT32_GENERATOR,
                                                                                                           Bijections.INT64_GENERATOR,
                                                                                                           Bijections.FLOAT_GENERATOR,
                                                                                                           Bijections.DOUBLE_GENERATOR,
                                                                                                           Bijections.UUID_GENERATOR,
                                                                                                           Bijections.TIME_UUID_GENERATOR,
                                                                                                           Bijections.TIMESTAMP_GENERATOR,
                                                                                                           Bijections.BLOB_GENERATOR,
                                                                                                           Bijections.TIME_GENERATOR,
                                                                                                           Bijections.VARINT_GENERATOR,
                                                                                                           Bijections.DECIMAL_GENERATOR,
                                                                                                           Bijections.INET_GENERATOR));

    private static int populationForBijection(Bijections.Bijection<?> bij)
    {
        if (bij == Bijections.INT8_GENERATOR) return 100;
        return 5000;
    }

    @Test
    public void toIndexedOrdered() throws Throwable
    {
        PropertyChecker.forAll(ALL_BIJECTIONS, Generators.int64())
                       .withRuns(200)
                       .check((inner, seed) -> {
                           int pop = populationForBijection(inner);
                           IndexedBijection<?> indexed = StridedBijection.toIndexed(inner, seed, pop);

                           for (int i = 1; i < pop; i++)
                           {
                               Assert.assertTrue(
                                   String.format("bij=%s: descriptor(%d) not < descriptor(%d)",
                                                 inner.getClass().getSimpleName(), i - 1, i),
                                   indexed.compare(indexed.descriptorAt(i - 1),
                                                   indexed.descriptorAt(i)) < 0);
                           }
                       });
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void toIndexedRoundTrip() throws Throwable
    {
        PropertyChecker.forAll(ALL_BIJECTIONS, Generators.int64())
                       .withRuns(200)
                       .check((inner, seed) -> {
                           int pop = populationForBijection(inner);
                           IndexedBijection indexed = StridedBijection.toIndexed(inner, seed, pop);

                           for (int i = 0; i < pop; i++)
                           {
                               long descriptor = indexed.descriptorAt(i);
                               Assert.assertEquals("idxFor at " + i, i, indexed.idxFor(descriptor));
                               Object value = indexed.inflate(descriptor);
                               Assert.assertEquals("deflate at " + i, descriptor, indexed.deflate(value));
                           }
                       });
    }

    @Test
    public void toIndexedDifferentSeeds() throws Throwable
    {
        // Use bijections with enough range that the PRF offset has room to differ.
        @SuppressWarnings("unchecked")
        Generator<Bijections.Bijection<?>> wideBijections = Generators.pick(Arrays.asList(Bijections.INT32_GENERATOR,
                                                                                          Bijections.INT64_GENERATOR,
                                                                                          Bijections.UUID_GENERATOR,
                                                                                          Bijections.TIMESTAMP_GENERATOR,
                                                                                          Bijections.BLOB_GENERATOR,
                                                                                          Bijections.VARINT_GENERATOR,
                                                                                          Bijections.DECIMAL_GENERATOR));

        PropertyChecker.forAll(wideBijections, Generators.int64(), Generators.int64())
                       .withRuns(100)
                       .check((inner, seedA, seedB) -> {
                           if (seedA.equals(seedB)) return;
                           int pop = 1000;
                           IndexedBijection<?> a = StridedBijection.toIndexed(inner, seedA, pop);
                           IndexedBijection<?> b = StridedBijection.toIndexed(inner, seedB, pop);
                           int diffCount = 0;
                           for (int i = 0; i < pop; i++)
                           {
                               if (a.descriptorAt(i) != b.descriptorAt(i))
                                   diffCount++;
                           }
                           Assert.assertTrue(
                               String.format("bij=%s: only %d/%d differ",
                                             inner.getClass().getSimpleName(), diffCount, pop),
                               diffCount > pop / 2);
                       });
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static long minForByteSize(int byteSize)
    {
        if (byteSize >= 8) return Long.MIN_VALUE;
        return -(1L << (byteSize * 8 - 1));
    }

    private static long maxForByteSize(int byteSize)
    {
        if (byteSize >= 8) return Long.MAX_VALUE;
        return (1L << (byteSize * 8 - 1)) - 1;
    }
}
