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

public class CompositePkBijectionTest
{
    // -----------------------------------------------------------------------
    // Fixed, hand-checked cases
    // -----------------------------------------------------------------------

    @Test
    public void twoColumnEnumeration()
    {
        IndexedBijection<?> col0 = StridedBijection.toIndexed(Bijections.INT32_GENERATOR, 42, 3);
        IndexedBijection<?> col1 = StridedBijection.toIndexed(Bijections.INT32_GENERATOR, 43, 4);
        CompositePkBijection cpk = new CompositePkBijection(col0, col1);

        Assert.assertEquals(12, cpk.totalPopulation());
        Assert.assertEquals(2, cpk.width());

        // Verify mixed-radix decomposition for all 12 entries
        for (int pk0 = 0; pk0 < 3; pk0++)
        {
            for (int pk1 = 0; pk1 < 4; pk1++)
            {
                long flat = pk0 * 4L + pk1;
                long[] indices = cpk.inflate(flat);
                Assert.assertEquals(pk0, indices[0]);
                Assert.assertEquals(pk1, indices[1]);
                Assert.assertEquals(flat, cpk.deflate(indices));
            }
        }
    }

    @Test
    public void singleColumnDegenerates()
    {
        IndexedBijection<?> col = StridedBijection.toIndexed(Bijections.INT64_GENERATOR, 7, 100);
        CompositePkBijection cpk = new CompositePkBijection(col);

        Assert.assertEquals(100, cpk.totalPopulation());

        for (int i = 0; i < 100; i++)
        {
            long[] indices = cpk.inflate(i);
            Assert.assertEquals(1, indices.length);
            Assert.assertEquals(i, indices[0]);
            Assert.assertEquals(i, cpk.deflate(indices));
        }
    }

    @Test
    public void populationOneColumn()
    {
        // One column has population 1 -- its index is always 0
        IndexedBijection<?> col0 = StridedBijection.toIndexed(Bijections.INT32_GENERATOR, 1, 5);
        IndexedBijection<?> col1 = StridedBijection.toIndexed(Bijections.INT32_GENERATOR, 2, 1);
        IndexedBijection<?> col2 = StridedBijection.toIndexed(Bijections.INT32_GENERATOR, 3, 4);
        CompositePkBijection cpk = new CompositePkBijection(col0, col1, col2);

        Assert.assertEquals(20, cpk.totalPopulation());

        for (long flat = 0; flat < 20; flat++)
        {
            long[] indices = cpk.inflate(flat);
            Assert.assertEquals(0, indices[1]); // population-1 column always 0
            Assert.assertEquals(flat, cpk.deflate(indices));
        }
    }

    // -----------------------------------------------------------------------
    // Round-trip: inflate then deflate == identity
    // -----------------------------------------------------------------------

    @Test
    public void roundTripIndices() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(200)
                       .check(seed -> {
                           CompositePkBijection cpk = randomComposite(seed);
                           long total = cpk.totalPopulation();
                           for (long flat = 0; flat < total; flat++)
                           {
                               long[] indices = cpk.inflate(flat);
                               Assert.assertEquals("round-trip failed at " + flat,
                                                   flat, cpk.deflate(indices));
                           }
                       });
    }

    // -----------------------------------------------------------------------
    // Round-trip through values: inflate to values, deflate back
    // -----------------------------------------------------------------------

    @Test
    public void roundTripValues() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(200)
                       .check(seed -> {
                           CompositePkBijection cpk = randomComposite(seed);
                           long total = cpk.totalPopulation();
                           for (long flat = 0; flat < total; flat++)
                           {
                               Object[] values = cpk.inflateValues(flat);
                               Assert.assertEquals("value round-trip failed at " + flat,
                                                   flat, cpk.deflateFromValues(values));
                           }
                       });
    }

    // -----------------------------------------------------------------------
    // Ordering: flat index order == lexicographic tuple order
    // -----------------------------------------------------------------------

    @Test
    public void lexicographicOrdering() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(200)
                       .check(seed -> {
                           CompositePkBijection cpk = randomComposite(seed);
                           long total = cpk.totalPopulation();
                           long[] prev = cpk.inflate(0);
                           for (long flat = 1; flat < total; flat++)
                           {
                               long[] curr = cpk.inflate(flat);
                               Assert.assertTrue(
                                   String.format("flat %d -> %s should sort after flat %d -> %s",
                                                 flat, Arrays.toString(curr),
                                                 flat - 1, Arrays.toString(prev)),
                                   compareLex(prev, curr) < 0);
                               prev = curr;
                           }
                       });
    }

    // -----------------------------------------------------------------------
    // Exhaustiveness: no collisions
    // -----------------------------------------------------------------------

    @Test
    public void allTuplesDistinct() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(200)
                       .check(seed -> {
                           CompositePkBijection cpk = randomComposite(seed);
                           long total = cpk.totalPopulation();
                           Set<String> seen = new HashSet<>((int) total * 2);
                           for (long flat = 0; flat < total; flat++)
                           {
                               long[] indices = cpk.inflate(flat);
                               Assert.assertTrue("duplicate tuple at flat " + flat,
                                                 seen.add(Arrays.toString(indices)));
                           }
                           Assert.assertEquals(total, seen.size());
                       });
    }

    // -----------------------------------------------------------------------
    // Boundary indices
    // -----------------------------------------------------------------------

    @Test
    public void boundaryIndices() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(200)
                       .check(seed -> {
                           CompositePkBijection cpk = randomComposite(seed);
                           long total = cpk.totalPopulation();

                           // flatIdx = 0 -> all per-column indices are 0
                           long[] first = cpk.inflate(0);
                           for (long idx : first)
                               Assert.assertEquals(0, idx);

                           // flatIdx = total - 1 -> each per-column index is max
                           long[] last = cpk.inflate(total - 1);
                           for (int i = 0; i < cpk.width(); i++)
                               Assert.assertEquals(cpk.column(i).population() - 1, last[i]);
                       });
    }

    // -----------------------------------------------------------------------
    // Mixed types: combine different bijection types
    // -----------------------------------------------------------------------

    @Test
    public void mixedTypes() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(100)
                       .check(seed -> {
                           IndexedBijection<?> intCol = StridedBijection.toIndexed(Bijections.INT32_GENERATOR, seed, 8);
                           IndexedBijection<?> longCol = StridedBijection.toIndexed(Bijections.INT64_GENERATOR, seed + 1, 6);
                           IndexedBijection<?> uuidCol = StridedBijection.toIndexed(Bijections.UUID_GENERATOR, seed + 2, 4);
                           CompositePkBijection cpk = new CompositePkBijection(intCol, longCol, uuidCol);

                           Assert.assertEquals(192, cpk.totalPopulation());

                           for (long flat = 0; flat < cpk.totalPopulation(); flat++)
                           {
                               Object[] values = cpk.inflateValues(flat);
                               Assert.assertEquals(flat, cpk.deflateFromValues(values));
                           }
                       });
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    @Test(expected = IllegalArgumentException.class)
    public void emptyColumnsRejected()
    {
        new CompositePkBijection();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Build a random composite bijection with 2-3 columns and small populations
     * so tests can exhaustively enumerate.
     */
    private static CompositePkBijection randomComposite(long seed)
    {
        // Use seed bits to pick column count and populations
        int nCols = 2 + (int) (Math.abs(seed) % 2); // 2 or 3
        IndexedBijection<?>[] cols = new IndexedBijection[nCols];
        for (int i = 0; i < nCols; i++)
        {
            // Populations between 2 and 8
            int pop = 2 + (int) (Math.abs(seed * 31 + i * 17) % 7);
            cols[i] = StridedBijection.toIndexed(Bijections.INT32_GENERATOR, seed + i, pop);
        }
        return new CompositePkBijection(cols);
    }

    private static int compareLex(long[] a, long[] b)
    {
        for (int i = 0; i < a.length; i++)
        {
            int cmp = Long.compare(a[i], b[i]);
            if (cmp != 0) return cmp;
        }
        return 0;
    }
}
