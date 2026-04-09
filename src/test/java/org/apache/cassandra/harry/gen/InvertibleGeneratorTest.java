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
import java.util.Collection;
import java.util.Comparator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Behavioural tests for InvertibleGenerator with MidpointCache, exercising both the
 * full-coverage path (population ≤ 1024) and the partial-coverage BFS path (population > 1024).
 */
@RunWith(Parameterized.class)
public class InvertibleGeneratorTest<T>
{
    // Exercises partial MidpointCache coverage (BFS path, population > 1024)
    private static final int LARGE_POPULATION = 100_000;
    // Exercises full MidpointCache coverage (population ≤ 1024)
    private static final int SMALL_POPULATION = 500;
    private static final long SEED = 42L;

    private final Generator<T> gen;
    private final Comparator<T> comparator;

    @SuppressWarnings("unchecked")
    @Parameters(name = "{0}")
    public static Collection<Object[]> params()
    {
        Comparator<Object[]> tupleComparator = (a, b) -> {
            int c = ((String) a[0]).compareTo((String) b[0]);
            if (c != 0) return c;
            return Long.compare((Long) a[1], (Long) b[1]);
        };
        return Arrays.asList(
            new Object[]{ "long",   (Generator<Long>)     EntropySource::next,                                             (Comparator<Long>)     Long::compare             },
            new Object[]{ "string", (Generator<String>)   Generators.ascii(1, 10),                                         (Comparator<String>)   Comparator.naturalOrder() },
            new Object[]{ "tuple",  (Generator<Object[]>) Generators.zipArray(Generators.ascii(1, 6), Generators.int64()), (Comparator<Object[]>) tupleComparator           }
        );
    }

    public InvertibleGeneratorTest(String name, Generator<T> gen, Comparator<T> comparator)
    {
        this.gen = gen;
        this.comparator = comparator;
    }

    // --- helpers ---

    private InvertibleGenerator<T> make(long seed, int population)
    {
        return new InvertibleGenerator<>(new JdkRandomEntropySource(seed), Long.MAX_VALUE, population, gen, comparator);
    }

    private InvertibleGenerator<T> make(long seed, int population, long typeEntropy)
    {
        return new InvertibleGenerator<>(new JdkRandomEntropySource(seed), typeEntropy, population, gen, comparator);
    }

    // --- tests: partial MidpointCache coverage (population > 1024) ---

    @Test
    public void testInflateDeflateRoundtripLargePopulation()
    {
        InvertibleGenerator<T> gen = make(SEED, LARGE_POPULATION);
        long pop = gen.population();
        assertTrue("population should be > 0", pop > 0);
        assertTrue("population should be <= requested", pop <= LARGE_POPULATION);

        for (int i = 0; i < pop; i += 100)
        {
            long descriptor = gen.descriptorAt(i);
            T value = gen.inflate(descriptor);
            assertEquals("deflate(inflate(d)) should equal d for idx=" + i, descriptor, gen.deflate(value));
        }
    }

    @Test
    public void testIdxForAndDescriptorAtLargePopulation()
    {
        InvertibleGenerator<T> gen = make(SEED, LARGE_POPULATION);
        for (int i = 0; i < gen.population(); i += 50)
        {
            long descriptor = gen.descriptorAt(i);
            assertEquals("idxFor(descriptorAt(i)) should equal i", (long) i, gen.idxFor(descriptor));
        }
    }

    @Test
    public void testSortedOrderLargePopulation()
    {
        InvertibleGenerator<T> gen = make(SEED, LARGE_POPULATION);
        T prev = null;
        for (int i = 0; i < gen.population(); i += 10)
        {
            T curr = gen.inflate(gen.descriptorAt(i));
            if (prev != null)
                assertTrue("values should be strictly ascending at i=" + i, comparator.compare(curr, prev) > 0);
            prev = curr;
        }
    }

    // --- tests: full MidpointCache coverage (population ≤ 1024) ---

    @Test
    public void testInflateDeflateRoundtripSmallPopulation()
    {
        InvertibleGenerator<T> gen = make(SEED, SMALL_POPULATION);
        long pop = gen.population();
        for (int i = 0; i < pop; i++)
        {
            long descriptor = gen.descriptorAt(i);
            T value = gen.inflate(descriptor);
            assertEquals("deflate(inflate(d)) should equal d for idx=" + i, descriptor, gen.deflate(value));
        }
    }

    @Test
    public void testIdxForAndDescriptorAtSmallPopulation()
    {
        InvertibleGenerator<T> gen = make(SEED, SMALL_POPULATION);
        for (int i = 0; i < gen.population(); i++)
        {
            long descriptor = gen.descriptorAt(i);
            assertEquals("idxFor(descriptorAt(i)) should equal i", (long) i, gen.idxFor(descriptor));
        }
    }

    @Test
    public void testSortedOrderSmallPopulation()
    {
        InvertibleGenerator<T> gen = make(SEED, SMALL_POPULATION);
        T prev = null;
        for (int i = 0; i < gen.population(); i++)
        {
            T curr = gen.inflate(gen.descriptorAt(i));
            if (prev != null)
                assertTrue("values should be strictly ascending at i=" + i, comparator.compare(curr, prev) > 0);
            prev = curr;
        }
    }

    // --- edge cases ---

    @Test
    public void testSingleElementPopulation()
    {
        InvertibleGenerator<T> gen = make(SEED, 1);
        assertEquals("population should be 1", 1L, gen.population());
        long descriptor = gen.descriptorAt(0);
        T value = gen.inflate(descriptor);
        assertEquals("deflate(inflate(d)) should equal d for single element", descriptor, gen.deflate(value));
        assertEquals("idxFor(descriptorAt(0)) should be 0", 0L, gen.idxFor(descriptor));
    }

    @Test
    public void testFirstAndLastElementRoundtrip()
    {
        InvertibleGenerator<T> gen = make(SEED, 200);
        long pop = gen.population();

        long firstDesc = gen.descriptorAt(0);
        assertEquals("idxFor(descriptorAt(0)) == 0", 0L, gen.idxFor(firstDesc));
        assertEquals("deflate(inflate(first)) == first", firstDesc, gen.deflate(gen.inflate(firstDesc)));

        long lastDesc = gen.descriptorAt(pop - 1);
        assertEquals("idxFor(descriptorAt(pop-1)) == pop-1", pop - 1, gen.idxFor(lastDesc));
        assertEquals("deflate(inflate(last)) == last", lastDesc, gen.deflate(gen.inflate(lastDesc)));
    }

    @Test
    public void testTypeEntropyCapsBelowPopulation()
    {
        long typeEntropy = 10L;
        InvertibleGenerator<T> gen = make(SEED, 1000, typeEntropy);
        assertTrue("population should be <= typeEntropy", gen.population() <= typeEntropy);
        assertTrue("population should be > 0", gen.population() > 0);
        for (int i = 0; i < gen.population(); i++)
        {
            long descriptor = gen.descriptorAt(i);
            assertEquals("deflate(inflate(d)) should equal d at idx=" + i, descriptor, gen.deflate(gen.inflate(descriptor)));
        }
    }

    @Test
    public void testDifferentSeedsProduceDifferentData()
    {
        InvertibleGenerator<T> gen1 = make(42L, 100);
        InvertibleGenerator<T> gen2 = make(99L, 100);
        assertNotEquals("different seeds should produce different descriptors at idx 0",
                        gen1.descriptorAt(0), gen2.descriptorAt(0));
    }

    @Test
    public void testPopulationAtCacheBoundary()
    {
        // Population exactly at cache capacity (1024) — exercises the boundary between full and partial coverage
        for (int pop : new int[]{ 1023, 1024, 1025, 2048 })
        {
            InvertibleGenerator<T> gen = make(SEED, pop);
            assertTrue("population should be > 0 for requested=" + pop, gen.population() > 0);

            long firstDesc = gen.descriptorAt(0);
            assertEquals("first roundtrip for pop=" + pop, firstDesc, gen.deflate(gen.inflate(firstDesc)));
            assertEquals("idxFor first for pop=" + pop, 0L, gen.idxFor(firstDesc));

            long lastDesc = gen.descriptorAt(gen.population() - 1);
            assertEquals("last roundtrip for pop=" + pop, lastDesc, gen.deflate(gen.inflate(lastDesc)));
            assertEquals("idxFor last for pop=" + pop, gen.population() - 1, gen.idxFor(lastDesc));
        }
    }
}
