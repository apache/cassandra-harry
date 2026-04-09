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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;

import java.io.UncheckedIOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class DiskBackedInvertibleGeneratorTest<T>
{
    private static final int POPULATION = 1_000_000;
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
            new Object[]{ "long",   (Generator<Long>)     EntropySource::next,                                             (Comparator<Long>)     Long::compare              },
            new Object[]{ "string", (Generator<String>)   Generators.ascii(1, 10),                                         (Comparator<String>)   Comparator.naturalOrder()  },
            new Object[]{ "tuple",  (Generator<Object[]>) Generators.zipArray(Generators.ascii(1, 6), Generators.int64()), (Comparator<Object[]>) tupleComparator            }
        );
    }

    public DiskBackedInvertibleGeneratorTest(String name, Generator<T> gen, Comparator<T> comparator)
    {
        this.gen = gen;
        this.comparator = comparator;
    }

    @Test
    public void testInflateDeflateRoundtrip() throws Exception
    {
        Path tmpDir = Files.createTempDirectory("harry-test-");
        try (DiskBackedInvertibleGenerator<T> disk = DiskBackedInvertibleGenerator.open(tmpDir, SEED, POPULATION, 64, gen, comparator, Long.MAX_VALUE))
        {
            long pop = disk.population();
            assertTrue("population should be > 0", pop > 0);
            assertTrue("population should be <= requested", pop <= POPULATION);

            for (int i = 0; i < pop; i += 100)
            {
                long descriptor = disk.descriptorAt(i);
                T value = disk.inflate(descriptor);
                assertEquals("deflate(inflate(d)) should equal d for idx=" + i, descriptor, disk.deflate(value));
            }
        }
    }

    @Test
    public void testIdxForAndDescriptorAt() throws Exception
    {
        Path tmpDir = Files.createTempDirectory("harry-test-");
        try (DiskBackedInvertibleGenerator<T> disk = DiskBackedInvertibleGenerator.open(tmpDir, SEED, POPULATION, 64, gen, comparator, Long.MAX_VALUE))
        {
            for (int i = 0; i < disk.population(); i += 50)
            {
                long descriptor = disk.descriptorAt(i);
                assertEquals("idxFor(descriptorAt(i)) should equal i", (long) i, disk.idxFor(descriptor));
            }
        }
    }

    @Test
    public void testSortedOrder() throws Exception
    {
        Path tmpDir = Files.createTempDirectory("harry-test-");
        try (DiskBackedInvertibleGenerator<T> disk = DiskBackedInvertibleGenerator.open(tmpDir, SEED, POPULATION, 64, gen, comparator, Long.MAX_VALUE))
        {
            T prev = null;
            for (int i = 0; i < disk.population(); i++)
            {
                T curr = disk.inflate(disk.descriptorAt(i));
                if (prev != null)
                    assertTrue("values should be strictly ascending at i=" + i, comparator.compare(curr, prev) > 0);
                prev = curr;
            }
        }
    }

    @Test
    public void testFileReuse() throws Exception
    {
        Path tmpDir = Files.createTempDirectory("harry-test-");
        long descriptor0First, descriptor0Second;

        try (DiskBackedInvertibleGenerator<T> disk = DiskBackedInvertibleGenerator.open(tmpDir, SEED, POPULATION, 64, gen, comparator, Long.MAX_VALUE))
        {
            descriptor0First = disk.descriptorAt(0);
        }

        try (DiskBackedInvertibleGenerator<T> disk = DiskBackedInvertibleGenerator.open(tmpDir, SEED, POPULATION, 64, gen, comparator, Long.MAX_VALUE))
        {
            descriptor0Second = disk.descriptorAt(0);
        }

        assertEquals("descriptor at 0 should be the same across opens", descriptor0First, descriptor0Second);
    }

    @Test
    public void testSingleElementPopulation() throws Exception
    {
        Path tmpDir = Files.createTempDirectory("harry-test-");
        try (DiskBackedInvertibleGenerator<T> disk = DiskBackedInvertibleGenerator.open(tmpDir, SEED, 1, 64, gen, comparator, Long.MAX_VALUE))
        {
            assertEquals("population should be 1", 1L, disk.population());
            long descriptor = disk.descriptorAt(0);
            T value = disk.inflate(descriptor);
            assertEquals("deflate(inflate(d)) should equal d for single element", descriptor, disk.deflate(value));
            assertEquals("idxFor(descriptorAt(0)) should be 0", 0L, disk.idxFor(descriptor));
        }
    }

    @Test
    public void testFirstAndLastElementRoundtrip() throws Exception
    {
        Path tmpDir = Files.createTempDirectory("harry-test-");
        try (DiskBackedInvertibleGenerator<T> disk = DiskBackedInvertibleGenerator.open(tmpDir, SEED, 200, 64, gen, comparator, Long.MAX_VALUE))
        {
            long pop = disk.population();
            long firstDesc = disk.descriptorAt(0);
            assertEquals("idxFor(descriptorAt(0)) == 0", 0L, disk.idxFor(firstDesc));
            assertEquals("deflate(inflate(first)) == first", firstDesc, disk.deflate(disk.inflate(firstDesc)));

            long lastDesc = disk.descriptorAt(pop - 1);
            assertEquals("idxFor(descriptorAt(pop-1)) == pop-1", pop - 1, disk.idxFor(lastDesc));
            assertEquals("deflate(inflate(last)) == last", lastDesc, disk.deflate(disk.inflate(lastDesc)));
        }
    }

    @Test
    public void testTypeEntropyCapsBelowPopulation() throws Exception
    {
        long typeEntropy = 10L;
        Path tmpDir = Files.createTempDirectory("harry-test-");
        try (DiskBackedInvertibleGenerator<T> disk = DiskBackedInvertibleGenerator.open(tmpDir, SEED, 1000, 64, gen, comparator, typeEntropy))
        {
            assertTrue("population should be <= typeEntropy", disk.population() <= typeEntropy);
            assertTrue("population should be > 0", disk.population() > 0);
            for (int i = 0; i < disk.population(); i++)
            {
                long descriptor = disk.descriptorAt(i);
                assertEquals("deflate(inflate(d)) should equal d at idx=" + i, descriptor, disk.deflate(disk.inflate(descriptor)));
            }
        }
    }

    @Test
    public void testStrideLargerThanPopulation() throws Exception
    {
        Path tmpDir = Files.createTempDirectory("harry-test-");
        try (DiskBackedInvertibleGenerator<T> disk = DiskBackedInvertibleGenerator.open(tmpDir, SEED, 50, 1024, gen, comparator, Long.MAX_VALUE))
        {
            long pop = disk.population();
            assertTrue("population should be > 0", pop > 0);
            for (int i = 0; i < pop; i++)
            {
                long descriptor = disk.descriptorAt(i);
                assertEquals("idxFor(descriptorAt(i)) == i for stride>pop", (long) i, disk.idxFor(descriptor));
            }
        }
    }

    @Test
    public void testStrideOfOne() throws Exception
    {
        Path tmpDir = Files.createTempDirectory("harry-test-");
        try (DiskBackedInvertibleGenerator<T> disk = DiskBackedInvertibleGenerator.open(tmpDir, SEED, 100, 1, gen, comparator, Long.MAX_VALUE))
        {
            long pop = disk.population();
            for (int i = 0; i < pop; i += 10)
            {
                long descriptor = disk.descriptorAt(i);
                assertEquals("idxFor(descriptorAt(i)) == i with stride=1", (long) i, disk.idxFor(descriptor));
            }
        }
    }

    @Test
    public void testPopulationAtStrideBoundaries() throws Exception
    {
        int stride = 64;
        for (int pop : new int[]{ stride - 1, stride, stride + 1, stride * 2 })
        {
            Path tmpDir = Files.createTempDirectory("harry-test-stride-" + pop + "-");
            try (DiskBackedInvertibleGenerator<T> disk = DiskBackedInvertibleGenerator.open(tmpDir, SEED, pop, stride, gen, comparator, Long.MAX_VALUE))
            {
                long actualPop = disk.population();
                assertTrue("population should be > 0 for requested=" + pop, actualPop > 0);

                long firstDesc = disk.descriptorAt(0);
                assertEquals("first roundtrip for pop=" + pop, firstDesc, disk.deflate(disk.inflate(firstDesc)));
                assertEquals("idxFor first for pop=" + pop, 0L, disk.idxFor(firstDesc));

                long lastDesc = disk.descriptorAt(actualPop - 1);
                assertEquals("last roundtrip for pop=" + pop, lastDesc, disk.deflate(disk.inflate(lastDesc)));
                assertEquals("idxFor last for pop=" + pop, actualPop - 1, disk.idxFor(lastDesc));
            }
        }
    }

    @Test
    public void testUseAfterClose() throws Exception
    {
        Path tmpDir = Files.createTempDirectory("harry-test-");
        DiskBackedInvertibleGenerator<T> disk = DiskBackedInvertibleGenerator.open(tmpDir, SEED, 100, 64, gen, comparator, Long.MAX_VALUE);
        disk.close();
        try
        {
            disk.descriptorAt(0);
            fail("Expected UncheckedIOException after close");
        }
        catch (UncheckedIOException expected)
        {
            // expected: channel is closed
        }
    }

    @Test
    public void testDifferentSeedsProduceDifferentData() throws Exception
    {
        Path tmpDir = Files.createTempDirectory("harry-test-");
        long desc1, desc2;
        try (DiskBackedInvertibleGenerator<T> disk = DiskBackedInvertibleGenerator.open(tmpDir, 42L, 100, 64, gen, comparator, Long.MAX_VALUE))
        {
            desc1 = disk.descriptorAt(0);
        }
        try (DiskBackedInvertibleGenerator<T> disk = DiskBackedInvertibleGenerator.open(tmpDir, 99L, 100, 64, gen, comparator, Long.MAX_VALUE))
        {
            desc2 = disk.descriptorAt(0);
        }
        assertNotEquals("different seeds should produce different descriptors at idx 0", desc1, desc2);
    }

    @Test
    public void testMatchesInvertibleGenerator() throws Exception
    {
        int smallPop = 500;
        Path tmpDir = Files.createTempDirectory("harry-test-");

        InvertibleGenerator<T> mem = new InvertibleGenerator<>(new JdkRandomEntropySource(SEED), Long.MAX_VALUE, smallPop, gen, comparator);

        try (DiskBackedInvertibleGenerator<T> disk = DiskBackedInvertibleGenerator.open(tmpDir, SEED, smallPop, 32, gen, comparator, Long.MAX_VALUE))
        {
            assertEquals("population should match", mem.population(), disk.population());
            for (int i = 0; i < mem.population(); i++)
            {
                T memVal  = mem.inflate(mem.descriptorAt(i));
                T diskVal = disk.inflate(disk.descriptorAt(i));
                assertEquals("value at idx=" + i + " should match", 0, comparator.compare(memVal, diskVal));
            }
        }
    }
}
