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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.harry.DataType;
import org.apache.cassandra.harry.checker.PropertyChecker;
import org.apache.cassandra.harry.dml.sql.ColumnIndex;
import org.apache.cassandra.harry.dml.sql.TableSpec;
import org.apache.cassandra.harry.dml.sql.ValueIndex;
import org.apache.cassandra.harry.gen.Bijections.IndexedBijection;

public class ValuePoolBuilderTest
{
    // All types that have a native Bijection and can use strided mode.
    // Boolean is excluded: its adjustEntropyDomain remaps to {1,2}, so
    // bucket-stride descriptors fall outside the valid domain.
    @SuppressWarnings("unchecked")
    private static final Generator<DataType<?>> STRIDED_TYPES = Generators.pick(Arrays.asList(DataType.int8Type,
                                                                                              DataType.int16Type,
                                                                                              DataType.int32Type,
                                                                                              DataType.int64Type,
                                                                                              DataType.floatType,
                                                                                              DataType.doubleType,
                                                                                              DataType.uuidType,
                                                                                              DataType.timestampType,
                                                                                              DataType.blobType,
                                                                                              DataType.timeType,
                                                                                              DataType.varintType,
                                                                                              DataType.decimalType,
                                                                                              DataType.inetType));

    private static int populationForType(DataType<?> type)
    {
        if (type == DataType.int8Type) return 100;
        // int16Type.typeEntropy() is 256 (pre-existing); cap population below that
        if (type == DataType.int16Type) return 200;
        return 5000;
    }

    // -----------------------------------------------------------------------
    // Part 1: single IndexedBijection via ValuePoolBuilder.indexed()
    // -----------------------------------------------------------------------

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void stridedOrderedAndUnique() throws Throwable
    {
        PropertyChecker.forAll(STRIDED_TYPES, Generators.int64())
                       .withRuns(100)
                       .check((type, seed) -> {
                           int pop = populationForType(type);
                           IndexedBijection bij = ValuePoolBuilder.indexed(type)
                                                                  .population(pop)
                                                                  .seed(seed)
                                                                  .strided();
                           for (int i = 1; i < pop; i++)
                           {
                               Assert.assertTrue(
                                       String.format("type=%s: value(%d) not < value(%d)", type, i - 1, i),
                                       bij.compare(bij.descriptorAt(i - 1), bij.descriptorAt(i)) < 0);
                           }
                       });
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void stridedRoundTrip() throws Throwable
    {
        PropertyChecker.forAll(STRIDED_TYPES, Generators.int64())
                       .withRuns(100)
                       .check((type, seed) -> {
                           int pop = populationForType(type);
                           IndexedBijection bij = ValuePoolBuilder.indexed(type)
                                                                  .population(pop)
                                                                  .seed(seed)
                                                                  .strided();
                           for (int i = 0; i < pop; i++)
                           {
                               long descriptor = bij.descriptorAt(i);
                               Assert.assertEquals("idxFor at " + i, i, bij.idxFor(descriptor));
                               Object value = bij.inflate(descriptor);
                               Assert.assertEquals("deflate at " + i, descriptor, bij.deflate(value));
                           }
                       });
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void stridedDifferentSeeds() throws Throwable
    {
        // Use types with enough range that stride > 1, so the PRF offset
        // has room to differ between seeds.
        Generator<DataType<?>> wideTypes =
                (Generator<DataType<?>>) (Generator<?>) Generators.pick(Arrays.asList(
                        DataType.int32Type, DataType.int64Type));

        PropertyChecker.forAll(wideTypes, Generators.int64(), Generators.int64())
                       .withRuns(100)
                       .check((type, seedA, seedB) -> {
                           if (seedA.equals(seedB)) return;
                           int pop = populationForType(type);
                           IndexedBijection a = ValuePoolBuilder.indexed(type)
                                                                .population(pop)
                                                                .seed(seedA)
                                                                .strided();
                           IndexedBijection b = ValuePoolBuilder.indexed(type)
                                                                .population(pop)
                                                                .seed(seedB)
                                                                .strided();
                           int diffCount = 0;
                           for (int i = 0; i < pop; i++)
                           {
                               if (a.descriptorAt(i) != b.descriptorAt(i))
                                   diffCount++;
                           }
                           Assert.assertTrue(
                                   String.format("type=%s: only %d/%d differ", type, diffCount, pop),
                                   diffCount > pop / 2);
                       });
    }

    @Test
    public void inMemoryAsciiRoundTrip() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(50)
                       .check(seed -> {
                           IndexedBijection<String> bij = ValuePoolBuilder.indexed(DataType.asciiType)
                                                                          .population(500)
                                                                          .seed(seed)
                                                                          .inMemory();
                           Assert.assertEquals(500, bij.population());
                           for (int i = 0; i < 500; i++)
                           {
                               long descriptor = bij.descriptorAt(i);
                               Assert.assertEquals(i, bij.idxFor(descriptor));
                               String value = bij.inflate(descriptor);
                               Assert.assertEquals(descriptor, bij.deflate(value));
                           }
                       });
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void inMemoryNumericRoundTrip() throws Throwable
    {
        PropertyChecker.forAll(STRIDED_TYPES, Generators.int64())
                       .withRuns(50)
                       .check((type, seed) -> {
                           int pop = populationForType(type);
                           IndexedBijection bij = ValuePoolBuilder.indexed(type)
                                                                  .population(pop)
                                                                  .seed(seed)
                                                                  .inMemory();
                           for (int i = 0; i < pop; i++)
                           {
                               long descriptor = bij.descriptorAt(i);
                               Assert.assertEquals(i, bij.idxFor(descriptor));
                               Object value = bij.inflate(descriptor);
                               Assert.assertEquals(descriptor, bij.deflate(value));
                           }
                       });
    }

    @Test
    public void diskBackedRoundTrip() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(10)
                       .check(seed -> {
                           Path tmpDir = Files.createTempDirectory("harry-dsl-test");
                           try
                           {
                               IndexedBijection<Long> bij = ValuePoolBuilder.indexed(DataType.int64Type)
                                                                            .population(2000)
                                                                            .seed(seed)
                                                                            .diskBacked(tmpDir);
                               for (int i = 0; i < 2000; i++)
                               {
                                   long descriptor = bij.descriptorAt(i);
                                   Assert.assertEquals(i, bij.idxFor(descriptor));
                                   Long value = bij.inflate(descriptor);
                                   Assert.assertEquals(descriptor, bij.deflate(value));
                               }
                           }
                           finally
                           {
                               java.io.File[] files = tmpDir.toFile().listFiles();
                               if (files != null)
                                   for (java.io.File f : files) f.delete();
                               tmpDir.toFile().delete();
                           }
                       });
    }

    // -----------------------------------------------------------------------
    // Part 2: ValuePool via ValuePoolBuilder.valuePool()
    // -----------------------------------------------------------------------

    @Test
    public void valuePoolStridedProperties() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64(), Generators.int32(100, 2000))
                       .withRuns(50)
                       .check((seed, pop) -> {
                           TableSpec spec = TableSpec.builder("s", "t")
                                                     .column("pk", DataType.int64Type, TableSpec.pk())
                                                     .column("age", DataType.int32Type)
                                                     .column("score", DataType.int64Type)
                                                     .build();

                           ValuePoolBuilder.ValuePool pool = ValuePoolBuilder.valuePool(spec)
                                                                             .seed(seed)
                                                                             .defaultPopulation(pop)
                                                                             .strided()
                                                                             .build();

                           for (int col = 0; col < 3; col++)
                           {
                               ColumnIndex idx = new ColumnIndex(col);
                               Assert.assertEquals(pop.intValue(), pool.population(idx));
                               for (int i = 0; i < pop; i++)
                                   Assert.assertNotNull(pool.inflate(idx, ValueIndex.value(i)));
                           }
                       });
    }

    @Test
    public void valuePoolInMemoryProperties() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(30)
                       .check(seed -> {
                           TableSpec spec = TableSpec.builder("s", "t")
                                                     .column("pk", DataType.int32Type, TableSpec.pk())
                                                     .column("name", DataType.asciiType)
                                                     .build();
                           ValuePoolBuilder.ValuePool pool = ValuePoolBuilder.valuePool(spec)
                                                                             .seed(seed)
                                                                             .defaultPopulation(200)
                                                                             .inMemory()
                                                                             .build();

                           Assert.assertEquals(200, pool.population(new ColumnIndex(0)));
                           Assert.assertEquals(200, pool.population(new ColumnIndex(1)));
                           Assert.assertEquals("pk", pool.columnName(new ColumnIndex(0)));
                           Assert.assertEquals("name", pool.columnName(new ColumnIndex(1)));
                       });
    }

    @Test
    public void valuePoolMixedStrategies() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(30)
                       .check(seed -> {
                           TableSpec spec = TableSpec.builder("s", "t")
                                                     .column("pk", DataType.int64Type, TableSpec.pk())
                                                     .column("data", DataType.asciiType)
                                                     .column("count", DataType.int32Type)
                                                     .build();

                           ValuePoolBuilder.ValuePool pool = ValuePoolBuilder.valuePool(spec)
                                                                             .seed(seed)
                                                                             .defaultPopulation(300)
                                                                             .strided()
                                                                             .column("data", ValuePoolBuilder.Strategy.IN_MEMORY)
                                                                             .build();

                           Assert.assertEquals(300, pool.population(new ColumnIndex(0)));
                           Assert.assertEquals(300, pool.population(new ColumnIndex(1)));
                           Assert.assertEquals(300, pool.population(new ColumnIndex(2)));

                           for (int i = 0; i < 300; i++)
                           {
                               Assert.assertNotNull(pool.inflate(new ColumnIndex(0), ValueIndex.value(i)));
                               Assert.assertNotNull(pool.inflate(new ColumnIndex(1), ValueIndex.value(i)));
                               Assert.assertNotNull(pool.inflate(new ColumnIndex(2), ValueIndex.value(i)));
                           }
                       });
    }

    @Test
    public void valuePoolPerColumnPopulation() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(30)
                       .check(seed -> {
                           TableSpec spec = TableSpec.builder("s", "t")
                                                     .column("pk", DataType.int64Type, TableSpec.pk())
                                                     .column("v", DataType.int32Type)
                                                     .build();

                           ValuePoolBuilder.ValuePool pool = ValuePoolBuilder.valuePool(spec)
                                                                             .seed(seed)
                                                                             .defaultPopulation(100)
                                                                             .strided()
                                                                             .column("v", ValuePoolBuilder.Strategy.STRIDED, 5000)
                                                                             .build();

                           Assert.assertEquals(100, pool.population(new ColumnIndex(0)));
                           Assert.assertEquals(5000, pool.population(new ColumnIndex(1)));
                       });
    }

    @Test
    public void valuePoolBijectionAccess() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(30)
                       .check(seed -> {
                           TableSpec spec = TableSpec.builder("s", "t")
                                                     .column("pk", DataType.int64Type, TableSpec.pk())
                                                     .build();

                           ValuePoolBuilder.ValuePool pool = ValuePoolBuilder.valuePool(spec)
                                                                             .seed(seed)
                                                                             .defaultPopulation(500)
                                                                             .strided()
                                                                             .build();

                           IndexedBijection<Object> bij = pool.bijection(new ColumnIndex(0));
                           Assert.assertNotNull(bij);
                           Assert.assertEquals(500, bij.population());
                       });
    }
}
