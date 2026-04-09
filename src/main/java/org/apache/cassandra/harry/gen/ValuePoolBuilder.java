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

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.harry.DataType;
import org.apache.cassandra.harry.dml.sql.ColumnIndex;
import org.apache.cassandra.harry.dml.sql.TableSpec;
import org.apache.cassandra.harry.dml.sql.ValueIndex;
import org.apache.cassandra.harry.gen.Bijections.IndexedBijection;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;

/**
 * A fluent DSL for creating {@link IndexedBijection} instances and {@link ValuePool} collections.
 * <p>
 * Three back-end strategies are available for every type:
 * <ul>
 *   <li><b>strided</b> -- O(1) seek and inversion, zero materialization. Uses
 *       {@link StridedBijection#toIndexed} to compose a bucket/stride mapping with
 *       the type's native {@link Bijections.Bijection}. Only works for types whose
 *       bijection is order-preserving (all built-in numeric types).</li>
 *   <li><b>in-memory</b> -- materializes the full sorted population in a heap array
 *       via {@link InvertibleGenerator}. Works for every type, including strings and
 *       composite generators.</li>
 *   <li><b>disk-backed</b> -- materializes the sorted population to a file via
 *       {@link DiskBackedInvertibleGenerator}. Required for populations too large for
 *       the heap.</li>
 * </ul>
 *
 * <h3>Creating a single IndexedBijection</h3>
 * <pre>
 *   IndexedBijection&lt;Integer&gt; ints = ValuePoolBuilder.indexed(DataType.int32Type)
 *       .population(10_000)
 *       .seed(42)
 *       .strided();
 *
 *   IndexedBijection&lt;String&gt; strings = ValuePoolBuilder.indexed(DataType.asciiType)
 *       .population(5_000)
 *       .seed(42)
 *       .inMemory();
 *
 *   IndexedBijection&lt;Long&gt; huge = ValuePoolBuilder.indexed(DataType.int64Type)
 *       .population(1_000_000_000L)
 *       .seed(99)
 *       .diskBacked(Paths.get("/tmp/harry"));
 * </pre>
 *
 * <h3>Creating a ValuePool from a TableSpec</h3>
 * <pre>
 *   ValuePool pool = ValuePoolBuilder.valuePool(spec)
 *       .seed(42)
 *       .defaultPopulation(1000)
 *       .strided()             // default strategy for all columns
 *       .column("name", b -&gt; b.inMemory())  // override for one column
 *       .build();
 * </pre>
 */
public class ValuePoolBuilder
{
    // -- Part 1: single IndexedBijection builder --

    public static <T> BijectionBuilder<T> indexed(DataType<T> type)
    {
        return new BijectionBuilder<>(type, Generators.defaultFor(type), type.comparator());
    }

    public static <T> BijectionBuilder<T> indexed(DataType<T> type, Generator<T> gen)
    {
        return new BijectionBuilder<>(type, gen, type.comparator());
    }

    public static class BijectionBuilder<T>
    {
        private final DataType<T> type;
        private final Generator<T> gen;
        private final Comparator<T> comparator;
        private long pop = 1000;
        private long seed = 0;

        BijectionBuilder(DataType<T> type, Generator<T> gen, Comparator<T> comparator)
        {
            this.type = type;
            this.gen = gen;
            this.comparator = comparator;
        }

        public BijectionBuilder<T> population(long population)
        {
            this.pop = population;
            return this;
        }

        public BijectionBuilder<T> seed(long seed)
        {
            this.seed = seed;
            return this;
        }

        /**
         * O(1) seek, O(1) inversion, zero materialization. Composes a
         * {@link StridedBijection} with the type's native bijection.
         */
        public IndexedBijection<T> strided()
        {
            return StridedBijection.toIndexed(resolveBijection(type), seed, pop);
        }

        /**
         * Materializes the sorted population in a heap array.
         * Works for every type including strings and composite generators.
         */
        @SuppressWarnings("unchecked")
        public IndexedBijection<T> inMemory()
        {
            return new InvertibleGenerator<>(new JdkRandomEntropySource(seed),
                                             type.typeEntropy(),
                                             (int) Math.min(pop, Integer.MAX_VALUE),
                                             gen,
                                             comparator);
        }

        /**
         * Materializes the sorted population to a file on disk.
         * Required for populations too large for the heap.
         */
        public IndexedBijection<T> diskBacked(Path dir) throws IOException
        {
            return DiskBackedInvertibleGenerator.open(dir, seed, pop,
                                                      DiskBackedInvertibleGenerator.DEFAULT_INDEX_STRIDE,
                                                      gen, comparator,
                                                      type.typeEntropy());
        }
    }

    // -- Part 2: ValuePool builder --

    public static Builder valuePool(TableSpec spec)
    {
        return new Builder(spec);
    }

    public static class ValuePool
    {
        private final Map<ColumnIndex, IndexedBijection<Object>> bijections;
        private final Map<ColumnIndex, String> columnNames;

        ValuePool(Map<ColumnIndex, IndexedBijection<Object>> bijections,
                  Map<ColumnIndex, String> columnNames)
        {
            this.bijections = bijections;
            this.columnNames = columnNames;
        }

        public String columnName(ColumnIndex column)
        {
            String name = columnNames.get(column);
            if (name == null)
                throw new IllegalArgumentException("Unknown column index: " + column.getValue());
            return name;
        }

        public Object inflate(ColumnIndex column, ValueIndex value)
        {
            IndexedBijection<Object> bij = bijections.get(column);
            if (bij == null)
                throw new IllegalArgumentException("Unknown column index: " + column.getValue());
            return bij.inflate(value.getValue());
        }

        public long population(ColumnIndex column)
        {
            IndexedBijection<Object> bij = bijections.get(column);
            if (bij == null)
                throw new IllegalArgumentException("Unknown column index: " + column.getValue());
            return bij.population();
        }

        public IndexedBijection<Object> bijection(ColumnIndex column)
        {
            IndexedBijection<Object> bij = bijections.get(column);
            if (bij == null)
                throw new IllegalArgumentException("Unknown column index: " + column.getValue());
            return bij;
        }
    }

    public enum Strategy { STRIDED, IN_MEMORY }

    public static class Builder
    {
        private final TableSpec spec;
        private long seed = 0;
        private int defaultPopulation = 1000;
        private Strategy defaultStrategy = Strategy.IN_MEMORY;
        private Path diskDir = null;
        private final Map<String, Strategy> columnStrategies = new HashMap<>();
        private final Map<String, Integer> columnPopulations = new HashMap<>();

        Builder(TableSpec spec)
        {
            this.spec = spec;
        }

        public Builder seed(long seed)
        {
            this.seed = seed;
            return this;
        }

        public Builder defaultPopulation(int population)
        {
            this.defaultPopulation = population;
            return this;
        }

        public Builder strided()
        {
            this.defaultStrategy = Strategy.STRIDED;
            return this;
        }

        public Builder inMemory()
        {
            this.defaultStrategy = Strategy.IN_MEMORY;
            return this;
        }

        public Builder diskBacked(Path dir)
        {
            this.diskDir = dir;
            return this;
        }

        public Builder column(String name, Strategy strategy)
        {
            columnStrategies.put(name, strategy);
            return this;
        }

        public Builder column(String name, int population)
        {
            columnPopulations.put(name, population);
            return this;
        }

        public Builder column(String name, Strategy strategy, int population)
        {
            columnStrategies.put(name, strategy);
            columnPopulations.put(name, population);
            return this;
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        public ValuePool build()
        {
            Map<ColumnIndex, IndexedBijection<Object>> bijections = new HashMap<>();
            Map<ColumnIndex, String> names = new HashMap<>();

            for (TableSpec.Column col : spec.columns())
            {
                ColumnIndex idx = new ColumnIndex(col.ordinal);
                names.put(idx, col.name);

                int pop = columnPopulations.getOrDefault(col.name, defaultPopulation);

                Strategy strategy = columnStrategies.getOrDefault(col.name, defaultStrategy);
                DataType colType = col.type;

                IndexedBijection<?> bij;
                if (diskDir != null && strategy != Strategy.STRIDED && strategy != Strategy.IN_MEMORY)
                {
                    try
                    {
                        bij = ValuePoolBuilder.indexed(colType)
                                          .population(pop)
                                          .seed(seed)
                                          .diskBacked(diskDir);
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException("Failed to create disk-backed bijection for " + col.name, e);
                    }
                }
                else if (strategy == Strategy.STRIDED)
                {
                    bij = ValuePoolBuilder.indexed(colType)
                                      .population(pop)
                                      .seed(seed)
                                      .strided();
                }
                else
                {
                    bij = ValuePoolBuilder.indexed(colType)
                                      .population(pop)
                                      .seed(seed)
                                      .inMemory();
                }

                bijections.put(idx, (IndexedBijection<Object>) bij);
            }

            return new ValuePool(bijections, names);
        }
    }

    // -- Bijection resolution --

    private static final Map<DataType<?>, Bijections.Bijection<?>> BIJECTION_MAP = new HashMap<>();

    static
    {
        BIJECTION_MAP.put(DataType.int8Type, Bijections.INT8_GENERATOR);
        BIJECTION_MAP.put(DataType.int16Type, Bijections.INT16_GENERATOR);
        BIJECTION_MAP.put(DataType.int32Type, Bijections.INT32_GENERATOR);
        BIJECTION_MAP.put(DataType.int64Type, Bijections.INT64_GENERATOR);
        BIJECTION_MAP.put(DataType.floatType, Bijections.FLOAT_GENERATOR);
        BIJECTION_MAP.put(DataType.doubleType, Bijections.DOUBLE_GENERATOR);
        BIJECTION_MAP.put(DataType.booleanType, Bijections.BOOLEAN_GENERATOR);
        BIJECTION_MAP.put(DataType.uuidType, Bijections.UUID_GENERATOR);
        BIJECTION_MAP.put(DataType.timestampType, Bijections.TIMESTAMP_GENERATOR);
        BIJECTION_MAP.put(DataType.blobType, Bijections.BLOB_GENERATOR);
        BIJECTION_MAP.put(DataType.timeType, Bijections.TIME_GENERATOR);
        BIJECTION_MAP.put(DataType.varintType, Bijections.VARINT_GENERATOR);
        BIJECTION_MAP.put(DataType.decimalType, Bijections.DECIMAL_GENERATOR);
        BIJECTION_MAP.put(DataType.inetType, Bijections.INET_GENERATOR);
    }

    @SuppressWarnings("unchecked")
    static <T> Bijections.Bijection<T> resolveBijection(DataType<T> type)
    {
        Bijections.Bijection<?> bij = BIJECTION_MAP.get(type);
        if (bij == null)
            throw new IllegalArgumentException("No native bijection for type: " + type +
                                               ". Use inMemory() or diskBacked() instead of strided().");
        return (Bijections.Bijection<T>) bij;
    }
}
