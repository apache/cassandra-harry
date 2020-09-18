/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package harry.ddl;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableList;

import harry.generators.Generator;
import harry.generators.Surjections;

public class SchemaGenerators
{
    private final static long SCHEMAGEN_STREAM_ID = 0x6264593273L;

    public static Builder schema(String ks)
    {
        return new Builder(ks);
    }

    public static final Collection<ColumnSpec.DataType<?>> clusteringKeyTypes;
    public static final Collection<ColumnSpec.DataType<?>> columnTypes;

    static
    {
        ImmutableList.Builder<ColumnSpec.DataType<?>> builder = ImmutableList.builder();
        builder.add(ColumnSpec.int8Type,
                    ColumnSpec.int16Type,
                    ColumnSpec.int32Type,
                    ColumnSpec.int64Type,
// TODO re-enable boolean type; add it to ByteBufferUtil in Cassandra for that
//                    ColumnSpec.booleanType,
                    ColumnSpec.floatType,
                    ColumnSpec.doubleType,
                    ColumnSpec.asciiType);
        columnTypes = builder.build();
        builder = ImmutableList.builder();
        builder.addAll(columnTypes);
        for (ColumnSpec.DataType<?> columnType : columnTypes)
        {
            builder.add(ColumnSpec.ReversedType.getInstance(columnType));
        }
        clusteringKeyTypes = builder.build();
    }

    @SuppressWarnings("unchecked")
    public static <T> Generator<T> fromValues(Collection<T> allValues)
    {
        return fromValues((T[]) allValues.toArray());
    }

    public static <T> Generator<T> fromValues(T[] allValues)
    {
        return (rng) -> {
            return allValues[rng.nextInt(allValues.length - 1)];
        };
    }

    @SuppressWarnings("unchecked")
    public static Generator<ColumnSpec<?>> columnSpecGenerator(String prefix, ColumnSpec.Kind kind)
    {
        return fromValues(columnTypes)
               .map(new Function<ColumnSpec.DataType<?>, ColumnSpec<?>>()
               {
                   private int counter = 0;

                   public ColumnSpec<?> apply(ColumnSpec.DataType<?> type)
                   {
                       return new ColumnSpec<>(prefix + (counter++),
                                               type,
                                               kind);
                   }
               });
    }

    @SuppressWarnings("unchecked")
    public static Generator<ColumnSpec<?>> columnSpecGenerator(Collection<ColumnSpec.DataType<?>> columnTypes, String prefix, ColumnSpec.Kind kind)
    {
        return fromValues(columnTypes)
               .map(new Function<ColumnSpec.DataType<?>, ColumnSpec<?>>()
               {
                   private int counter = 0;

                   public ColumnSpec<?> apply(ColumnSpec.DataType<?> type)
                   {
                       return new ColumnSpec<>(prefix + (counter++),
                                               type,
                                               kind);
                   }
               });
    }

    @SuppressWarnings("unchecked")
    public static Generator<ColumnSpec<?>> clusteringColumnSpecGenerator(String prefix)
    {
        return fromValues(clusteringKeyTypes)
               .map(new Function<ColumnSpec.DataType<?>, ColumnSpec<?>>()
               {
                   private int counter = 0;

                   public ColumnSpec<?> apply(ColumnSpec.DataType<?> type)
                   {
                       return ColumnSpec.ck(prefix + (counter++), type);
                   }
               });
    }

    private static AtomicInteger tableCounter = new AtomicInteger(1);

    public static class Builder
    {
        private final String keyspace;
        private final Supplier<String> tableNameSupplier;

        private Generator<ColumnSpec<?>> pkGenerator = columnSpecGenerator("pk", ColumnSpec.Kind.PARTITION_KEY);
        private Generator<ColumnSpec<?>> ckGenerator = clusteringColumnSpecGenerator("ck");
        private Generator<ColumnSpec<?>> regularGenerator = columnSpecGenerator("regular", ColumnSpec.Kind.REGULAR);

        private int minPks = 1;
        private int maxPks = 1;
        private int minCks = 0;
        private int maxCks = 0;
        private int minRegular = 0;
        private int maxRegular = 0;

        public Builder(String keyspace)
        {
            this(keyspace, () -> "table_" + tableCounter.getAndIncrement());
        }

        public Builder(String keyspace, Supplier<String> tableNameSupplier)
        {
            this.keyspace = keyspace;
            this.tableNameSupplier = tableNameSupplier;
        }

        public Builder partitionKeyColumnCount(int numCols)
        {
            return partitionKeyColumnCount(numCols, numCols);
        }

        public Builder partitionKeyColumnCount(int minCols, int maxCols)
        {
            this.minPks = minCols;
            this.maxPks = maxCols;
            return this;
        }

        public Builder partitionKeySpec(int minCols, int maxCols, ColumnSpec.DataType<?>... columnTypes)
        {
            return partitionKeySpec(minCols, maxCols, Arrays.asList(columnTypes));
        }

        public Builder partitionKeySpec(int minCols, int maxCols, Collection<ColumnSpec.DataType<?>> columnTypes)
        {
            this.minPks = minCols;
            this.maxPks = maxCols;
            this.pkGenerator = columnSpecGenerator(columnTypes, "pk", ColumnSpec.Kind.PARTITION_KEY);
            return this;
        }

        public Builder clusteringColumnCount(int numCols)
        {
            return clusteringColumnCount(numCols, numCols);
        }

        public Builder clusteringColumnCount(int minCols, int maxCols)
        {
            this.minCks = minCols;
            this.maxCks = maxCols;
            return this;
        }

        public Builder clusteringKeySpec(int minCols, int maxCols, ColumnSpec.DataType<?>... columnTypes)
        {
            return clusteringKeySpec(minCols, maxCols, Arrays.asList(columnTypes));
        }

        public Builder clusteringKeySpec(int minCols, int maxCols, Collection<ColumnSpec.DataType<?>> columnTypes)
        {
            this.minCks = minCols;
            this.maxCks = maxCols;
            this.ckGenerator = columnSpecGenerator(columnTypes, "ck", ColumnSpec.Kind.CLUSTERING);
            return this;
        }

        public Builder regularColumnCount(int numCols)
        {
            return regularColumnCount(numCols, numCols);
        }

        public Builder regularColumnCount(int minCols, int maxCols)
        {
            this.minRegular = minCols;
            this.maxRegular = maxCols;
            return this;
        }

        public Builder regularColumnSpec(int minCols, int maxCols, ColumnSpec.DataType<?>... columnTypes)
        {
            return this.regularColumnSpec(minCols, maxCols, Arrays.asList(columnTypes));
        }

        public Builder regularColumnSpec(int minCols, int maxCols, Collection<ColumnSpec.DataType<?>> columnTypes)
        {
            this.minRegular = minCols;
            this.maxRegular = maxCols;
            this.regularGenerator = columnSpecGenerator(columnTypes, "regular", ColumnSpec.Kind.REGULAR);
            return this;
        }

        private static class ColumnCounts
        {
            private final int pks;
            private final int cks;
            private final int regulars;

            private ColumnCounts(int pks, int cks, int regulars)
            {
                this.pks = pks;
                this.cks = cks;
                this.regulars = regulars;
            }
        }

        public Generator<ColumnCounts> columnCountsGenerator()
        {
            return (rand) -> {
                int pks = rand.nextInt(minPks, maxPks);
                int cks = rand.nextInt(minCks, maxCks);
                int regulars = rand.nextInt(minRegular, maxRegular);

                return new ColumnCounts(pks, cks, regulars);
            };
        }

        public Generator<SchemaSpec> generator()
        {
            Generator<ColumnCounts> columnCountsGenerator = columnCountsGenerator();

            return columnCountsGenerator.flatMap(counts -> {
                return rand -> {
                    List<ColumnSpec<?>> pk = pkGenerator.generate(rand, counts.pks);
                    List<ColumnSpec<?>> ck = ckGenerator.generate(rand, counts.cks);
                    return new SchemaSpec(keyspace,
                                          tableNameSupplier.get(),
                                          pk,
                                          ck,
                                          regularGenerator.generate(rand, counts.regulars));
                };
            });
        }

        public Surjections.Surjection<SchemaSpec> surjection()
        {
            return generator().toSurjection(SCHEMAGEN_STREAM_ID);
        }
    }

    public static Surjections.Surjection<SchemaSpec> defaultSchemaSpecGen(String ks, String table)
    {
        return new SchemaGenerators.Builder(ks, () -> table)
               .partitionKeySpec(1, 4,
//                                                                             ColumnSpec.int8Type,
//                                                                             ColumnSpec.int16Type,
                                 ColumnSpec.int32Type,
                                 ColumnSpec.int64Type,
//                                                                             ColumnSpec.floatType,
//                                                                             ColumnSpec.doubleType,
                                 ColumnSpec.asciiType(4, 10))
               .clusteringKeySpec(1, 4,
//                                                                              ColumnSpec.int8Type,
//                                                                              ColumnSpec.int16Type,
                                  ColumnSpec.int32Type,
                                  ColumnSpec.int64Type,
//                                                                              ColumnSpec.floatType,
//                                                                              ColumnSpec.doubleType,
                                  ColumnSpec.asciiType(4, 10))
               .regularColumnSpec(1, 10,
//                                                                              ColumnSpec.int8Type,
//                                                                              ColumnSpec.int16Type,
                                  ColumnSpec.int32Type,
                                  ColumnSpec.int64Type,
//                                                                              ColumnSpec.floatType,
//                                                                              ColumnSpec.doubleType,
                                  ColumnSpec.asciiType(5, 10))
               .surjection();
    }

    public static final String DEFAULT_KEYSPACE_NAME = "harry";
    private static final String DEFAULT_PREFIX = "table_";
    private static final AtomicInteger counter = new AtomicInteger();
    private static final Supplier<String> tableNameSupplier = () -> DEFAULT_PREFIX + counter.getAndIncrement();

    // simplest schema gen, nothing can go wrong with it
    public static final Surjections.Surjection<SchemaSpec> longOnlySpecBuilder = new SchemaGenerators.Builder(DEFAULT_KEYSPACE_NAME, tableNameSupplier)
                                                                                 .partitionKeySpec(1, 1, ColumnSpec.int64Type)
                                                                                 .clusteringKeySpec(1, 1, ColumnSpec.int64Type)
                                                                                 .regularColumnSpec(1, 10, ColumnSpec.int64Type)
                                                                                 .surjection();

    private static final ColumnSpec.DataType<String> simpleStringType = ColumnSpec.asciiType(4, 10);
    private static final Surjections.Surjection<SchemaSpec> longAndStringSpecBuilder = new SchemaGenerators.Builder(DEFAULT_KEYSPACE_NAME, tableNameSupplier)
                                                                                       .partitionKeySpec(2, 2, ColumnSpec.int64Type, simpleStringType)
                                                                                       .clusteringKeySpec(2, 2, ColumnSpec.int64Type, simpleStringType)
                                                                                       .regularColumnSpec(1, 10, ColumnSpec.int64Type, simpleStringType)
                                                                                       .surjection();

    public static final Surjections.Surjection<SchemaSpec> longOnlyWithReverseSpecBuilder = new SchemaGenerators.Builder(DEFAULT_KEYSPACE_NAME, tableNameSupplier)
                                                                                            .partitionKeySpec(1, 1, ColumnSpec.int64Type)
                                                                                            .clusteringKeySpec(1, 1, ColumnSpec.ReversedType.getInstance(ColumnSpec.int64Type))
                                                                                            .regularColumnSpec(1, 10, ColumnSpec.int64Type)
                                                                                            .surjection();

    public static final Surjections.Surjection<SchemaSpec> longAndStringSpecWithReversedLongBuilder = new SchemaGenerators.Builder(DEFAULT_KEYSPACE_NAME, tableNameSupplier)
                                                                                                      .partitionKeySpec(2, 2, ColumnSpec.int64Type, simpleStringType)
                                                                                                      .clusteringKeySpec(2, 2, ColumnSpec.ReversedType.getInstance(ColumnSpec.int64Type), simpleStringType)
                                                                                                      .regularColumnSpec(1, 10, ColumnSpec.int64Type, simpleStringType)
                                                                                                      .surjection();

    public static final Surjections.Surjection<SchemaSpec> longAndStringSpecWithReversedStringBuilder = new SchemaGenerators.Builder(DEFAULT_KEYSPACE_NAME, tableNameSupplier)
                                                                                                        .partitionKeySpec(2, 2, ColumnSpec.int64Type, simpleStringType)
                                                                                                        .clusteringKeySpec(2, 2, ColumnSpec.int64Type, ColumnSpec.ReversedType.getInstance(simpleStringType))
                                                                                                        .regularColumnSpec(1, 10, ColumnSpec.int64Type, simpleStringType)
                                                                                                        .surjection();

    public static final Surjections.Surjection<SchemaSpec> longAndStringSpecWithReversedBothBuilder = new SchemaGenerators.Builder(DEFAULT_KEYSPACE_NAME, tableNameSupplier)
                                                                                                      .partitionKeySpec(2, 2, ColumnSpec.int64Type, simpleStringType)
                                                                                                      .clusteringKeySpec(2, 2, ColumnSpec.ReversedType.getInstance(ColumnSpec.int64Type), ColumnSpec.ReversedType.getInstance(simpleStringType))
                                                                                                      .regularColumnSpec(1, 10, ColumnSpec.int64Type, simpleStringType)
                                                                                                      .surjection();

    public static final Surjections.Surjection<SchemaSpec> withAllFeaturesEnabled = new SchemaGenerators.Builder(DEFAULT_KEYSPACE_NAME, tableNameSupplier)
                                                                                    .partitionKeySpec(1, 4, columnTypes)
                                                                                    .clusteringKeySpec(1, 4, clusteringKeyTypes)
                                                                                    .regularColumnSpec(1, 10, columnTypes)
                                                                                    .surjection();

    public static final Surjections.Surjection<SchemaSpec>[] PROGRESSIVE_GENERATORS = new Surjections.Surjection[]{
    longOnlySpecBuilder,
    longAndStringSpecBuilder,
    longOnlyWithReverseSpecBuilder,
    longAndStringSpecWithReversedLongBuilder,
    longAndStringSpecWithReversedStringBuilder,
    longAndStringSpecWithReversedBothBuilder,
    withAllFeaturesEnabled
    };
    // Create schema generators that would produce tables starting with just a few features, progressing to use more
    public static Supplier<SchemaSpec> progression(int switchAfter)
    {
        Supplier<SchemaSpec>[] generators = new Supplier[PROGRESSIVE_GENERATORS.length];
        for (int i = 0; i < generators.length; i++)
            generators[i] = PROGRESSIVE_GENERATORS[i].toSupplier();

        return new Supplier<SchemaSpec>()
        {
            private final AtomicInteger counter = new AtomicInteger();
            public SchemaSpec get()
            {
                int idx = (counter.getAndIncrement() / switchAfter) % generators.length;
                SchemaSpec spec = generators[idx].get();
                int tries = 100;
                while ((spec.ckGenerator.byteSize() != Long.BYTES || spec.pkGenerator.byteSize() != Long.BYTES) && tries > 0)
                {
                    System.out.println("Skipping schema, since it doesn't have enough entropy bits available: " + spec.compile().cql());
                    spec = generators[idx].get();
                    tries--;
                }

                assert tries > 0 : String.format("Max number of tries exceeded on generator %d, can't generate a needed schema", idx);
                return spec;
            }


        };
    }

    public static int DEFAULT_SWITCH_AFTER = 5;
    public static int GENERATORS_COUNT = PROGRESSIVE_GENERATORS.length;
    public static int DEFAULT_RUNS = DEFAULT_SWITCH_AFTER * PROGRESSIVE_GENERATORS.length;

    public static Supplier<SchemaSpec> progression()
    {
        return progression(DEFAULT_SWITCH_AFTER); // would generate 30 tables before wrapping around
    }
}