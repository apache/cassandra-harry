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

package org.apache.cassandra.harry;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import accord.utils.Invariants;

import org.apache.cassandra.harry.util.StringUtils;
import org.apache.cassandra.harry.gen.Generator;

// TODO: counters
// TODO: UDTs
// TODO: collections: frozen/unfrozen
// TODO: empty / 0 / min / max values if present
public class ColumnSpec<T>
{
    public final String name;
    public final DataType<T> type;
    public final Generator<T> gen;
    public final Kind kind;
    /** Per-column population override; 0 means use the schema-wide default (populationPerColumn). */
    public final int population;

    public ColumnSpec(String name,
                      DataType<T> type,
                      Generator<T> gen,
                      Kind kind)
    {
        this(name, type, gen, kind, 0);
    }

    public ColumnSpec(String name,
                      DataType<T> type,
                      Generator<T> gen,
                      Kind kind,
                      int population)
    {
        this.name = name;
        this.type = Invariants.nonNull(type);
        this.gen = Invariants.nonNull(gen);
        this.kind = kind;
        this.population = population;
    }

    public ColumnSpec<T> withPopulation(int population)
    {
        return new ColumnSpec<>(name, type, gen, kind, population);
    }

    public ColumnSpec<T> withGenerator(Generator<T> gen)
    {
        return new ColumnSpec<>(name, type, gen, kind, population);
    }

    public String toCQL()
    {
        return String.format("%s %s%s",
                             StringUtils.maybeQuote(name),
                             type,
                             kind == Kind.STATIC ? " static" : "");
    }

    public String toSQL()
    {
        return String.format("%s %s", StringUtils.maybeQuote(name), type.sqlName());
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnSpec<?> that = (ColumnSpec<?>) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(type, that.type) &&
               kind == that.kind;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, kind);
    }

    public String name()
    {
        return name;
    }

    public boolean isReversed()
    {
        return type.isReversed();
    }

    public String toString()
    {
        return name + '(' + type.toString() + ")";
    }

    public Generator<T> gen()
    {
        return gen;
    }

    public static <T> ColumnSpec<T> pk(String name, DataType<T> type, Generator<T> gen)
    {
        return new ColumnSpec<>(name, type, gen, Kind.PARTITION_KEY);
    }

    public static <T> ColumnSpec<T> pk(String name, DataType<T> type)
    {
        return new ColumnSpec<>(name, type, org.apache.cassandra.harry.gen.Generators.defaultFor(type), Kind.PARTITION_KEY);
    }

    @SuppressWarnings("unchecked")
    public static <T> ColumnSpec<T> ck(String name, DataType<T> type, Generator<T> gen, boolean isReversed)
    {
        return new ColumnSpec(name, isReversed ? DataType.ReversedType.getInstance(type) : type, gen, Kind.CLUSTERING);
    }

    public static <T> ColumnSpec<T> ck(String name, DataType<T> type)
    {
        return ck(name, type, false);
    }

    public static <T> ColumnSpec<T> ck(String name, DataType<T> type, boolean isReversed)
    {
        return new ColumnSpec(name, isReversed ? DataType.ReversedType.getInstance(type) : type,
                              org.apache.cassandra.harry.gen.Generators.defaultFor(type),
                              Kind.CLUSTERING);
    }


    public static <T> ColumnSpec<T> regularColumn(String name, DataType<T> type, Generator<T> gen)
    {
        return new ColumnSpec<>(name, type, gen, Kind.REGULAR);
    }

    public static <T> ColumnSpec<T> regularColumn(String name, DataType<T> type)
    {
        return new ColumnSpec(name, type, org.apache.cassandra.harry.gen.Generators.defaultFor(type), Kind.REGULAR);
    }

    public static <T> ColumnSpec<T> staticColumn(String name, DataType<T> type, Generator<T> gen)
    {
        return new ColumnSpec<>(name, type, gen, Kind.STATIC);
    }

    public static <T> ColumnSpec<T> staticColumn(String name, DataType<T> type)
    {
        return new ColumnSpec(name, type, org.apache.cassandra.harry.gen.Generators.defaultFor(type), Kind.STATIC);
    }

    public enum Kind
    {
        CLUSTERING, REGULAR, STATIC, PARTITION_KEY
    }

    // Forwarding references for backward compatibility
    public static final DataType<Byte> int8Type = DataType.int8Type;
    public static final DataType<Short> int16Type = DataType.int16Type;
    public static final DataType<Integer> int32Type = DataType.int32Type;
    public static final DataType<Long> int64Type = DataType.int64Type;
    public static final DataType<Boolean> booleanType = DataType.booleanType;
    public static final DataType<Float> floatType = DataType.floatType;
    public static final DataType<Double> doubleType = DataType.doubleType;
    public static final DataType<ByteBuffer> blobType = DataType.blobType;
    public static final DataType<String> asciiType = DataType.asciiType;
    public static final DataType<String> textType = DataType.textType;
    public static final DataType<UUID> uuidType = DataType.uuidType;
    public static final DataType<Date> timestampType = DataType.timestampType;
    public static final DataType<BigInteger> varintType = DataType.varintType;
    public static final DataType<Long> timeType = DataType.timeType;
    public static final DataType<BigDecimal> decimalType = DataType.decimalType;
    public static final DataType<InetAddress> inetType = DataType.inetType;
    public static final List<DataType<?>> TYPES = DataType.TYPES;

    // Forwarding type alias for backward compatibility
    public static class ReversedType<T> extends DataType.ReversedType<T>
    {
        public ReversedType(DataType<T> baseType)
        {
            super(baseType);
        }
    }
}
