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

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.common.collect.ImmutableList;

import harry.generators.Bijections;
import harry.generators.StringBijection;

public class ColumnSpec<T>
{
    public final String name;
    public final DataType<T> type;
    public final Kind kind;
    int columnIndex;

    public ColumnSpec(String name,
                      DataType<T> type,
                      Kind kind)
    {
        this.name = name;
        this.type = type;
        this.kind = kind;
    }

    void setColumnIndex(int idx)
    {
        this.columnIndex = idx;
    }

    public int getColumnIndex()
    {
        return columnIndex;
    }

    public String toCQL()
    {
        return String.format("%s %s%s",
                             name,
                             type.toString(),
                             kind == Kind.STATIC ? " static" : "");
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnSpec<?> that = (ColumnSpec<?>) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(type, that.type) &&
               kind == that.kind;
    }

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

    public Bijections.Bijection<T> generator()
    {
        return type.generator();
    }

    public T inflate(long current)
    {
        return type.generator().inflate(current);
    }

    public long adjustEntropyDomain(long current)
    {
        return type.generator().adjustEntropyDomain(current);
    }

    public long deflate(T value)
    {
        return type.generator().deflate(value);
    }

    public static ColumnSpec<?> pk(String name, DataType<?> type)
    {
        return new ColumnSpec<>(name, type, Kind.PARTITION_KEY);
    }

    @SuppressWarnings("unchecked")
    public static ColumnSpec<?> ck(String name, DataType<?> type, boolean isReversed)
    {
        return new ColumnSpec(name, isReversed ? ReversedType.getInstance(type) : type, Kind.CLUSTERING);
    }

    @SuppressWarnings("unchecked")
    public static ColumnSpec<?> ck(String name, DataType<?> type)
    {
        return new ColumnSpec(name, type, Kind.CLUSTERING);
    }

    public static ColumnSpec<?> regularColumn(String name, DataType<?> type)
    {
        return new ColumnSpec<>(name, type, Kind.REGULAR);
    }

    public static ColumnSpec<?> staticColumn(String name, DataType<?> type)
    {
        return new ColumnSpec<>(name, type, Kind.CLUSTERING);
    }

    public enum Kind
    {
        CLUSTERING, REGULAR, STATIC, PARTITION_KEY
    }

    public static abstract class DataType<T>
    {
        protected final String cqlName;

        protected DataType(String cqlName)
        {
            this.cqlName = cqlName;
        }

        public boolean isReversed()
        {
            return false;
        }

        public abstract Bijections.Bijection<T> generator();

        public abstract int maxSize();

        public String toString()
        {
            return cqlName;
        }
    }

    public static final DataType<Byte> int8Type = new DataType<Byte>("tinyint")
    {
        public Bijections.Bijection<Byte> generator()
        {
            return Bijections.INT8_GENERATOR;
        }

        public int maxSize()
        {
            return Byte.BYTES;
        }
    };
    public static final DataType<Short> int16Type = new DataType<Short>("smallint")
    {
        public Bijections.Bijection<Short> generator()
        {
            return Bijections.INT16_GENERATOR;
        }

        public int maxSize()
        {
            return Short.BYTES;
        }
    };
    public static final DataType<Integer> int32Type = new DataType<Integer>("int")
    {
        public Bijections.Bijection<Integer> generator()
        {
            return Bijections.INT32_GENERATOR;
        }

        public int maxSize()
        {
            return Integer.BYTES;
        }
    };
    public static final DataType<Long> int64Type = new DataType<Long>("bigint")
    {
        public Bijections.Bijection<Long> generator()
        {
            return Bijections.INT64_GENERATOR;
        }

        public int maxSize()
        {
            return Long.BYTES;
        }
    };
    public static final DataType<Boolean> booleanType = new DataType<Boolean>("boolean")
    {
        public Bijections.Bijection<Boolean> generator()
        {
            return Bijections.BOOLEAN_GENERATOR;
        }

        public int maxSize()
        {
            return Byte.BYTES;
        }
    };
    public static final DataType<Float> floatType = new DataType<Float>("float")
    {
        public Bijections.Bijection<Float> generator()
        {
            return Bijections.FLOAT_GENERATOR;
        }

        public int maxSize()
        {
            return Float.BYTES;
        }
    };
    public static final DataType<Double> doubleType = new DataType<Double>("double")
    {
        public Bijections.Bijection<Double> generator()
        {
            return Bijections.DOUBLE_GENERATOR;
        }

        public int maxSize()
        {
            return Double.BYTES;
        }
    };
    public static final DataType<String> asciiType = new DataType<String>("ascii")
    {
        private final Bijections.Bijection<String> gen = new StringBijection();

        public Bijections.Bijection<String> generator()
        {
            return gen;
        }

        public int maxSize()
        {
            return Long.BYTES;
        }
    };

    public static DataType<String> asciiType(int nibbleSize, int maxRandomNibbles)
    {
        Bijections.Bijection<String> gen = new StringBijection(nibbleSize, maxRandomNibbles);

        return new DataType<String>("ascii")
        {
            public Bijections.Bijection<String> generator()
            {
                return gen;
            }

            public int maxSize()
            {
                return Long.BYTES;
            }
        };
    }

    public static final DataType<UUID> uuidType = new DataType<UUID>("uuid")
    {
        public Bijections.Bijection<UUID> generator()
        {
            return Bijections.UUID_GENERATOR;
        }

        public int maxSize()
        {
            return Long.BYTES;
        }
    };

    public static final DataType<Date> timestampType = new DataType<Date>("timestamp")
    {
        public Bijections.Bijection<Date> generator()
        {
            return Bijections.TIMESTAMP_GENERATOR;
        }

        public int maxSize()
        {
            return Long.BYTES;
        }
    };

    public static final Collection<DataType<?>> DATA_TYPES = ImmutableList.of(
    ColumnSpec.int8Type,
    ColumnSpec.int16Type,
    ColumnSpec.int32Type,
    ColumnSpec.int64Type,
    ColumnSpec.booleanType,
    ColumnSpec.floatType,
    ColumnSpec.doubleType,
    ColumnSpec.asciiType,
    ColumnSpec.uuidType,
    ColumnSpec.timestampType);

    public static class ReversedType<T> extends DataType<T>
    {
        public static final Map<DataType<?>, ReversedType<?>> cache = new HashMap()
        {{
            put(int8Type, new ReversedType<>(int8Type));
            put(int16Type, new ReversedType<>(int16Type));
            put(int32Type, new ReversedType<>(int32Type));
            put(int64Type, new ReversedType<>(int64Type));
            put(booleanType, new ReversedType<>(booleanType));
            put(floatType, new ReversedType<>(floatType, new Bijections.ReverseFloatGenerator()));
            put(doubleType, new ReversedType<>(doubleType, new Bijections.ReverseDoubleGenerator()));
            put(asciiType, new ReversedType<>(asciiType));
        }};

        private final DataType<T> baseType;
        private final Bijections.Bijection<T> generator;

        public ReversedType(DataType<T> baseType)
        {
            super(baseType.cqlName);
            this.baseType = baseType;
            this.generator = new Bijections.ReverseBijection<>(baseType.generator());
        }

        public ReversedType(DataType<T> baseType, Bijections.Bijection<T> generator)
        {
            super(baseType.cqlName);
            this.baseType = baseType;
            this.generator = generator;
        }

        public boolean isReversed()
        {
            return true;
        }

        public Bijections.Bijection<T> generator()
        {
            return generator;
        }

        public int maxSize()
        {
            return baseType.maxSize();
        }

        public DataType<T> baseType()
        {
            return baseType;
        }

        public static <T> DataType<T> getInstance(DataType<T> type)
        {
            ReversedType<T> t = (ReversedType<T>) cache.get(type);
            if (t == null)
                t = new ReversedType<>(type);
            assert t.baseType == type : "Type mismatch";
            return t;
        }
    }
}