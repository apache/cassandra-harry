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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public abstract class DataType<T>
{
    protected final String cqlName;

    protected DataType(String cqlName)
    {
        this.cqlName = cqlName;
    }

    public abstract /* unsigned */ long typeEntropy();

    public boolean isReversed()
    {
        return false;
    }

    public final String toString()
    {
        return cqlName;
    }

    public String sqlName()
    {
        return cqlName;
    }

    public final boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataType<?> dataType = (DataType<?>) o;
        return Objects.equals(cqlName, dataType.cqlName);
    }

    public final int hashCode()
    {
        return Objects.hash(cqlName);
    }

    public abstract Comparator<T> comparator();

    public static abstract class ComparableDataType<T extends Comparable<? super T>> extends DataType<T>
    {
        protected ComparableDataType(String cqlName)
        {
            super(cqlName);
        }

        @Override
        public Comparator<T> comparator()
        {
            return Comparable::compareTo;
        }
    }

    public static final DataType<Byte> int8Type = new ComparableDataType<>("tinyint")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << (8 * Byte.BYTES);
        }

        @Override
        public String sqlName() { return "smallint"; }
    };

    public static final DataType<Short> int16Type = new ComparableDataType<>("smallint")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << Short.SIZE;
        }
    };

    public static final DataType<Integer> int32Type = new ComparableDataType<>("int")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << Integer.SIZE;
        }

        @Override
        public String sqlName() { return "integer"; }
    };

    public static final DataType<Long> int64Type = new ComparableDataType<Long>("bigint")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << (8 * Long.BYTES - 1);
        }
    };

    public static final DataType<Boolean> booleanType = new ComparableDataType<Boolean>("boolean")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 2;
        }
    };

    public static final DataType<Float> floatType = new ComparableDataType<Float>("float")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << (4 * Float.BYTES);
        }

        @Override
        public String sqlName() { return "real"; }
    };

    public static final DataType<Double> doubleType = new ComparableDataType<Double>("double")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public String sqlName() { return "double precision"; }
    };

    public static final DataType<ByteBuffer> blobType = new DataType<>("blob")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        public Comparator<ByteBuffer> comparator()
        {
            return (o1, o2) -> {
                ByteBuffer b1 = o1.duplicate();
                ByteBuffer b2 = o2.duplicate();
                int len1 = b1.remaining();
                int len2 = b2.remaining();
                int len = Math.min(len1, len2);
                for (int i = 0; i < len; i++)
                {
                    int cmp = Byte.compareUnsigned(b1.get(), b2.get());
                    if (cmp != 0)
                        return cmp;
                }
                return len1 - len2;
            };
        }

        @Override
        public String sqlName() { return "bytea"; }
    };

    public static final DataType<String> asciiType = new ComparableDataType<>("ascii")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public String sqlName() { return "text"; }
    };

    // utf8
    public static final DataType<String> textType = new DataType<>("text")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public Comparator<String> comparator()
        {
            return (o1, o2) -> Arrays.compareUnsigned(o1.getBytes(StandardCharsets.UTF_8), o2.getBytes(StandardCharsets.UTF_8));
        }
    };

    public static final DataType<UUID> uuidType = new DataType<>("uuid")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        public Comparator<UUID> comparator()
        {
            return UUID::compareTo;
        }
    };

    public static final DataType<Date> timestampType = new ComparableDataType<>("timestamp")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }
    };

    /**
     * Timestamp restricted to PostgreSQL's valid range (4713 BC to 294276 AD).
     * Uses the same SQL type name ("timestamp") but a generator that stays
     * within PG bounds.
     */
    public static final DataType<Date> pgTimestampType = new ComparableDataType<>("timestamp")
    {
        // PG range in millis: roughly -210866803200000 to 9224318016000000.
        // We use 0 (1970) to 4102444800000 (2100) for practical test coverage.
        private static final long PG_TS_MIN = 0L;
        private static final long PG_TS_MAX = 4102444800000L;

        @Override
        public /* unsigned */ long typeEntropy()
        {
            return PG_TS_MAX - PG_TS_MIN;
        }
    };

    /**
     * Fixed-size 8-byte blob that round-trips through BlobBijection. The
     * standard blobType uses a variable-length generator (10-20 bytes) whose
     * output cannot be deflated back through the 8-byte bijection. This
     * variant generates exactly 8 bytes so inflate/deflate is lossless and
     * lexicographic comparison matches PostgreSQL's bytea ordering.
     */
    public static final DataType<ByteBuffer> pgBlobType = new DataType<>("blob")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        public Comparator<ByteBuffer> comparator()
        {
            return (o1, o2) -> {
                ByteBuffer b1 = o1.duplicate();
                ByteBuffer b2 = o2.duplicate();
                int len1 = b1.remaining();
                int len2 = b2.remaining();
                int len = Math.min(len1, len2);
                for (int i = 0; i < len; i++)
                {
                    int cmp = Byte.compareUnsigned(b1.get(), b2.get());
                    if (cmp != 0)
                        return cmp;
                }
                return len1 - len2;
            };
        }

        @Override
        public String sqlName() { return "bytea"; }
    };

    public static final DataType<BigInteger> varintType = new ComparableDataType<>("varint")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public String sqlName() { return "numeric"; }
    };

    public static final DataType<Long> timeType = new ComparableDataType<>("time")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }
    };

    public static final DataType<BigDecimal> decimalType = new ComparableDataType<>("decimal")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }
    };

    public static final DataType<InetAddress> inetType = new DataType<>("inet")
    {
        @Override
        public /* unsigned */ long typeEntropy()
        {
            return 1L << 63;
        }

        @Override
        public Comparator<InetAddress> comparator()
        {
            return (o1, o2) -> Arrays.compareUnsigned(o1.getAddress(), o2.getAddress());
        }
    };

    public static final List<DataType<?>> TYPES;

    static
    {
        List<DataType<?>> types = new ArrayList<>()
        {{
            add(int8Type);
            add(int16Type);
            add(int32Type);
            add(int64Type);
            add(floatType);
            add(doubleType);
            // TODO: SAI tests seem to fail these types
            // add(booleanType);
            // add(inetType);
            // add(varintType);
            // add(decimalType);
            add(asciiType);
            add(textType);
            // TODO: blob is not supported in SAI
            // add(blobType);
            add(uuidType);
            add(timestampType);
            // TODO: SAI test fails due to TimeSerializer#toString in tracing
            //  add(timeType);
            // TODO: compose proper value
            // add(timeUuidType);
        }};
        TYPES = Collections.unmodifiableList(types);
    }

    public static final long MAX_ENTROPY = 1L << 63;

    public static /* unsigned */ long cumulativeEntropy(List<DataType<?>> types)
    {
        if (types.isEmpty())
            return 0;

        long entropy = 1;
        for (DataType<?> type : types)
        {
            if (Long.compareUnsigned(type.typeEntropy(), MAX_ENTROPY) == 0)
                return MAX_ENTROPY;

            long next = entropy * type.typeEntropy();
            if (Long.compareUnsigned(next, entropy) < 0 || Long.compareUnsigned(next, type.typeEntropy()) < 0)
                return MAX_ENTROPY;

            entropy = next;
        }

        return entropy;
    }

    public static class ReversedType<T> extends DataType<T>
    {
        public static final Map<DataType<?>, ReversedType<?>> cache = new HashMap<>()
        {{
            for (DataType<?> type : TYPES)
                put(type, new ReversedType<>(type));
        }};

        private final DataType<T> baseType;

        public ReversedType(DataType<T> baseType)
        {
            super(baseType.cqlName);
            this.baseType = baseType;
        }

        @Override
        public /* unsigned */ long typeEntropy()
        {
            return baseType.typeEntropy();
        }

        public boolean isReversed()
        {
            return true;
        }

        public static <T> DataType<T> getInstance(DataType<T> type)
        {
            ReversedType<T> t = (ReversedType<T>) cache.get(type);
            if (t == null)
                t = new ReversedType<>(type);
            assert t.baseType == type : String.format("Type mismatch %s != %s", t.baseType, type);
            return t;
        }

        @Override
        public Comparator<T> comparator()
        {
            return baseType.comparator();
        }
    }
}
