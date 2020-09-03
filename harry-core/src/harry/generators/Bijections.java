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

package harry.generators;

import java.util.Date;
import java.util.UUID;

public class Bijections
{
    public static final Bijection<Byte> INT8_GENERATOR = new ByteGenerator();
    public static final Bijection<Short> INT16_GENERATOR = new Int16Generator();
    public static final Bijection<Integer> INT32_GENERATOR = new Int32Generator();
    public static final Bijection<Long> INT64_GENERATOR = new LongGenerator();
    public static final Bijection<Float> FLOAT_GENERATOR = new FloatGenerator();
    public static final Bijection<Double> DOUBLE_GENERATOR = new DoubleGenerator();
    public static final Bijection<Boolean> BOOLEAN_GENERATOR = new BooleanGenerator();

    public static final Bijection<UUID> UUID_GENERATOR = new UUIDGenerator();
    public static final Bijection<Date> TIMESTAMP_GENERATOR = new TimestampGenerator();

    /**
     * When generating a value, invertible generator first draws a long from the random number generator, and
     * passes it to the normalization function. Normalization scales the long value down to the range that corresponds
     * to the generated value range. For example, for Boolean, the range is of a size 2. For Integer - 2^32, etc.
     * <p>
     * deflated has to be equal to adjustEntropyDomain value.
     * <p>
     * When inflating, we should inflate up to adjustEntropyDomain values. This way, deflated values will correspond to infoated ones.
     */
    public interface Bijection<T>
    {
        T inflate(long descriptor);

        long deflate(T value);

        int byteSize();

        int compare(long l, long r);

        default long adjustEntropyDomain(long descriptor)
        {
            return descriptor & Bytes.bytePatternFor(byteSize());
        }
    }

    // TODO: two points:
    //   * We might be able to avoid boxing if we can generate straight to byte buffer (?)
    //   * since these data types are quite specialized, we do not strictly need complex interface for them, it might
    //     be easier to even create a special type for these. We need randomness source in cases of more complex generation,
    //     but not really here.
    public static class ReverseBijection<T> implements Bijection<T>
    {
        private final Bijection<T> delegate;

        public ReverseBijection(Bijection<T> delegate)
        {
            this.delegate = delegate;
        }

        public T inflate(long descriptor)
        {
            return delegate.inflate(descriptor * -1);
        }

        public long deflate(T value)
        {
            return -1 * delegate.deflate(value);
        }

        public int byteSize()
        {
            return delegate.byteSize();
        }

        public int compare(long l, long r)
        {
            return delegate.compare(r, l);
        }

        public long adjustEntropyDomain(long descriptor)
        {
            long pattern = Bytes.BYTES[byteSize() - 1];
            return descriptor & (pattern >> 1);
        }
    }

    public static class LongGenerator implements Bijection<Long>
    {
        public Long inflate(long current)
        {
            return current;
        }

        public long deflate(Long value)
        {
            return value;
        }

        public int compare(long l, long r)
        {
            return Long.compare(l, r);
        }

        public int byteSize()
        {
            return Long.BYTES;
        }
    }

    public static class Int32Generator implements Bijection<Integer>
    {
        public Integer inflate(long current)
        {
            return (int) current;
        }

        public long deflate(Integer value)
        {
            return value & 0xffffffffL;
        }

        public int compare(long l, long r)
        {
            return Integer.compare((int) l, (int) r);
        }

        public int byteSize()
        {
            return Integer.BYTES;
        }
    }

    public static class Int16Generator implements Bijection<Short>
    {
        public Short inflate(long current)
        {
            return (short) current;
        }

        public long deflate(Short value)
        {
            return value & 0xffffL;
        }

        public int compare(long l, long r)
        {
            return Short.compare((short) l, (short) r);
        }

        public int byteSize()
        {
            return Short.BYTES;
        }
    }

    public static class ByteGenerator implements Bijection<Byte>
    {
        public Byte inflate(long current)
        {
            return (byte) current;
        }

        public long deflate(Byte value)
        {
            return value & 0xffL;
        }

        public int compare(long l, long r)
        {
            return Byte.compare((byte) l, (byte) r);
        }

        public int byteSize()
        {
            return Byte.BYTES;
        }
    }

    public static class BooleanGenerator implements Bijection<Boolean>
    {
        public Boolean inflate(long current)
        {
            return inflatePrimitive(current);
        }

        private boolean inflatePrimitive(long current)
        {
            return current == 2;
        }

        public long deflate(Boolean value)
        {
            return value ? 2 : 1;
        }

        public int byteSize()
        {
            return Byte.BYTES;
        }

        public int compare(long l, long r)
        {
            return Byte.compare((byte) l, (byte) r);
        }

        // TODO: this this right? Why +1?
        public long adjustEntropyDomain(long descriptor)
        {
            return (descriptor & 1) + 1;
        }
    }

    public static class FloatGenerator implements Bijection<Float>
    {
        public Float inflate(long current)
        {
            return inflatePrimitive(current);
        }

        protected float inflatePrimitive(long current)
        {
            long tmp = current & 0xffffffffL;
            tmp ^= ((tmp >> 31) & 0x7fffffffL);
            return Float.intBitsToFloat((int) tmp);
        }

        public long deflate(Float value)
        {
            int tmp = Float.floatToRawIntBits(value);
            tmp ^= ((tmp >> 31) & 0x7fffffffL);
            return tmp;
        }

        public int compare(long l, long r)
        {
            return Float.compare(inflatePrimitive(l), inflatePrimitive(r));
        }

        public int byteSize()
        {
            return Float.BYTES;
        }

        public long adjustEntropyDomain(long descriptor)
        {
            return (~0x8F000000L & descriptor) & 0xffffffffL;
        }
    }

    public static class ReverseFloatGenerator extends FloatGenerator
    {
        public float inflatePrimitive(long current)
        {
            return super.inflatePrimitive(current) * -1;
        }

        public long deflate(Float value)
        {
            return super.deflate(value * -1);
        }
    }

    public static class DoubleGenerator implements Bijection<Double>
    {
        public Double inflate(long current)
        {
            return inflatePrimitive(current);
        }

        protected double inflatePrimitive(long current)
        {
            current = current ^ ((current >> 63) & 0x7fffffffffffffffL);
            return Double.longBitsToDouble(current);
        }

        public long deflate(Double value)
        {
            long current = Double.doubleToRawLongBits(value);
            current = current ^ ((current >> 63) & 0x7fffffffffffffffL);
            return current;
        }

        public int compare(long l, long r)
        {
            return Double.compare(inflatePrimitive(l), inflatePrimitive(r));
        }

        public int byteSize()
        {
            return Double.BYTES;
        }

        public long adjustEntropyDomain(long descriptor)
        {
            return (~0x8F00000000000000L & descriptor);
        }
    }


    public static class ReverseDoubleGenerator extends DoubleGenerator
    {
        public double inflatePrimitive(long current)
        {
            return super.inflatePrimitive(current) * -1;
        }

        public long deflate(Double value)
        {
            return super.deflate(value * -1);
        }
    }

    public static class UUIDGenerator implements Bijection<UUID>
    {
        public UUID inflate(long current)
        {
            // order is determined by the top bits
            return new UUID(current, current);
        }

        public long deflate(UUID value)
        {
            return value.getMostSignificantBits();
        }

        public int compare(long l, long r)
        {
            return Byte.compare((byte) l, (byte) r);
        }

        public int byteSize()
        {
            return Long.BYTES;
        }
    }

    public static class TimestampGenerator implements Bijection<Date>
    {
        public Date inflate(long descriptor)
        {
            return new Date(descriptor);
        }

        public long deflate(Date value)
        {
            return value.getTime();
        }

        public int compare(long l, long r)
        {
            return Byte.compare((byte) l, (byte) r);
        }

        public int byteSize()
        {
            return Long.BYTES;
        }

        public long adjustEntropyDomain(long descriptor)
        {
            return descriptor & (Bytes.bytePatternFor(byteSize() >> 1));
        }
    }
}