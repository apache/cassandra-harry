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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Date;
import java.util.UUID;
import org.apache.cassandra.harry.util.Bytes;

public class Bijections
{
    public static final Bijection<Byte> INT8_GENERATOR = new ByteBijection();
    public static final Bijection<Short> INT16_GENERATOR = new Int16Bijection();
    public static final Bijection<Integer> INT32_GENERATOR = new Int32Bijection();
    public static final Bijection<Long> INT64_GENERATOR = new LongBijection();
    public static final Bijection<Float> FLOAT_GENERATOR = new FloatBijection();
    public static final Bijection<Double> DOUBLE_GENERATOR = new DoubleBijection();
    public static final Bijection<Boolean> BOOLEAN_GENERATOR = new BooleanBijection();

    public static final Bijection<UUID> UUID_GENERATOR = new UUIDBijection();
    public static final Bijection<UUID> TIME_UUID_GENERATOR = new TimeUUIDBijection();
    public static final Bijection<Date> TIMESTAMP_GENERATOR = new TimestampBijection();
    public static final Bijection<ByteBuffer> BLOB_GENERATOR = new BlobBijection();
    public static final Bijection<Long> TIME_GENERATOR = new TimeBijection();
    public static final Bijection<BigInteger> VARINT_GENERATOR = new VarintBijection();
    public static final Bijection<BigDecimal> DECIMAL_GENERATOR = new DecimalBijection();
    public static final Bijection<InetAddress> INET_GENERATOR = new InetBijection();

    /**
     * Indexed bijection allows to decouple descriptor order from value order, which makes data generation simpler.
     * <p>
     * For regular Harry bijections, this is done at no cost, since values are inflated in a way that preserves
     * descriptor order, which means that idx order is consistent with descriptor order and consistent with value order.
     * <p>
     * An indexed bijection allows order to be established via index, and use descriptor simply as a seed for random values.
     */
    public interface IndexedBijection<T> extends Bijection<T>
    {
        long idxFor(long descriptor);

        long descriptorAt(long idx);

        @Override
        default String toString(long descriptor)
        {
            if (descriptor == org.apache.cassandra.harry.MagicConstants.UNSET_DESCR)
                return Integer.toString(org.apache.cassandra.harry.MagicConstants.UNSET_IDX);

            if (descriptor == org.apache.cassandra.harry.MagicConstants.NIL_DESCR)
                return Integer.toString(org.apache.cassandra.harry.MagicConstants.NIL_IDX);

            return Long.toString(idxFor(descriptor));
        }
    }

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

        // TODO: byteSize is great, but you know what's better? Bit size! For example, for `boolean`, we only need a single bit.
        int byteSize();
        default long population()
        {
            return (long) byteSize() * Byte.SIZE;
        }
        /**
         * Compare as if we were comparing the values in question
         */
        int compare(long l, long r);

        default long adjustEntropyDomain(long descriptor)
        {
            return descriptor & Bytes.bytePatternFor(byteSize());
        }

        default long minValue()
        {
            return minForSize(byteSize());
        }

        default long maxValue()
        {
            return maxForSize(byteSize());
        }

        default boolean unsigned()
        {
            return false;
        }

        default Comparator<Long> descriptorsComparator()
        {
            return Long::compare;
        }

        default String toString(long pd)
        {
            return Long.toString(pd);
        }
    }

    public static long minForSize(int size)
    {
        long min = 1L << (size * Byte.SIZE - 1);

        if (size < Long.BYTES)
            min ^= Bytes.signMaskFor(size);

        return min;
    }

    public static long maxForSize(int size)
    {
        long max = Bytes.bytePatternFor(size) >>> 1;

        if (size < Long.BYTES)
            max ^= Bytes.signMaskFor(size);

        return max;
    }

    // TODO: two points:
    //   * We might be able to avoid boxing if we can generate straight to byte buffer (?)
    //   * since these data types are quite specialized, we do not strictly need complex interface for them, it might
    //     be easier to even create a special type for these. We need randomness source in cases of more complex generation,
    //     but not really here.

    /**
     * Reverse type is different from the regular one in that will generate values
     * that will sort the order that is opposite to the order of descriptor.
     */
    public static class ReverseBijection<T> implements Bijection<T>
    {
        private final Bijection<T> delegate;

        public ReverseBijection(Bijection<T> delegate)
        {
            this.delegate = delegate;
        }

        public T inflate(long descriptor)
        {
            return delegate.inflate(descriptor * -1 - 1);
        }

        public long deflate(T value)
        {
            return -1 * (delegate.deflate(value) + 1);
        }

        public int byteSize()
        {
            return delegate.byteSize();
        }

        public int compare(long l, long r)
        {
            return delegate.compare(r, l);
        }
    }

    public static class LongBijection implements Bijection<Long>
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

    public static class Int32Bijection implements Bijection<Integer>
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

    public static class Int16Bijection implements Bijection<Short>
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

    public static class ByteBijection implements Bijection<Byte>
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

    public static class BooleanBijection implements Bijection<Boolean>
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

        public long adjustEntropyDomain(long descriptor)
        {
            return (descriptor & 1) + 1;
        }
    }

    public static class FloatBijection implements Bijection<Float>
    {
        private static final int SIZE = Float.BYTES - 1;

        public Float inflate(long current)
        {
            return inflatePrimitive(current);
        }

        protected float inflatePrimitive(long current)
        {
            return Float.intBitsToFloat((int) current);
        }

        public long deflate(Float value)
        {
            return Float.floatToRawIntBits(value);
        }

        // In other words, there's no way we can extend entropy to a sign
        public boolean unsigned()
        {
            return true;
        }

        public int compare(long l, long r)
        {
            return Float.compare(inflatePrimitive(l), inflatePrimitive(r));
        }

        public int byteSize()
        {
            return SIZE;
        }
    }

    public static class ReverseFloatBijection extends FloatBijection
    {
        public float inflatePrimitive(long current)
        {
            return super.inflatePrimitive(current - 1) * -1;
        }

        public long deflate(Float value)
        {
            return super.deflate(value * -1 ) + 1;
        }

        public int compare(long l, long r)
        {
            return super.compare(r, l);
        }
    }

    public static class DoubleBijection implements Bijection<Double>
    {
        private static int SIZE = Double.BYTES - 1;

        public Double inflate(long current)
        {
            return inflatePrimitive(current);
        }

        protected double inflatePrimitive(long current)
        {
            return Double.longBitsToDouble(current);
        }

        public long deflate(Double value)
        {
            return Double.doubleToRawLongBits(value);
        }

        public int compare(long l, long r)
        {
            return Double.compare(inflatePrimitive(l), inflatePrimitive(r));
        }

        public int byteSize()
        {
            return SIZE;
        }

        /**
         * To avoid generating NaNs, we're using a smaller size for Double. But because of that, double became
         * sign-less. In other words, even if we generate a double, it will always be positive, since its most
         * significant bit isn't set. This means that
         */
        public boolean unsigned()
        {
            return true;
        }
    }

    public static class ReverseDoubleBijection extends DoubleBijection
    {
        public double inflatePrimitive(long current)
        {
            return super.inflatePrimitive(current - 1) * -1;
        }

        public long deflate(Double value)
        {
            return super.deflate(value * -1) + 1;
        }

        public int compare(long l, long r)
        {
            return super.compare(r, l);
        }
    }

    public static class UUIDBijection implements Bijection<UUID>
    {
        public UUID inflate(long d)
        {
            long top48 = (d >>> 8);
            long bot8 = d & 0xFFL;
            long msb = (top48 << 16) | 0x4000L | (bot8 << 4);
            long lsb = 0x8000_0000_0000_0000L;
            return new UUID(msb, lsb);
        }

        public long deflate(UUID value)
        {
            long msb = value.getMostSignificantBits();
            long top48 = (msb >>> 16) & 0x0000_FFFF_FFFF_FFFFL;
            long bot8 = (msb >>> 4) & 0xFFL;
            return (top48 << 8) | bot8;
        }

        public int compare(long l, long r)
        {
            return Long.compare(l, r);
        }

        public int byteSize()
        {
            return 7;
        }

        public boolean unsigned()
        {
            return true;
        }
    }

    public static class TimeUUIDBijection implements Bijection<UUID>
    {
        public UUID inflate(long d)
        {
            long timeLow = d & 0xFFFFFFFFL;
            long timeMid = (d >>> 32) & 0xFFFFL;
            long timeHi = (d >>> 48) & 0xFFL;
            long msb = (timeLow << 32) | (timeMid << 16) | 0x1000L | timeHi;
            long lsb = 0x8000_0000_0000_0000L;
            return new UUID(msb, lsb);
        }

        public long deflate(UUID value)
        {
            long msb = value.getMostSignificantBits();
            long timeLow = (msb >>> 32) & 0xFFFFFFFFL;
            long timeMid = (msb >>> 16) & 0xFFFFL;
            long timeHi = msb & 0xFFL;
            return (timeHi << 48) | (timeMid << 32) | timeLow;
        }

        public int compare(long l, long r)
        {
            return Long.compare(l, r);
        }

        public int byteSize()
        {
            return 7;
        }

        public boolean unsigned()
        {
            return true;
        }
    }

    public static class TimestampBijection implements Bijection<Date>
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
            return Long.compare(l, r);
        }

        public int byteSize()
        {
            return Long.BYTES;
        }
    }

    public static class BlobBijection implements Bijection<ByteBuffer>
    {
        public ByteBuffer inflate(long d)
        {
            long unsigned = d ^ Long.MIN_VALUE;
            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.putLong(0, unsigned);
            return buf;
        }

        public long deflate(ByteBuffer value)
        {
            return value.getLong(value.position()) ^ Long.MIN_VALUE;
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

    public static class TimeBijection implements Bijection<Long>
    {
        public Long inflate(long d)
        {
            return d;
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
            return 7;
        }

        public boolean unsigned()
        {
            return true;
        }
    }

    public static class VarintBijection implements Bijection<BigInteger>
    {
        public BigInteger inflate(long d)
        {
            return BigInteger.valueOf(d);
        }

        public long deflate(BigInteger value)
        {
            return value.longValueExact();
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

    public static class DecimalBijection implements Bijection<BigDecimal>
    {
        public BigDecimal inflate(long d)
        {
            return BigDecimal.valueOf(d);
        }

        public long deflate(BigDecimal value)
        {
            return value.longValueExact();
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

    public static class InetBijection implements Bijection<InetAddress>
    {
        public InetAddress inflate(long d)
        {
            int signed = (int) d;
            int unsigned = signed ^ 0x80000000;
            byte[] addr = {
                (byte) (unsigned >> 24), (byte) (unsigned >> 16),
                (byte) (unsigned >> 8), (byte) (unsigned)
            };
            try
            {
                return InetAddress.getByAddress(addr);
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }

        public long deflate(InetAddress value)
        {
            byte[] b = value.getAddress();
            int unsigned = ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16)
                           | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
            return (unsigned ^ 0x80000000) & 0xFFFFFFFFL;
        }

        public int compare(long l, long r)
        {
            return Integer.compare((int) l, (int) r);
        }

        public int byteSize()
        {
            return 4;
        }
    }
}
