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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import accord.utils.Invariants;

import org.apache.cassandra.harry.DataType;
import org.apache.cassandra.harry.util.BitSet;

public class Generators
{
    public static Generator<BitSet> bitSet(int size)
    {
        return rng -> {
            BitSet bitSet = BitSet.allUnset(size);
            for (int i = 0; i < size; i++)
                if (rng.nextBoolean())
                    bitSet.set(i);
            return bitSet;
        };
    }

    public static Generator<String> ascii(int minLength, int maxLength)
    {
        return new StringGenerator(minLength, maxLength, 1, 127);
    }

    public static Generator<String> utf8(int minLength, int maxLength)
    {
        return rng -> {
            int length = rng.nextInt(minLength, maxLength);
            int[] codePoints = new int[length];
            for (int i = 0; i < length; i++)
            {
                int next;
                // Exclude null (0x0000) and surrogate range (0xD800-0xDFFF)
                if (rng.nextBoolean())
                    next = rng.nextInt(0x0001, 0xD800);
                else
                    next = rng.nextInt(0xE000, 0x110000);
                codePoints[i] = next;
            }

            return new String(codePoints, 0, codePoints.length);
        };
    }

    public static Generator<String> englishAlphabet(int minLength, int maxLength)
    {
        return new StringGenerator(minLength, maxLength, 97, 122);
    }

    public static Generator<Byte> int8()
    {
        return rng -> (byte) rng.nextInt();
    }

    public static Generator<Short> int16()
    {
        return rng -> (short) rng.nextInt();
    }

    public static Generator<Integer> int32()
    {
        return EntropySource::nextInt;
    }

    public static Generator<Integer> int32(int min, int max)
    {
        return rng -> rng.nextInt(min, max);
    }

    public static Generator<Long> int64()
    {
        return new LongGenerator();
    }

    public static Generator<Long> int64(long min, long max)
    {
        return rng -> rng.nextLong(min, max);
    }

    public static Generator<Boolean> bool()
    {
        return EntropySource::nextBoolean;
    }

    public static Generator<Double> doubles()
    {
        return EntropySource::nextDouble;
    }

    public static Generator<Float> floats()
    {
        return EntropySource::nextFloat;
    }

    public static Generator<InetAddress> inetAddr()
    {
        return new InetAddressGenerator();
    }

    public static <T> Generator<T> inetAddr(Generator<T> delegate)
    {
        return new UniqueGenerator<>(delegate, 10);
    }

    public static Generator<ByteBuffer> bytes(int minSize, int maxSize)
    {
        return byteArrays(minSize, maxSize).map(ByteBuffer::wrap);
    }

    public static Generator<byte[]> byteArrays(int minSize, int maxSize)
    {
        return rng -> {
            int size = rng.nextInt(minSize, maxSize);
            byte[] bytes = new byte[size];
            for (int i = 0; i < size; )
                for (long v = rng.next(),
                     n = Math.min(size - i, Long.SIZE / Byte.SIZE);
                     n-- > 0; v >>= Byte.SIZE)
                    bytes[i++] = (byte) v;
            return bytes;
        };
    }

    public static Generator<UUID> uuidGen()
    {
        return rng -> {
            long msb = rng.next();
            // Adopted from JDK code, UUID#randomUUID
            // randomBytes[6]  &= 0x0f;  /* clear version        */
            msb &= ~(0xFL << 12);
            // randomBytes[6]  |= 0x40;  /* set to version 4     */
            msb |= (0x40L << 8);
            long lsb = rng.next();
            // randomBytes[8]  &= 0x3f;  /* clear variant        */
            lsb &= ~(0x3L << 62);
            // randomBytes[8]  |= 0x80;  /* set to IETF variant  */
            lsb |= (0x2L << 62);
            return new UUID(msb, lsb);
        };
    }

    public static Generator<BigInteger> bigInt()
    {
        return rng -> BigInteger.valueOf(rng.next());
    }

    public static Generator<BigDecimal> bigDecimal()
    {
        return rng -> BigDecimal.valueOf(rng.next());
    }

    public static <T> TrackingGenerator<T> tracking(Generator<T> delegate)
    {
        return new TrackingGenerator<>(delegate);
    }

    public static class TrackingGenerator<T> implements Generator<T>
    {
        private final Set<T> generated;
        private final Generator<T> delegate;
        public TrackingGenerator(Generator<T> delegate)
        {
            this.generated = new HashSet<>();
            this.delegate = delegate;
        }

        public Iterable<T> generated()
        {
            return generated;
        }

        @Override
        public T generate(EntropySource rng)
        {
            T next = delegate.generate(rng);
            generated.add(next);
            return next;
        }
    }

    public static <T> Generator<T> unique(Generator<T> delegate)
    {
        return new UniqueGenerator<>(delegate, 100);
    }


    /**
     * WARNING: uses hash code as a proxy for equality
     */
    public static class UniqueGenerator<T> implements Generator<T>
    {
        private final Set<Integer> hashCodes = new HashSet<>();
        private final Generator<T> delegate;
        private final int maxSteps;

        public UniqueGenerator(Generator<T> delegate, int maxSteps)
        {
            this.delegate = delegate;
            this.maxSteps = maxSteps;
        }

        /**
         *
         */
        public void clear()
        {
            hashCodes.clear();
        }

        public T generate(EntropySource rng)
        {
            for (int i = 0; i < maxSteps; i++)
            {
                T v = delegate.generate(rng);
                int hashCode = v.hashCode();
                Invariants.require(hashCode != System.identityHashCode(v), "hashCode was not overridden for type %s", v.getClass());
                if (hashCodes.contains(hashCode))
                    continue;
                hashCodes.add(hashCode);
                return v;
            }

            throw new IllegalStateException(String.format("Could not generate a unique value within %d from %s", maxSteps, delegate));
        }
    }

    public static final class StringGenerator implements Generator<String>
    {
        private final int minLength;
        private final int maxLength;
        private final int minChar;
        private final int maxChar;

        public StringGenerator(int minLength, int maxLength, int minChar, int maxChar)
        {
            this.minLength = minLength;
            this.maxLength = maxLength;
            this.minChar = minChar;
            this.maxChar = maxChar;
        }

        @Override
        public String generate(EntropySource rng)
        {
            int length = rng.nextInt(minLength, maxLength);
            int[] codePoints = new int[length];
            for (int i = 0; i < length; i++)
                codePoints[i] = rng.nextInt(minChar, maxChar);
            return new String(codePoints, 0, codePoints.length);
        }
    }

    public static final class LongGenerator implements Generator<Long>
    {
        @Override
        public Long generate(EntropySource rng)
        {
            return rng.next();
        }
    }

    public static class InetAddressGenerator implements Generator<InetAddress>
    {
        @Override
        public InetAddress generate(EntropySource rng)
        {
            int orig = rng.nextInt();
            byte[] bytes = new byte[]{ (byte) (orig & 0xff),
                                       (byte) ((orig >> 8) & 0xff),
                                       (byte) ((orig >> 16) & 0xff),
                                       (byte) ((orig >> 24) & 0xff) };
            try
            {
                return InetAddress.getByAddress(bytes);
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static <T> Generator<T> pick(List<T> ts)
    {
        if (ts.isEmpty())
            throw new IllegalStateException("Can't pick from an empty list");
        return (rng) -> ts.get(rng.nextInt(0, ts.size()));
    }

    public static <T> Generator<T> pick(T... ts)
    {
        return pick(Arrays.asList(ts));
    }

    public static <T> Generator<List<T>> list(int minSize, int maxSize, Generator<T> gen)
    {
        return rng -> {
            List<T> objects = new ArrayList<>();
            int size = rng.nextInt(minSize, maxSize);
            for (int i = 0; i < size; i++)
                objects.add(gen.generate(rng));

            return objects;
        };
    }

    public static Generator<Object[]> zipArray(Generator<?>... gens)
    {
        return rng -> {
            Object[] objects = new Object[gens.length];
            for (int i = 0; i < objects.length; i++)
                objects[i] = gens[i].generate(rng);

            return objects;
        };
    }

    public static <T> Generator<List<T>> subsetGenerator(List<T> list)
    {
        return subsetGenerator(list, 0, list.size() - 1);
    }

    public static <T> Generator<List<T>> subsetGenerator(List<T> list, int minSize, int maxSize)
    {
        return (rng) -> {
            int count = rng.nextInt(minSize, maxSize);
            Set<T> set = new HashSet<>();
            for (int i = 0; i < count; i++)
                set.add(list.get(rng.nextInt(minSize, maxSize)));

            return (List<T>) new ArrayList<>(set);
        };
    }

    public static <T extends Enum<T>> Generator<T> enumValues(Class<T> e)
    {
        return pick(Arrays.asList(e.getEnumConstants()));
    }

    public static <T> Generator<List<T>> list(Generator<T> of, int maxSize)
    {
        return list(of, 0, maxSize);
    }

    public static <T> Generator<List<T>> list(Generator<T> of, int minSize, int maxSize)
    {
        return (rng) -> {
            int count = rng.nextInt(minSize, maxSize);
            return of.generate(rng, count);
        };
    }

    public static <T> Generator<T> constant(T constant)
    {
        return (random) -> constant;
    }

    public static <T> Generator<T> constant(Supplier<T> constant)
    {
        return (random) -> constant.get();
    }

    private static final Map<DataType<?>, Generator<?>> DEFAULT_GENERATORS = new HashMap<>();

    static
    {
        DEFAULT_GENERATORS.put(DataType.int8Type, int8());
        DEFAULT_GENERATORS.put(DataType.int16Type, int16());
        DEFAULT_GENERATORS.put(DataType.int32Type, int32());
        DEFAULT_GENERATORS.put(DataType.int64Type, int64());
        DEFAULT_GENERATORS.put(DataType.booleanType, bool());
        DEFAULT_GENERATORS.put(DataType.floatType, floats());
        DEFAULT_GENERATORS.put(DataType.doubleType, doubles());
        DEFAULT_GENERATORS.put(DataType.blobType, bytes(10, 20));
        DEFAULT_GENERATORS.put(DataType.pgBlobType, bytes(8, 9));
        DEFAULT_GENERATORS.put(DataType.asciiType, englishAlphabet(5, 10));
        DEFAULT_GENERATORS.put(DataType.textType, utf8(10, 20));
        DEFAULT_GENERATORS.put(DataType.uuidType, uuidGen());
        DEFAULT_GENERATORS.put(DataType.timestampType, int64(0, Long.MAX_VALUE).map(Date::new));
        DEFAULT_GENERATORS.put(DataType.pgTimestampType, int64(0, 4102444800000L).map(Date::new));
        DEFAULT_GENERATORS.put(DataType.varintType, bigInt());
        DEFAULT_GENERATORS.put(DataType.timeType, int64(0, Long.MAX_VALUE));
        DEFAULT_GENERATORS.put(DataType.decimalType, bigDecimal());
        DEFAULT_GENERATORS.put(DataType.inetType, inetAddr());
    }

    @SuppressWarnings("unchecked")
    public static <T> Generator<T> defaultFor(DataType<T> type)
    {
        // Handle ReversedType by looking up the base type
        Generator<?> gen = DEFAULT_GENERATORS.get(type);
        if (gen == null)
            throw new IllegalArgumentException("No default generator for type: " + type);
        return (Generator<T>) gen;
    }
}
