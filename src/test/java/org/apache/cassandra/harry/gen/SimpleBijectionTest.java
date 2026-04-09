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

import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.harry.DataType;
import org.apache.cassandra.harry.checker.PropertyChecker;
import org.apache.cassandra.harry.dml.sql.TableSpec;
import org.apache.cassandra.harry.gen.Bijections.Bijection;
import org.apache.cassandra.harry.gen.Bijections.IndexedBijection;


import static org.apache.cassandra.harry.checker.Properties.*;

public class SimpleBijectionTest
{
    @Test
    public void testOrder() throws Throwable
    {
        for (BijectionFixture<?> f : ALL_BIJECTIONS)
            checkOrder(f);
    }

    @SuppressWarnings("unchecked")
    private <T> void checkOrder(BijectionFixture<T> f) throws Throwable
    {
        PropertyChecker.forAll(Generators.list(descriptorGen(f.bij), 20, 20))
                       .withRuns(100)
                       .check(descriptors -> {
                           descriptors.sort((a, b) -> f.bij.compare(a, b));
                           for (int i = 1; i < descriptors.size(); i++)
                           {
                               T prev = f.bij.inflate(descriptors.get(i - 1));
                               T curr = f.bij.inflate(descriptors.get(i));
                               int cmp = f.cmp.compare(prev, curr);
                               Assert.assertTrue(
                                   String.format("%s: inflate values not sorted: %s >= %s (descriptors %d, %d)",
                                                 f.name, prev, curr, descriptors.get(i - 1), descriptors.get(i)),
                                   cmp <= 0);
                           }
                       });
    }

    @Test
    public void testArrayOrder() throws Throwable
    {
        PropertyChecker.forAll(Generators.int64())
                       .withRuns(20)
                       .check(seed -> {
                           TableSpec spec = TableSpec.builder("s", "t")
                                                     .seed(seed)
                                                     .defaultPopulation(100)
                                                     .column("pk", DataType.int64Type, TableSpec.pk())
                                                     .column("ck0", DataType.asciiType)
                                                     .column("ck1", DataType.asciiType)
                                                     .build();

                           List<TableSpec.Column> cols = spec.regularColumns();
                           for (int col = 0; col < cols.size(); col++)
                           {
                               IndexedBijection<Object> bij = cols.get(col).bijection;
                               Object previous = null;
                               for (int i = 0; i < bij.population(); i++)
                               {
                                   long descr = bij.descriptorAt(i);
                                   Object next = bij.inflate(descr);
                                   if (previous != null)
                                   {
                                       @SuppressWarnings("unchecked")
                                       Comparator<Object> cmp = (Comparator<Object>) cols.get(col).type.comparator();
                                       Assert.assertTrue(cmp.compare(next, previous) > 0);
                                   }
                                   Assert.assertEquals(descr, bij.deflate(next));
                                   previous = next;
                               }
                           }
                       });
    }

    @Test
    public void testInflateDeflateSymmetry() throws Throwable
    {
        for (BijectionFixture<?> f : ALL_BIJECTIONS)
            checkInflateDeflateSymmetry(f);
    }

    private <T> void checkInflateDeflateSymmetry(BijectionFixture<T> f) throws Throwable
    {
        PropertyChecker.forAll(descriptorGen(f.bij))
                       .withRuns(1000)
                       .check(roundtrip(f.bij::inflate, f.bij::deflate));
    }

    @Test
    public void testOrderPreservation() throws Throwable
    {
        for (BijectionFixture<?> f : ALL_BIJECTIONS)
            checkOrderPreservation(f);
    }

    @SuppressWarnings("unchecked")
    private <T> void checkOrderPreservation(BijectionFixture<T> f) throws Throwable
    {
        PropertyChecker.forAll(descriptorGen(f.bij), descriptorGen(f.bij))
                       .withRuns(1000)
                       .check((d1, d2) -> {
                           if (d1.equals(d2))
                               return;
                           T v1 = f.bij.inflate(d1);
                           T v2 = f.bij.inflate(d2);
                           int descriptorOrder = f.bij.compare(d1, d2);
                           int valueOrder = f.cmp.compare(v1, v2);
                           Assert.assertEquals(
                               String.format("%s: order mismatch for descriptors %d, %d -> %s, %s",
                                             f.name, d1, d2, v1, v2),
                               Integer.signum(descriptorOrder), Integer.signum(valueOrder));
                       });
    }

    @Test
    public void testUuidVersion() throws Throwable
    {
        PropertyChecker.forAll(descriptorGen(Bijections.UUID_GENERATOR))
                       .withRuns(1000)
                       .check(invariant(d -> Bijections.UUID_GENERATOR.inflate(d).version() == 4,
                                        "UUID bijection must produce version 4"));
    }

    @Test
    public void testTimeUuidVersion() throws Throwable
    {
        PropertyChecker.forAll(descriptorGen(Bijections.TIME_UUID_GENERATOR))
                       .withRuns(1000)
                       .check(invariant(d -> Bijections.TIME_UUID_GENERATOR.inflate(d).version() == 1,
                                        "TimeUUID bijection must produce version 1"));
    }

    @Test
    public void testCompareReflexive() throws Throwable
    {
        for (BijectionFixture<?> f : ALL_BIJECTIONS)
            checkCompareReflexive(f);
    }

    private <T> void checkCompareReflexive(BijectionFixture<T> f) throws Throwable
    {
        PropertyChecker.forAll(descriptorGen(f.bij))
                       .withRuns(500)
                       .check(d -> Assert.assertEquals(
                               String.format("%s: compare(d, d) must be 0 for descriptor %d", f.name, d),
                               0, f.bij.compare(d, d)));
    }

    @Test
    public void testCompareAntisymmetric() throws Throwable
    {
        for (BijectionFixture<?> f : ALL_BIJECTIONS)
            checkCompareAntisymmetric(f);
    }

    private <T> void checkCompareAntisymmetric(BijectionFixture<T> f) throws Throwable
    {
        PropertyChecker.forAll(descriptorGen(f.bij), descriptorGen(f.bij))
                       .withRuns(500)
                       .check((d1, d2) -> {
                           int fwd = Integer.signum(f.bij.compare(d1, d2));
                           int rev = Integer.signum(f.bij.compare(d2, d1));
                           Assert.assertEquals(
                               String.format("%s: compare(%d,%d)=%d but compare(%d,%d)=%d",
                                             f.name, d1, d2, fwd, d2, d1, rev),
                               fwd, -rev);
                       });
    }

    /**
     * Generate descriptors in the valid range for a given bijection, respecting
     * byteSize masking and unsigned constraints, avoiding descriptor 0 which
     * may collide with magic constants.
     */
    private static Generator<Long> descriptorGen(Bijection<?> bij)
    {
        return rng -> {
            long d;
            do
            {
                long raw = rng.next();
                d = bij.adjustEntropyDomain(bij.unsigned() ? (raw & Long.MAX_VALUE) : raw);
            }
            while (d == 0);
            return d;
        };
    }

    /**
     * Cassandra-compatible UUID comparator: compare version first, then
     * unsigned MSB (for non-v1), then unsigned LSB.
     */
    private static final Comparator<UUID> CASSANDRA_UUID_CMP = (a, b) -> {
        int versionCmp = Integer.compare(a.version(), b.version());
        if (versionCmp != 0)
            return versionCmp;
        int msbCmp = Long.compareUnsigned(a.getMostSignificantBits(), b.getMostSignificantBits());
        if (msbCmp != 0)
            return msbCmp;
        return Long.compareUnsigned(a.getLeastSignificantBits(), b.getLeastSignificantBits());
    };

    /**
     * Cassandra-compatible TimeUUID comparator: reorder timestamp bytes from
     * UUID v1 MSB, then signed compare of reordered MSBs, then LSBs.
     */
    private static final Comparator<UUID> CASSANDRA_TIMEUUID_CMP = (a, b) -> {
        long reorderedA = reorderTimestampBytes(a.getMostSignificantBits());
        long reorderedB = reorderTimestampBytes(b.getMostSignificantBits());
        int cmp = Long.compare(reorderedA, reorderedB);
        if (cmp != 0)
            return cmp;
        return Long.compare(a.getLeastSignificantBits(), b.getLeastSignificantBits());
    };

    private static long reorderTimestampBytes(long msb)
    {
        long timeLow = (msb >>> 32) & 0xFFFFFFFFL;
        long timeMid = (msb >>> 16) & 0xFFFFL;
        long timeHi = msb & 0xFFFL;
        return (timeHi << 48) | (timeMid << 32) | timeLow;
    }

    private static final class BijectionFixture<T>
    {
        final String name;
        final Bijection<T> bij;
        final Comparator<T> cmp;

        BijectionFixture(String name, Bijection<T> bij, Comparator<T> cmp)
        {
            this.name = name;
            this.bij = bij;
            this.cmp = cmp;
        }

        @SuppressWarnings("unchecked")
        static <T> BijectionFixture<T> of(String name, Bijection<T> bij, DataType<T> type)
        {
            return new BijectionFixture<>(name, bij, type.comparator());
        }
    }

    @SuppressWarnings("rawtypes")
    private static final BijectionFixture[] ALL_BIJECTIONS = {
        BijectionFixture.of("tinyint", Bijections.INT8_GENERATOR, DataType.int8Type),
        BijectionFixture.of("smallint", Bijections.INT16_GENERATOR, DataType.int16Type),
        BijectionFixture.of("int", Bijections.INT32_GENERATOR, DataType.int32Type),
        BijectionFixture.of("bigint", Bijections.INT64_GENERATOR, DataType.int64Type),
        BijectionFixture.of("boolean", Bijections.BOOLEAN_GENERATOR, DataType.booleanType),
        BijectionFixture.of("float", Bijections.FLOAT_GENERATOR, DataType.floatType),
        BijectionFixture.of("double", Bijections.DOUBLE_GENERATOR, DataType.doubleType),
        BijectionFixture.of("timestamp", Bijections.TIMESTAMP_GENERATOR, DataType.timestampType),
        BijectionFixture.of("blob", Bijections.BLOB_GENERATOR, DataType.blobType),
        BijectionFixture.of("time", Bijections.TIME_GENERATOR, DataType.timeType),
        BijectionFixture.of("varint", Bijections.VARINT_GENERATOR, DataType.varintType),
        BijectionFixture.of("decimal", Bijections.DECIMAL_GENERATOR, DataType.decimalType),
        BijectionFixture.of("inet", Bijections.INET_GENERATOR, DataType.inetType),
        new BijectionFixture<>("uuid", Bijections.UUID_GENERATOR, CASSANDRA_UUID_CMP),
        new BijectionFixture<>("timeuuid", Bijections.TIME_UUID_GENERATOR, CASSANDRA_TIMEUUID_CMP),
    };

    // TODO (now): negative tests
}
