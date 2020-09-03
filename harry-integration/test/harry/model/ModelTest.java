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

package harry.model;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import harry.core.Configuration;
import harry.ddl.SchemaSpec;
import harry.generators.Surjections;
import harry.generators.distribution.Distribution;
import harry.model.sut.InJvmSut;
import harry.runner.Runner;
import harry.util.BitSet;
import org.apache.cassandra.distributed.Cluster;

// TODO: split tests into concurrent and sequential
public class ModelTest extends TestBaseImpl
{
    static
    {
        KEYSPACE = MockSchema.KEYSPACE;
    }

    private final static Surjections.Surjection<BitSet> columnMaskSelector = MockSchema.columnMaskSelector1;
    private final static SchemaSpec schema = MockSchema.tbl1;
    private final static int cycles = 1000;

    @Test
    public void testScale()
    {
        Random rand = new Random();
        for (int cycle = 0; cycle < cycles; cycle++)
        {
            int a = rand.nextInt(100);
            int b = rand.nextInt(100);
            while (a == b)
                b = rand.nextInt(100);

            int min = Math.min(a, b);
            int max = Math.max(a, b);
            long[] cardinality = new long[max - min];
            for (int i = 0; i < 100000; i++)
            {
                long rnd = rand.nextLong();
                long scaled = Distribution.ScaledDistribution.scale(rnd, min, max);
                cardinality[(int) scaled - min]++;
            }

            for (long c : cardinality)
                Assert.assertTrue(c > 0);
        }
    }

    @Test
    public void statelessVisibleRowsCheckerTest() throws Throwable
    {
        visibleRowsCheckerTest(StatelessVisibleRowsChecker::new);
    }

    @Test
    public void statefulVisibleRowsCheckerTest() throws Throwable
    {
        visibleRowsCheckerTest(VisibleRowsChecker::new);
    }

    @Test
    public void exhaustiveCheckerTest() throws Throwable
    {
        visibleRowsCheckerTest(ExhaustiveChecker::new);
    }

    public void visibleRowsCheckerTest(Model.ModelFactory factory) throws Throwable
    {
        try (Cluster cluster = Cluster.create(3))
        {
            Configuration.ConfigurationBuilder configuration = new Configuration.ConfigurationBuilder();
            configuration.setClock(new Configuration.ApproximateMonotonicClockConfiguration((int) TimeUnit.MINUTES.toMillis(2),
                                                                                            1, TimeUnit.SECONDS))
                         .setRunTime(1, TimeUnit.MINUTES)
                         .setRunner(new Configuration.ConcurrentRunnerConfig(2, 2, 2))
                         .setSchemaProvider((seed) -> schema)
                         .setClusteringDescriptorSelector((OpSelectors.Rng rng, SchemaSpec schemaSpec) -> {
                             return new DescriptorSelectorBuilder()
                             .setColumnMaskSelector(columnMaskSelector)
                             .setOperationTypeSelector(Surjections.constant(OpSelectors.OperationKind.WRITE))
                             .make(rng, schemaSpec);
                         })
                         .setPartitionDescriptorSelector(new Configuration.DefaultPDSelectorConfiguration(10, 10))
                         .setCreateSchema(true)
                         .setTruncateTable(false)
                         .setDropSchema(false)
                         .setModel(factory::create)
                         .setSUT(() -> new InJvmSut(cluster));

            Runner runner = configuration.build().createRunner();
            try
            {
                runner.initAndStartAll().get(2, TimeUnit.MINUTES);
            }
            catch (Throwable t)
            {
                throw t;
            }
            finally
            {
                runner.shutdown();
            }

        }
    }

    @Test
    public void descendingIteratorTest()
    {
        VisibleRowsChecker.LongIterator iter = VisibleRowsChecker.descendingIterator(new long[] { 4,2,1,3,2,1,3,4});;
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(4, iter.nextLong());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(3, iter.nextLong());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(2, iter.nextLong());
        Assert.assertTrue(iter.hasNext());
        Assert.assertEquals(1, iter.nextLong());
    }
}

// TODO: test things gradually. First with simple schemas, then with more complex, then with completely random.