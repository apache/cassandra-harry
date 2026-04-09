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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.harry.gen.rng.PureRng;
import org.apache.cassandra.harry.gen.rng.Sequence;

public class SequenceTest
{
    private static final long SEED = 42L;
    private static final int SIZE = 100;

    @Test
    public void valueAtMatchesPureRng()
    {
        long stream = 7;
        PureRng rng = new PureRng.PCGFast(SEED);
        Sequence seq = new Sequence(SEED, stream);

        for (int i = 0; i < SIZE; i++)
            Assert.assertEquals(rng.randomNumber(i, stream), seq.valueAt(i));
    }

    @Test
    public void indexOfRoundtripsWithValueAt()
    {
        Sequence seq = new Sequence(SEED, 3);
        for (int i = 0; i < SIZE; i++)
        {
            long value = seq.valueAt(i);
            Assert.assertEquals("indexOf(valueAt(" + i + ")) should be " + i, i, seq.indexOf(value));
        }
    }

    @Test
    public void nextAndPrevAreInverses()
    {
        Sequence seq = new Sequence(SEED, 5);
        for (int i = 0; i < SIZE; i++)
        {
            long value = seq.valueAt(i);
            long nextValue = seq.next(value);
            Assert.assertEquals("prev(next(v)) should equal v", value, seq.prev(nextValue));
        }
    }

    @Test
    public void nextMatchesValueAtSuccessor()
    {
        Sequence seq = new Sequence(SEED, 2);
        for (int i = 0; i < SIZE - 1; i++)
        {
            long current = seq.valueAt(i);
            long next = seq.next(current);
            Assert.assertEquals("next(valueAt(i)) should equal valueAt(i+1)",
                                seq.valueAt(i + 1), next);
        }
    }

    @Test
    public void distanceIsConsistent()
    {
        Sequence seq = new Sequence(SEED, 4);
        for (int gap = 1; gap <= 10; gap++)
        {
            long a = seq.valueAt(0);
            long b = seq.valueAt(gap);
            Assert.assertEquals("distance(valueAt(0), valueAt(" + gap + ")) should be " + gap,
                                gap, seq.distance(a, b));
        }
    }

    @Test
    public void forkProducesIndependentSequence()
    {
        Sequence parent = new Sequence(SEED, 0);
        Sequence child = parent.fork(5);

        // Child should produce different values than parent
        // (different seed and stream)
        boolean anyDifferent = false;
        for (int i = 0; i < SIZE; i++)
        {
            if (parent.valueAt(i) != child.valueAt(i))
            {
                anyDifferent = true;
                break;
            }
        }
        Assert.assertTrue("Forked sequence should differ from parent", anyDifferent);

        // Child should still be internally consistent
        for (int i = 0; i < SIZE; i++)
            Assert.assertEquals(i, child.indexOf(child.valueAt(i)));
    }

    @Test
    public void forkIsDeterministic()
    {
        Sequence parent = new Sequence(SEED, 0);
        Sequence fork1 = parent.fork(3);
        Sequence fork2 = parent.fork(3);

        for (int i = 0; i < SIZE; i++)
            Assert.assertEquals(fork1.valueAt(i), fork2.valueAt(i));
    }

    @Test
    public void toEntropySourceMatchesValueAt()
    {
        Sequence seq = new Sequence(SEED, 3);
        EntropySource es = seq.toEntropySource();

        // EntropySource.next() advances then reads (PCG convention),
        // so the first next() should return valueAt(1)
        for (int i = 1; i <= SIZE; i++)
            Assert.assertEquals("next() call " + i + " should match valueAt(" + i + ")",
                                seq.valueAt(i), es.next());
    }

    @Test
    public void differentStreamsProduceDifferentSequences()
    {
        Sequence s1 = new Sequence(SEED, 0);
        Sequence s2 = new Sequence(SEED, 1);

        boolean anyDifferent = false;
        for (int i = 0; i < SIZE; i++)
        {
            if (s1.valueAt(i) != s2.valueAt(i))
            {
                anyDifferent = true;
                break;
            }
        }
        Assert.assertTrue("Different streams should produce different values", anyDifferent);
    }

    @Test
    public void defaultStreamIsZero()
    {
        Sequence withDefault = new Sequence(SEED);
        Sequence withExplicit = new Sequence(SEED, 0);

        for (int i = 0; i < SIZE; i++)
            Assert.assertEquals(withDefault.valueAt(i), withExplicit.valueAt(i));
    }
}
