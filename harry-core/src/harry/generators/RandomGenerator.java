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

import harry.core.VisibleForTesting;

/**
 * Random generator interface that offers:
 * * Settable seed
 * * Ability to generate multiple "next" random seeds
 * * Ability to generate multiple "dependent" seeds, from which we can retrace the base seed with subtraction
 */
public interface RandomGenerator
{

    long next();

    default long[] next(int n)
    {
        long[] next = new long[n];
        for (int i = 0; i < n; i++)
            next[i] = next();
        return next;
    }

    void seed(long seed);

    void seek(long step);

    default int nextInt()
    {
        return RngUtils.asInt(next());
    }

    default int nextInt(int max)
    {
        return RngUtils.asInt(next(), max);
    }

    default int nextInt(int min, int max)
    {
        return RngUtils.asInt(next(), min, max);
    }

    default boolean nextBoolean()
    {
        return RngUtils.asBoolean(next());
    }


    default long nextAt(long step)
    {
        seek(step);
        return next();
    }

    @VisibleForTesting
    public static RandomGenerator forTests()
    {
        return forTests(System.currentTimeMillis());
    }

    @VisibleForTesting
    public static RandomGenerator forTests(long seed)
    {
        System.out.println("Seed: " + seed);
        return new PcgRSUFast(seed, 1);
    }
}

