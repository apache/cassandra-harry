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

package harry;

import harry.generators.Generator;
import harry.generators.RandomGenerator;
import org.quicktheories.core.Gen;
import org.quicktheories.core.RandomnessSource;
import org.quicktheories.impl.Constraint;

public class QuickTheoriesAdapter
{
    public static <T> Gen<T> convert(Generator<T> generator)
    {
        return new Gen<T>()
        {
            private final RandomnessSourceAdapter<T> adapter = new RandomnessSourceAdapter<>();

            public T generate(RandomnessSource randomnessSource)
            {
                return adapter.generate(randomnessSource, generator);
            }
        };
    }

    public static class RandomnessSourceAdapter<T> implements RandomGenerator
    {
        private RandomnessSource rnd;

        public long next()
        {
            return rnd.next(Constraint.none());
        }

        public void seed(long seed)
        {
            throw new RuntimeException("Seed is not settable");
        }

        public void seek(long step)
        {
            throw new RuntimeException("Seed is not settable");
        }

        public T generate(RandomnessSource rnd, Generator<T> generate)
        {
            this.rnd = rnd;
            T value = generate.generate(this);
            this.rnd = null;
            return value;
        }
    }
}