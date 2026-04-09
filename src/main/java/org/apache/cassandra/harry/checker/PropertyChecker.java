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

package org.apache.cassandra.harry.checker;

import java.lang.ref.Cleaner;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import com.google.errorprone.annotations.CheckReturnValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;

@CheckReturnValue
public class PropertyChecker
{
    private static final Logger logger = LoggerFactory.getLogger(PropertyChecker.class);
    private static final int DEFAULT_RUNS = 100;

    // --- quick check (no generators, raw EntropySource) ---

    public static Quick quick()
    {
        return new Quick();
    }

    // --- generator-based entry points ---

    public static <T> Stateless<T> forAll(Generator<T> gen)
    {
        return new Stateless<>(gen);
    }

    public static <T1, T2> Stateless2<T1, T2> forAll(Generator<T1> g1, Generator<T2> g2)
    {
        return new Stateless2<>(g1, g2);
    }

    public static <T1, T2, T3> Stateless3<T1, T2, T3> forAll(Generator<T1> g1, Generator<T2> g2, Generator<T3> g3)
    {
        return new Stateless3<>(g1, g2, g3);
    }

    // --- stateful entry points ---

    public static <STATE> Stateful<STATE> stateful(STATE initial)
    {
        return new Stateful<>(initial);
    }

    public static <STATE, SUT> StatefulWithSut<STATE, SUT> stateful(STATE initial, SUT sut)
    {
        return new StatefulWithSut<>(initial, sut);
    }

    // -----------------------------------------------------------------------
    // Config base
    // -----------------------------------------------------------------------

    public abstract static class Config<SELF extends Config<SELF>>
    {
        private static final Cleaner CLEANER = Cleaner.create();

        int runs = DEFAULT_RUNS;
        long seed = -1;
        int startRun = 0;
        private final LeakDetector leak;

        Config()
        {
            this.leak = new LeakDetector();
            CLEANER.register(this, leak);
        }

        @SuppressWarnings("unchecked")
        public SELF withRuns(int n)
        {
            this.runs = n;
            return (SELF) this;
        }

        @SuppressWarnings("unchecked")
        public SELF withSeed(long seed)
        {
            this.seed = seed;
            return (SELF) this;
        }

        /**
         * Skip to a specific 1-based run number, fast-forwarding the RNG
         * past all prior runs without executing the property.
         * Useful for reproducing a failure like "failed on run 10645"
         * without waiting through the first 10644 iterations.
         *
         * Only supported for stateless (Quick, Stateless, Stateless2, Stateless3) checks.
         * Stateful checks ignore this setting since each step depends on prior state.
         */
        @SuppressWarnings("unchecked")
        public SELF skipToRun(int run)
        {
            if (run < 1)
                throw new IllegalArgumentException("Run number must be >= 1, got " + run);
            this.startRun = run - 1; // convert 1-based to 0-based
            return (SELF) this;
        }

        protected void markChecked()
        {
            leak.checked = true;
        }

        protected long effectiveSeed()
        {
            return seed == -1 ? System.nanoTime() : seed;
        }

        private static class LeakDetector implements Runnable
        {
            volatile boolean checked = false;
            final Throwable allocationSite = new Throwable("PropertyChecker builder allocated here");

            public void run()
            {
                if (!checked)
                    logger.error("PropertyChecker builder was created but check() was never called", allocationSite);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Quick (raw EntropySource, no generators)
    // -----------------------------------------------------------------------

    public static class Quick extends Config<Quick>
    {
        public void check(ThrowingConsumer<EntropySource> property) throws Throwable
        {
            markChecked();
            long effectiveSeed = effectiveSeed();
            logger.info("PropertyChecker seed: {}L", effectiveSeed);
            EntropySource rng = new JdkRandomEntropySource(effectiveSeed);
            for (int i = 0; i < runs; i++)
            {
                try
                {
                    EntropySource es = rng.derive();
                    if (i < startRun)
                        continue;

                    if (i > 0 && i == startRun)
                        logger.info("Skipped to run {}", i);
                    property.accept(es);
                }
                catch (Throwable t)
                {
                    throw new AssertionError(
                        String.format("Property failed at seed:%dL on run %d of %d", effectiveSeed, i + 1, runs), t);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Stateless (single generator)
    // -----------------------------------------------------------------------

    public static class Stateless<T> extends Config<Stateless<T>>
    {
        private final Generator<T> gen;

        Stateless(Generator<T> gen)
        {
            this.gen = gen;
        }

        public void check(ThrowingConsumer<T> property) throws Throwable
        {
            markChecked();
            long effectiveSeed = effectiveSeed();
            logger.info("PropertyChecker seed: {}L", effectiveSeed);
            EntropySource rng = new JdkRandomEntropySource(effectiveSeed);
            for (int i = 0; i < runs; i++)
            {
                T value = gen.generate(rng);
                try
                {
                    if (i < startRun)
                        continue;

                    if (i == startRun && startRun > 0)
                        logger.info("Skipped to run {}", i);
                    property.accept(value);
                }
                catch (Throwable t)
                {
                    throw new AssertionError(
                        String.format("Property failed at seed:%dL on run %d of %d, value: %s",
                                      effectiveSeed, i + 1, runs, value), t);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Stateless2 (two generators)
    // -----------------------------------------------------------------------

    public static class Stateless2<T1, T2> extends Config<Stateless2<T1, T2>>
    {
        private final Generator<T1> g1;
        private final Generator<T2> g2;

        Stateless2(Generator<T1> g1, Generator<T2> g2)
        {
            this.g1 = g1;
            this.g2 = g2;
        }

        public void check(ThrowingBiConsumer<T1, T2> property) throws Throwable
        {
            markChecked();
            long effectiveSeed = effectiveSeed();
            logger.info("PropertyChecker seed: {}L", effectiveSeed);
            EntropySource rng = new JdkRandomEntropySource(effectiveSeed);
            for (int i = 0; i < runs; i++)
            {
                T1 v1 = g1.generate(rng);
                T2 v2 = g2.generate(rng);
                try
                {
                    if (i < startRun)
                        continue;

                    if (i == startRun && startRun > 0)
                        logger.info("Skipped to run {}", i);
                    property.accept(v1, v2);
                }
                catch (Throwable t)
                {
                    throw new AssertionError(
                        String.format("Property failed at seed:%dL on run %d of %d, values: (%s, %s)",
                                      effectiveSeed, i + 1, runs, v1, v2), t);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Stateless3 (three generators)
    // -----------------------------------------------------------------------

    public static class Stateless3<T1, T2, T3> extends Config<Stateless3<T1, T2, T3>>
    {
        private final Generator<T1> g1;
        private final Generator<T2> g2;
        private final Generator<T3> g3;

        Stateless3(Generator<T1> g1, Generator<T2> g2, Generator<T3> g3)
        {
            this.g1 = g1;
            this.g2 = g2;
            this.g3 = g3;
        }

        public void check(ThrowingTriConsumer<T1, T2, T3> property) throws Throwable
        {
            markChecked();
            long effectiveSeed = effectiveSeed();
            logger.info("PropertyChecker seed: {}L", effectiveSeed);
            EntropySource rng = new JdkRandomEntropySource(effectiveSeed);
            for (int i = 0; i < runs; i++)
            {
                T1 v1 = g1.generate(rng);
                T2 v2 = g2.generate(rng);
                T3 v3 = g3.generate(rng);
                try
                {
                    if (i < startRun)
                        continue;

                    if (i == startRun && startRun > 0)
                        logger.info("Skipped to run {}", startRun + 1);
                    property.accept(v1, v2, v3);
                }
                catch (Throwable t)
                {
                    throw new AssertionError(
                        String.format("Property failed at seed:%dL on run %d of %d, values: (%s, %s, %s)",
                                      effectiveSeed, i + 1, runs, v1, v2, v3), t);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Stateful (state-only, no SUT)
    // -----------------------------------------------------------------------

    public static class Stateful<STATE> extends Config<Stateful<STATE>>
    {
        private final STATE initial;
        private final List<ModelChecker.ThrowingConsumer<ModelChecker<STATE, Void>.Simple>> configurators = new ArrayList<>();

        Stateful(STATE initial)
        {
            this.initial = initial;
        }

        public Stateful<STATE> step(ModelChecker.ThrowingFunction<STATE, STATE> step)
        {
            configurators.add(simple -> simple.step(step));
            return this;
        }

        public Stateful<STATE> step(ModelChecker.ThrowingConsumer<STATE> step)
        {
            configurators.add(simple -> simple.step(step));
            return this;
        }

        public Stateful<STATE> step(ModelChecker.ThrowingBiConsumer<STATE, EntropySource> step)
        {
            configurators.add(simple -> simple.step(step));
            return this;
        }

        public Stateful<STATE> step(Predicate<STATE> precondition, java.util.function.Consumer<STATE> step)
        {
            configurators.add(simple -> simple.step(precondition, step));
            return this;
        }

        public Stateful<STATE> invariant(Predicate<STATE> invariant)
        {
            configurators.add(simple -> simple.invariant(invariant));
            return this;
        }

        public void check() throws Throwable
        {
            markChecked();
            long effectiveSeed = effectiveSeed();
            logger.info("PropertyChecker seed: {}L", effectiveSeed);
            EntropySource rng = new JdkRandomEntropySource(effectiveSeed);

            ModelChecker<STATE, Void> mc = new ModelChecker<>();
            ModelChecker<STATE, Void>.Simple simple = mc.init(initial);
            simple.exitCondition(state -> false);
            for (ModelChecker.ThrowingConsumer<ModelChecker<STATE, Void>.Simple> c : configurators)
                c.accept(simple);

            try
            {
                simple.run(0, runs, rng);
            }
            catch (Throwable t)
            {
                throw new AssertionError(
                    String.format("Stateful property failed at seed:%dL after up to %d steps", effectiveSeed, runs), t);
            }
        }
    }

    // -----------------------------------------------------------------------
    // StatefulWithSut (model + SUT)
    // -----------------------------------------------------------------------

    public static class StatefulWithSut<STATE, SUT> extends Config<StatefulWithSut<STATE, SUT>>
    {
        private final STATE initialState;
        private final SUT initialSut;
        private final List<ModelChecker.ThrowingConsumer<ModelChecker<STATE, SUT>>> configurators = new ArrayList<>();

        StatefulWithSut(STATE initialState, SUT initialSut)
        {
            this.initialState = initialState;
            this.initialSut = initialSut;
        }

        public StatefulWithSut<STATE, SUT> step(ModelChecker.Step<STATE, SUT> step)
        {
            configurators.add(mc -> mc.step(step));
            return this;
        }

        public StatefulWithSut<STATE, SUT> step(ModelChecker.Precondition<STATE, SUT> precondition,
                                                 ModelChecker.Step<STATE, SUT> step)
        {
            configurators.add(mc -> mc.step(precondition, step));
            return this;
        }

        public StatefulWithSut<STATE, SUT> invariant(ModelChecker.Precondition<STATE, SUT> invariant)
        {
            configurators.add(mc -> mc.invariant(invariant));
            return this;
        }

        public StatefulWithSut<STATE, SUT> beforeAll(ModelChecker.Step<STATE, SUT> beforeAll)
        {
            configurators.add(mc -> mc.beforeAll(beforeAll));
            return this;
        }

        public StatefulWithSut<STATE, SUT> afterAll(ModelChecker.Step<STATE, SUT> afterAll)
        {
            configurators.add(mc -> mc.afterAll(afterAll));
            return this;
        }

        public void check() throws Throwable
        {
            markChecked();
            long effectiveSeed = effectiveSeed();
            logger.info("PropertyChecker seed: {}L", effectiveSeed);
            EntropySource rng = new JdkRandomEntropySource(effectiveSeed);

            ModelChecker<STATE, SUT> mc = new ModelChecker<>();
            mc.init(initialState, initialSut);
            mc.exitCondition((state, sut) -> false);
            for (ModelChecker.ThrowingConsumer<ModelChecker<STATE, SUT>> c : configurators)
                c.accept(mc);

            try
            {
                mc.run(0, runs, rng);
            }
            catch (Throwable t)
            {
                throw new AssertionError(
                    String.format("Stateful property failed at seed:%dL after up to %d steps", effectiveSeed, runs), t);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Functional interfaces
    // -----------------------------------------------------------------------

    public interface ThrowingConsumer<T>
    {
        void accept(T t) throws Throwable;
    }

    public interface ThrowingBiConsumer<T1, T2>
    {
        void accept(T1 t1, T2 t2) throws Throwable;
    }

    public interface ThrowingTriConsumer<T1, T2, T3>
    {
        void accept(T1 t1, T2 t2, T3 t3) throws Throwable;
    }

    public interface ThrowingFunction<I, O>
    {
        O apply(I input) throws Throwable;
    }
}
