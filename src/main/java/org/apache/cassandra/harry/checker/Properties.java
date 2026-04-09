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

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Predicate;

public class Properties
{
    public static <A, B> PropertyChecker.ThrowingConsumer<A> roundtrip(PropertyChecker.ThrowingFunction<A, B> forward,
                                                                       PropertyChecker.ThrowingFunction<B, A> backward)
    {
        return (a) -> {
            B encoded = forward.apply(a);
            A decoded = backward.apply(encoded);
            if (!Objects.equals(a, decoded))
                throw new AssertionError(
                    String.format("roundtrip failed: forward(%s) = %s, backward(%s) = %s",
                                  a, encoded, encoded, decoded));
        };
    }

    public static <T> PropertyChecker.ThrowingConsumer<T> invariant(Predicate<T> predicate, String description)
    {
        return (t) -> {
            if (!predicate.test(t))
                throw new AssertionError(
                    String.format("Invariant violated: %s for value: %s", description, t));
        };
    }

    public static <T> PropertyChecker.ThrowingConsumer<T> idempotent(PropertyChecker.ThrowingFunction<T, T> f)
    {
        return (t) -> {
            T once = f.apply(t);
            T twice = f.apply(once);
            if (!Objects.equals(once, twice))
                throw new AssertionError(
                    String.format("idempotent failed: f(%s) = %s, f(f(%s)) = %s", t, once, t, twice));
        };
    }

    public static <T, R> PropertyChecker.ThrowingBiConsumer<T, T> commutative(BiFunction<T, T, R> f)
    {
        return (a, b) -> {
            R lr = f.apply(a, b);
            R rl = f.apply(b, a);
            if (!Objects.equals(lr, rl))
                throw new AssertionError(
                    String.format("commutativity failed: f(%s,%s)=%s but f(%s,%s)=%s", a, b, lr, b, a, rl));
        };
    }

    public static <T> PropertyChecker.ThrowingConsumer<List<T>> sorted(Comparator<T> cmp)
    {
        return (list) -> {
            for (int i = 1; i < list.size(); i++)
            {
                if (cmp.compare(list.get(i - 1), list.get(i)) > 0)
                    throw new AssertionError(
                        String.format("sorted violated at index %d: %s > %s",
                                      i, list.get(i - 1), list.get(i)));
            }
        };
    }
}
