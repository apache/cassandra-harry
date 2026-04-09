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

import java.util.Arrays;

/**
 * A bijection from a single flat index to a composite primary key tuple and back.
 * Uses mixed-radix positional numbering: each PK column is a "digit" with its own
 * base (the column's population). The {@code strides} array holds the positional
 * value for each column -- changing column i by 1 moves the flat index by strides[i].
 * <p>
 * The enumeration is lexicographic: the first column is most significant. This matches
 * SQL composite key ordering: (0, 3) sorts before (1, 0).
 */
public class CompositePkBijection
{
    private final Bijections.IndexedBijection<?>[] columns;
    private final long[] populations;
    private final long[] strides;
    private final long totalPopulation;

    public CompositePkBijection(Bijections.IndexedBijection<?>... columns)
    {
        if (columns.length == 0)
            throw new IllegalArgumentException("At least one column required");

        this.columns = columns;
        this.populations = new long[columns.length];
        this.strides = new long[columns.length];

        long total = 1;
        for (int i = columns.length - 1; i >= 0; i--)
        {
            populations[i] = columns[i].population();
            strides[i] = total;
            long prev = total;
            total *= populations[i];
            if (populations[i] != 0 && total / populations[i] != prev)
                throw new IllegalArgumentException("Total population overflows long");
        }
        this.totalPopulation = total;
    }

    /**
     * Decompose a flat index into per-column indices.
     */
    public long[] inflate(long flatIdx)
    {
        long[] perColumn = new long[columns.length];
        long remainder = flatIdx;
        for (int i = 0; i < columns.length; i++)
        {
            perColumn[i] = remainder / strides[i];
            remainder = remainder % strides[i];
        }
        return perColumn;
    }

    /**
     * Combine per-column indices into a flat index.
     */
    public long deflate(long[] perColumnIndices)
    {
        long flat = 0;
        for (int i = 0; i < columns.length; i++)
            flat += perColumnIndices[i] * strides[i];
        return flat;
    }

    /**
     * Inflate a flat index all the way to typed values.
     */
    public Object[] inflateValues(long flatIdx)
    {
        long[] indices = inflate(flatIdx);
        Object[] values = new Object[columns.length];
        for (int i = 0; i < columns.length; i++)
        {
            long descriptor = columns[i].descriptorAt(indices[i]);
            values[i] = columns[i].inflate(descriptor);
        }
        return values;
    }

    /**
     * Deflate typed values back to a flat index by going through each
     * column's sub-bijection.
     */
    @SuppressWarnings("unchecked")
    public long deflateFromValues(Object[] values)
    {
        long flat = 0;
        for (int i = 0; i < columns.length; i++)
        {
            Bijections.IndexedBijection<Object> bij = (Bijections.IndexedBijection<Object>) columns[i];
            long descriptor = bij.deflate(values[i]);
            long idx = bij.idxFor(descriptor);
            flat += idx * strides[i];
        }
        return flat;
    }

    public long totalPopulation()
    {
        return totalPopulation;
    }

    public int width()
    {
        return columns.length;
    }

    public Bijections.IndexedBijection<?> column(int i)
    {
        return columns[i];
    }

    @Override
    public String toString()
    {
        return String.format("CompositePkBijection{populations=%s, totalPopulation=%d}",
                             Arrays.toString(populations), totalPopulation);
    }
}
