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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;

import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;
import org.apache.cassandra.harry.gen.rng.SeedableEntropySource;

/**
 * A disk-backed alternative to {@link InvertibleGenerator} for populations too large to fit in heap
 * (e.g. billions of descriptors).
 * <p>
 * The sorted descriptor sequence lives in a single {@code .data} file on disk as a flat sequence of
 * 8-byte little-endian longs. A configurable sparse in-memory index (one entry per {@code indexStride}
 * descriptors, kept as both raw descriptors and their inflated values) allows deflation in
 * O(log(N/stride) + log(stride)) time regardless of N.
 * <p>
 * Files are identified by {@code (baseDir, generatorKey)} and reused across runs if they already exist,
 * so the expensive sort is paid only once per unique (seed, population, generator-type) combination.
 *
 * @param <T> the value type produced by the underlying generator
 */
public class DiskBackedInvertibleGenerator<T> implements Bijections.IndexedBijection<T>, Closeable
{
    public static final int DEFAULT_INDEX_STRIDE = 1024;

    private final Generator<T> gen;
    private final Comparator<T> comparator;
    private final int indexStride;
    private final long totalCount;

    // Sparse index: indexDescriptors[k] is the descriptor at position k*indexStride
    private final long[] indexDescriptors;
    // Pre-inflated values — binary search of the index phase never inflates
    private final T[] indexValues;

    // Read-only channel kept open for the lifetime of this generator
    private final FileChannel channel;

    @SuppressWarnings("unchecked")
    private DiskBackedInvertibleGenerator(FileChannel channel, long totalCount,
                                          Generator<T> gen, Comparator<T> comparator,
                                          int indexStride) throws IOException
    {
        this.gen = gen;
        this.comparator = comparator;
        this.indexStride = indexStride;
        this.totalCount = totalCount;
        this.channel = channel;

        int indexSize = (int) ((totalCount + indexStride - 1) / indexStride);
        this.indexDescriptors = new long[indexSize];
        this.indexValues = (T[]) new Object[indexSize];

        ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        for (int k = 0; k < indexSize; k++)
        {
            long byteOffset = (long) k * indexStride * 8;
            buf.clear();
            channel.read(buf, byteOffset);
            buf.flip();
            long descriptor = buf.getLong();
            indexDescriptors[k] = descriptor;
            indexValues[k] = inflate(descriptor);
        }
    }

    /**
     * Opens (or creates) the generator for the given parameters.
     *
     * <p>If the data file does not exist it is built via an external merge sort and then persisted.
     * Subsequent calls with the same key reuse the existing file without re-sorting.
     *
     * @param baseDir     directory in which the data file is stored
     * @param seed        entropy seed used during generation
     * @param population  desired number of distinct descriptors
     * @param indexStride one sparse-index entry per this many descriptors (default {@link #DEFAULT_INDEX_STRIDE})
     * @param gen         generator for inflating descriptors to values
     * @param comparator  value comparator (determines sort order)
     * @param typeEntropy entropy of the type (caps effective population)
     */
    public static <T> DiskBackedInvertibleGenerator<T> open(Path baseDir,
                                                             long seed,
                                                             long population,
                                                             int indexStride,
                                                             Generator<T> gen,
                                                             Comparator<T> comparator,
                                                             long typeEntropy) throws IOException
    {
        if (Long.compareUnsigned(typeEntropy, Integer.MAX_VALUE) > 0)
            typeEntropy = Integer.MAX_VALUE;
        population = Math.min(typeEntropy, population);

        Files.createDirectories(baseDir);
        Path dataFile = baseDir.resolve(fileKey(gen, seed, population));

        if (!Files.exists(dataFile))
        {
            EntropySource rng = new JdkRandomEntropySource(seed);
            new ExternalSorter<T>().sort(rng, population, typeEntropy, gen, comparator, dataFile,
                                         ExternalSorter.DEFAULT_CHUNK_SIZE_LONGS);
        }

        long actualCount = Files.size(dataFile) / 8;
        FileChannel channel = FileChannel.open(dataFile, StandardOpenOption.READ);
        return new DiskBackedInvertibleGenerator<>(channel, actualCount, gen, comparator, indexStride);
    }

    // -------------------------------------------------------------------------
    // IndexedBijection implementation
    // -------------------------------------------------------------------------

    @Override
    public T inflate(long descriptor)
    {
        return SeedableEntropySource.computeWithSeed(descriptor, gen::generate);
    }

    @Override
    public long deflate(T value)
    {
        return readDescriptorAt(findPosition(value));
    }

    @Override
    public long idxFor(long descriptor)
    {
        return findPosition(inflate(descriptor));
    }

    @Override
    public long descriptorAt(long idx)
    {
        return readDescriptorAt(idx);
    }

    @Override
    public long population()
    {
        return totalCount;
    }

    @Override
    public int byteSize()
    {
        return Long.BYTES;
    }

    @Override
    public int compare(long d1, long d2)
    {
        if (d1 == d2) return 0;
        return comparator.compare(inflate(d1), inflate(d2));
    }

    @Override
    public Comparator<Long> descriptorsComparator()
    {
        return (d1, d2) -> Long.compare(idxFor(d1), idxFor(d2));
    }

    // -------------------------------------------------------------------------
    // Closeable
    // -------------------------------------------------------------------------

    @Override
    public void close() throws IOException
    {
        channel.close();
    }

    // -------------------------------------------------------------------------
    // Internal helpers
    // -------------------------------------------------------------------------

    /**
     * Binary-searches for {@code value} in the sorted data file.
     *
     * <p>Phase 1: find the sparse-index window using pre-inflated {@code indexValues} — no inflation,
     * no disk I/O.
     * Phase 2: read that window from disk, binary-search with inflation — at most log2(indexStride)
     * inflations.
     *
     * @return absolute position (0-based) of the descriptor for {@code value}
     * @throws IllegalStateException if the value is not found
     */
    private long findPosition(T value)
    {
        // Phase 1: find largest k such that indexValues[k] <= value
        int lo = 0, hi = indexValues.length - 1, k = 0;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            int cmp = comparator.compare(indexValues[mid], value);
            if (cmp <= 0)
            {
                k = mid;
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        // Phase 2: read window [k*stride, min((k+1)*stride, totalCount)) from disk
        long windowStart = (long) k * indexStride;
        long windowEnd = Math.min((long) (k + 1) * indexStride, totalCount);
        int windowSize = (int) (windowEnd - windowStart);

        ByteBuffer buf = ByteBuffer.allocate(windowSize * 8).order(ByteOrder.LITTLE_ENDIAN);
        try
        {
            channel.read(buf, windowStart * 8);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        buf.flip();

        lo = 0; hi = windowSize - 1;
        while (lo <= hi)
        {
            int mid = (lo + hi) >>> 1;
            long d = buf.getLong(mid * 8);
            int cmp = comparator.compare(inflate(d), value);
            if (cmp < 0)      lo = mid + 1;
            else if (cmp > 0) hi = mid - 1;
            else              return windowStart + mid;
        }

        throw new IllegalStateException("Value not found in data file: " + value);
    }

    private long readDescriptorAt(long idx)
    {
        ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        try
        {
            channel.read(buf, idx * 8);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        buf.flip();
        return buf.getLong();
    }

    /**
     * Produces a filesystem-safe filename for the given generator, seed and population.
     * Non-alphanumeric characters in the generator's {@code toString()} are replaced with {@code _}.
     */
    static String fileKey(Generator<?> gen, long seed, long population)
    {
        String genStr = gen.toString().replaceAll("[^A-Za-z0-9._-]", "_");
        return genStr + "-s" + seed + "-p" + population + ".data";
    }
}
