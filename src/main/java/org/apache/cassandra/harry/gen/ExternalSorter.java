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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import accord.utils.Invariants;
import org.agrona.collections.IntHashSet;
import org.apache.cassandra.harry.MagicConstants;
import org.apache.cassandra.harry.gen.rng.SeedableEntropySource;
import org.apache.cassandra.utils.ArrayUtils;

/**
 * External merge sort for producing a large sorted descriptor file.
 *
 * <p>Phase 1 — Chunk generation: draw descriptors from the entropy source in batches, deduplicate
 * within each batch using a hash set, sort each batch by inflated value, and write to a temp file.
 *
 * <p>Phase 2 — K-way merge: open all sorted chunk files as streams and merge them into the final
 * output using a min-heap. Cross-chunk duplicates (same inflated value) are dropped.
 *
 * <p>Temp chunk files are deleted after the merge completes.
 */
class ExternalSorter<T>
{
    static final int DEFAULT_CHUNK_SIZE_LONGS = 32_000_000; // 32M longs = 256 MB

    /**
     * Generates up to {@code population} descriptors (capped by {@code typeEntropy}), sorts them
     * by inflated value, and writes the result to {@code dataFile}.
     */
    void sort(EntropySource rng, long population, long typeEntropy,
              Generator<T> gen, Comparator<T> comparator,
              Path dataFile, int chunkSizeLongs) throws IOException
    {
        if (Long.compareUnsigned(typeEntropy, Integer.MAX_VALUE) > 0)
            typeEntropy = Integer.MAX_VALUE;
        population = Math.min(typeEntropy, population);

        Path dir = dataFile.getParent();
        if (dir == null) dir = Path.of(".");

        List<Path> chunkFiles = new ArrayList<>();
        long remaining = population;

        // Phase 1: generate sorted chunks
        while (remaining > 0)
        {
            int chunkTarget = (int) Math.min(chunkSizeLongs, remaining);
            Path chunkFile = Files.createTempFile(dir, "harry-chunk-", ".tmp");
            chunkFiles.add(chunkFile);
            writeChunk(rng, chunkTarget, gen, comparator, chunkFile);
            remaining -= chunkTarget;
        }

        // Phase 2: merge into a temp output, then atomically rename to final path
        Path tmpOut = Files.createTempFile(dir, "harry-merge-", ".tmp");
        try
        {
            mergeChunks(chunkFiles, gen, comparator, tmpOut);
            Files.move(tmpOut, dataFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        }
        finally
        {
            // Phase 3: clean up chunk temp files; tmpOut is either renamed or left for deleteIfExists
            for (Path f : chunkFiles)
                Files.deleteIfExists(f);
            Files.deleteIfExists(tmpOut);
        }
    }

    private void writeChunk(EntropySource rng, int target,
                            Generator<T> gen, Comparator<T> comparator,
                            Path file) throws IOException
    {
        // TODO: switc
        List<Long> chunk = new ArrayList<>(target);
        IntHashSet hashes = new IntHashSet(target);

        while (chunk.size() < target)
        {
            long candidate = rng.next();
            if (MagicConstants.MAGIC_DESCRIPTOR_VALS.contains(candidate))
                continue;

            Object inflated = inflate(candidate, gen);
            int hash = ArrayUtils.hashCode(inflated);
            if (hashes.add(hash))
                chunk.add(candidate);
        }

        chunk.sort((a, b) -> comparator.compare(inflate(a, gen), inflate(b, gen)));

        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
        {
            ByteBuffer buf = ByteBuffer.allocateDirect(Math.min(chunk.size(), 4096) * 8)
                                       .order(ByteOrder.LITTLE_ENDIAN);
            for (long descriptor : chunk)
            {
                if (!buf.hasRemaining())
                {
                    buf.flip();
                    ch.write(buf);
                    buf.clear();
                }
                buf.putLong(descriptor);
            }
            if (buf.position() > 0)
            {
                buf.flip();
                ch.write(buf);
            }
        }
    }

    private void mergeChunks(List<Path> chunkFiles, Generator<T> gen, Comparator<T> comparator, Path output) throws IOException
    {
        class InputFile implements AutoCloseable
        {
            final FileChannel channel;
            final long size;
            long position;
            long headDescriptor;
            T headValue;

            InputFile(Path path) throws IOException
            {
                this.channel = FileChannel.open(path, StandardOpenOption.READ);
                this.size = channel.size();
            }

            boolean advance() throws IOException
            {
                if (position + 8 > size)
                    return false;
                ByteBuffer buf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
                channel.read(buf, position);
                buf.flip();
                headDescriptor = buf.getLong();
                headValue = inflate(headDescriptor, gen);
                position += 8;
                return true;
            }

            @Override
            public void close() throws IOException
            {
                channel.close();
            }
        }

        List<InputFile> inputs = new ArrayList<>(chunkFiles.size());
        try
        {
            for (Path f : chunkFiles)
                inputs.add(new InputFile(f));

            PriorityQueue<InputFile> heap = new PriorityQueue<>(Math.max(inputs.size(), 1),
                                                                (a, b) -> comparator.compare(a.headValue, b.headValue));

            for (InputFile input : inputs)
            {
                if (input.advance())
                    heap.add(input);
            }

            try (FileChannel outCh = FileChannel.open(output, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING))
            {
                ByteBuffer writeBuf = ByteBuffer.allocateDirect(4096 * 8).order(ByteOrder.LITTLE_ENDIAN);
                T prevValue = null;

                while (!heap.isEmpty())
                {
                    InputFile input = heap.poll();

                    Invariants.require(prevValue == null || comparator.compare(input.headValue, prevValue) > 0,
                                       "Merge produced duplicate or out-of-order value: %s", input.headValue);
                    if (!writeBuf.hasRemaining())
                    {
                        writeBuf.flip();
                        outCh.write(writeBuf);
                        writeBuf.clear();
                    }
                    writeBuf.putLong(input.headDescriptor);
                    prevValue = input.headValue;

                    if (input.advance())
                        heap.add(input);
                }

                if (writeBuf.position() > 0)
                {
                    writeBuf.flip();
                    outCh.write(writeBuf);
                }
            }
        }
        finally
        {
            for (InputFile input : inputs)
            {
                try
                {
                    input.close();
                }
                catch (IOException ignored)
                {
                    throw new UncheckedIOException(ignored);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private T inflate(long descriptor, Generator<T> gen)
    {
        return (T) SeedableEntropySource.computeWithSeed(descriptor, gen::generate);
    }
}
