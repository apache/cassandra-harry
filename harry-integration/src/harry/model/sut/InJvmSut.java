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

package harry.model.sut;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Configuration;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

public class InJvmSut implements SystemUnderTest
{
    public static void init()
    {
        Configuration.registerSubtypes(InJvmSutConfiguration.class);
    }

    private static final Logger logger = LoggerFactory.getLogger(InJvmSut.class);

    // TODO: shut down properly
    private final ExecutorService executor;
    public final Cluster cluster;
    private final AtomicLong cnt = new AtomicLong();
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    public InJvmSut(Cluster cluster)
    {
        this(cluster, 10);
    }

    public InJvmSut(Cluster cluster, int threads)
    {
        this.cluster = cluster;
        this.executor = Executors.newFixedThreadPool(threads);
    }

    public boolean isShutdown()
    {
        return isShutdown.get();
    }

    public void shutdown()
    {
        assert isShutdown.compareAndSet(false, true);

        cluster.close();
        executor.shutdown();
        try
        {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void schemaChange(String statement)
    {
        cluster.schemaChange(statement);
    }

    public Object[][] execute(String statement, Object... bindings)
    {
        if (isShutdown.get())
            throw new RuntimeException("Instance is shut down");

        try
        {
            return cluster
                   // round-robin
                   .coordinator((int) (cnt.getAndIncrement() % cluster.size() + 1))
                   .execute(statement, ConsistencyLevel.QUORUM, bindings);
        }
        catch (Throwable t)
        {
            logger.error(String.format("Caught error while trying execute statement %s", statement),
                         t);
            throw t;
        }
    }

    public CompletableFuture<Object[][]> executeAsync(String statement, Object... bindings)
    {
        return CompletableFuture.supplyAsync(() -> execute(statement, bindings), executor);
    }

    @JsonTypeName("in_jvm")
    public static class InJvmSutConfiguration implements Configuration.SutConfiguration
    {
        public final int nodes;
        public final int worker_threads;
        public final String root;

        @JsonCreator
        public InJvmSutConfiguration(@JsonProperty(value = "nodes", defaultValue = "3") int nodes,
                                     @JsonProperty(value = "worker_threads", defaultValue = "10") int worker_threads,
                                     @JsonProperty("root") String root)
        {
            this.nodes = nodes;
            this.worker_threads = worker_threads;
            this.root = root;
        }

        public SystemUnderTest make()
        {
            Cluster cluster;
            try
            {
                cluster = Cluster.build().withConfig((cfg) -> {
                    // TODO: make this configurable
                    cfg.set("row_cache_size_in_mb", 10L)
                       .set("index_summary_capacity_in_mb", 10L)
                       .set("counter_cache_size_in_mb", 10L)
                       .set("key_cache_size_in_mb", 10L)
                       .set("file_cache_size_in_mb", 10)
                       .set("prepared_statements_cache_size_mb", 10L)
                       .set("memtable_heap_space_in_mb", 128)
                       .set("memtable_offheap_space_in_mb", 128)
                       .set("memtable_flush_writers", 1)
                       .set("concurrent_compactors", 1)
                       .set("concurrent_reads", 5)
                       .set("concurrent_writes", 5)
                       .set("compaction_throughput_mb_per_sec", 10)
                       .set("hinted_handoff_enabled", false);
                }).withNodes(nodes)
                                 .withRoot(new File(root)).createWithoutStarting();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }

            cluster.startup();
            return new InJvmSut(cluster);
        }
    }
}