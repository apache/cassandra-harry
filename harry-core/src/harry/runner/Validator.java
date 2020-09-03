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

package harry.runner;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.ddl.SchemaSpec;
import harry.generators.Surjections;
import harry.model.Model;
import harry.model.OpSelectors;

// This might be something that potentially grows into the validator described in the design doc;
// right now it's just a helper/container class
public class Validator
{
    private static final Logger logger = LoggerFactory.getLogger(Validator.class);

    private final Model model;
    private final SchemaSpec schema;

    private final OpSelectors.MonotonicClock clock;
    private final OpSelectors.PdSelector pdSelector;
    private final QuerySelector querySelector;

    public Validator(Model model,
                     SchemaSpec schema,
                     OpSelectors.MonotonicClock clock,
                     OpSelectors.PdSelector pdSelector,
                     OpSelectors.DescriptorSelector descriptorSelector,
                     OpSelectors.Rng rng)
    {
        this.model = model;
        this.schema = schema;
        this.clock = clock;
        this.pdSelector = pdSelector;
        this.querySelector = new QuerySelector(schema,
                                               pdSelector,
                                               descriptorSelector,
                                               Surjections.enumValues(Query.QueryKind.class),
                                               rng);
    }

    // TODO: expose metric, how many times validated recent partitions
    public void validateRecentPartitions(int partitionCount)
    {
        long maxLts = clock.maxLts();
        long pos = pdSelector.positionFor(maxLts);

        int maxPartitions = partitionCount;
        while (pos > 0 && maxPartitions > 0 && !Thread.currentThread().isInterrupted())
        {
            long visitLts = pdSelector.minLtsAt(pos);

            model.validatePartitionState(visitLts,
                                         querySelector.inflate(visitLts, 0));

            pos--;
            maxPartitions--;
        }
    }

    public CompletableFuture<Void> validateAllPartitions(ExecutorService executor, BooleanSupplier keepRunning, int parallelism)
    {
        long maxLts = clock.maxLts() - 1;
        long maxPos = pdSelector.positionFor(maxLts);
        AtomicLong counter = new AtomicLong();
        CompletableFuture[] futures = new CompletableFuture[parallelism];
        for (int i = 0; i < parallelism; i++)
        {
            futures[i] = CompletableFuture.supplyAsync(() -> {
                long pos;
                while ((pos = counter.getAndIncrement()) < maxPos && !executor.isShutdown() && keepRunning.getAsBoolean())
                {
                    if (pos > 0 && pos % 1000 == 0)
                        logger.debug(String.format("Validated %d out of %d partitions", pos, maxPos));
                    long visitLts = pdSelector.minLtsAt(pos);
                    for (boolean reverse : new boolean[]{ true, false })
                    {
                        model.validatePartitionState(visitLts,
                                                     Query.selectPartition(schema, pdSelector.pd(visitLts, schema), reverse));
                    }
                }
                return null;
            }, executor);
        }
        return CompletableFuture.allOf(futures);
    }

    public void validatePartition(long lts)
    {
        for (boolean reverse : new boolean[]{ true, false })
        {
            model.validatePartitionState(lts,
                                         Query.selectPartition(schema, pdSelector.pd(lts, schema), reverse));
        }
    }
}
