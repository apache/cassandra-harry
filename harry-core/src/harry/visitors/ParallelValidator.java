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

package harry.visitors;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Run;

public abstract class ParallelValidator<T extends ParallelValidator.State> implements PartitionVisitor
{
    private static final Logger logger = LoggerFactory.getLogger(AllPartitionsValidator.class);

    protected final Run run;
    protected final int parallelism;
    protected final int triggerAfter;
    protected final ExecutorService executor;
    protected final AtomicLong maxPos = new AtomicLong(-1);

    public ParallelValidator(int parallelism,
                             int triggerAfter,
                             Run run)
    {
        this.triggerAfter = triggerAfter;
        this.run = run;
        this.parallelism = parallelism;
        this.executor = Executors.newFixedThreadPool(parallelism);
    }

    protected abstract void doOne(T state);
    protected abstract T initialState();

    protected CompletableFuture<Void> startThreads(ExecutorService executor, int parallelism)
    {
        CompletableFuture<?>[] futures = new CompletableFuture[parallelism];
        T shared = initialState();

        for (int i = 0; i < parallelism; i++)
        {
            futures[i] = CompletableFuture.supplyAsync(() -> {
                while (!shared.signalled())
                    doOne(shared);

                return null;
            }, executor);
        }

        return CompletableFuture.allOf(futures);
    }

    public abstract static class State
    {
        private final AtomicBoolean isDone = new AtomicBoolean(false);

        public void signal()
        {
            isDone.set(true);
        }

        public boolean signalled()
        {
            return isDone.get();
        }
    }

    public void visitPartition(long lts)
    {
        maxPos.updateAndGet(current -> Math.max(run.pdSelector.positionFor(lts), current));

        if (triggerAfter > 0 && lts % triggerAfter == 0)
        {
            try
            {
                startThreads(executor, parallelism).get();
            }
            catch (Throwable e)
            {
                throw new RuntimeException(e);
            }
            logger.info("Finished validations");
        }
    }

    public void shutdown() throws InterruptedException
    {
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.SECONDS);
    }
}