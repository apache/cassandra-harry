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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.junit.Assert;
import org.junit.Test;

import harry.concurrent.ExecutorFactory;
import harry.concurrent.Interruptible;
import harry.concurrent.WaitQueue;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.model.OpSelectors;

import static harry.concurrent.InfiniteLoopExecutor.Daemon.NON_DAEMON;
import static harry.concurrent.InfiniteLoopExecutor.Interrupts.UNSYNCHRONIZED;
import static harry.concurrent.InfiniteLoopExecutor.SimulatorSafe.SAFE;
import static harry.runner.Runner.mergeAndThrow;

public class LockingDataTrackerTest
{
    @Test
    public void testDataTracker() throws Throwable
    {
        SchemaSpec schemaSpec = SchemaGenerators.defaultSchemaSpecGen("test").inflate(1L);
        OpSelectors.Rng rng = new OpSelectors.PCGFast(1L);
        OpSelectors.PdSelector pdSelector = new OpSelectors.DefaultPdSelector(rng, 5, 2);
        LockingDataTracker tracker = new LockingDataTracker(pdSelector, schemaSpec);

        AtomicReference<State> excluded = new AtomicReference<>(State.UNLOCKED);
        AtomicInteger readers = new AtomicInteger(0);
        AtomicInteger writers = new AtomicInteger(0);

        WaitQueue queue = WaitQueue.newWaitQueue();
        WaitQueue.Signal interrupt = queue.register();
        List<Throwable> errors = new CopyOnWriteArrayList<>();

        long lts = 1;
        long pd = pdSelector.pd(lts, schemaSpec);
        for (int i = 0; i < 2; i++)
        {
            ExecutorFactory.Global.executorFactory().infiniteLoop("read-" + i, Runner.wrapInterrupt(state -> {
                if (state == Interruptible.State.NORMAL)
                {
                    tracker.beginModification(lts);
                    writers.incrementAndGet();
                    Assert.assertEquals(0, readers.get());
                    excluded.updateAndGet((prev) -> {
                        assert (prev == State.UNLOCKED || prev == State.LOCKED_FOR_WRITE) : prev;
                        return State.LOCKED_FOR_WRITE;
                    });
                    Assert.assertEquals(0, readers.get());
                    excluded.updateAndGet((prev) -> {
                        assert (prev == State.UNLOCKED || prev == State.LOCKED_FOR_WRITE) : prev;
                        return State.UNLOCKED;
                    });
                    Assert.assertEquals(0, readers.get());
                    writers.decrementAndGet();
                    tracker.endModification(lts);
                    LockSupport.parkNanos(100);
                }
            }, interrupt::signal, errors::add), SAFE, NON_DAEMON, UNSYNCHRONIZED);
        }

        for (int i = 0; i < 2; i++)
        {
            ExecutorFactory.Global.executorFactory().infiniteLoop("write-" + i, Runner.wrapInterrupt(state -> {
                if (state == Interruptible.State.NORMAL)
                {
                    tracker.beginValidation(pd);
                    readers.incrementAndGet();
                    Assert.assertEquals(0, writers.get());
                    excluded.updateAndGet((prev) -> {
                        assert (prev == State.UNLOCKED || prev == State.LOCKED_FOR_READ) : prev;
                        return State.LOCKED_FOR_READ;
                    });
                    Assert.assertEquals(0, writers.get());
                    excluded.updateAndGet((prev) -> {
                        assert (prev == State.UNLOCKED || prev == State.LOCKED_FOR_READ) : prev;
                        return State.UNLOCKED;
                    });
                    Assert.assertEquals(0, writers.get());
                    readers.decrementAndGet();
                    tracker.endValidation(pd);
                    LockSupport.parkNanos(100);
                }
            }, interrupt::signal, errors::add), SAFE, NON_DAEMON, UNSYNCHRONIZED);
        }

        interrupt.await(1, TimeUnit.MINUTES);
        if (!errors.isEmpty())
            Runner.mergeAndThrow(errors);
    }
    enum State { UNLOCKED, LOCKED_FOR_READ, LOCKED_FOR_WRITE }
}
