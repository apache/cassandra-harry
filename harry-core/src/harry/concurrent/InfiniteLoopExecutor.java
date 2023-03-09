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

package harry.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static harry.concurrent.Clock.Global.nanoTime;
import static harry.concurrent.Condition.newOneTimeCondition;
import static harry.concurrent.InfiniteLoopExecutor.InternalState.SHUTTING_DOWN_NOW;
import static harry.concurrent.InfiniteLoopExecutor.InternalState.TERMINATED;
import static harry.concurrent.InfiniteLoopExecutor.Interrupts.UNSYNCHRONIZED;
import static harry.concurrent.Interruptible.State.INTERRUPTED;
import static harry.concurrent.Interruptible.State.NORMAL;
import static harry.concurrent.Interruptible.State.SHUTTING_DOWN;

/**
 * This class was borrowed from Apache Cassandra, org.cassandra.utils.concurrent, until there's a shared concurrency lib.
 */
public class InfiniteLoopExecutor implements Interruptible
{
    private static final Logger logger = LoggerFactory.getLogger(InfiniteLoopExecutor.class);

    public enum InternalState { SHUTTING_DOWN_NOW, TERMINATED }

    public enum SimulatorSafe { SAFE, UNSAFE }

    public enum Daemon        { DAEMON, NON_DAEMON }

    public enum Interrupts    { SYNCHRONIZED, UNSYNCHRONIZED }

    private static final AtomicReferenceFieldUpdater<InfiniteLoopExecutor, Object> stateUpdater = AtomicReferenceFieldUpdater.newUpdater(InfiniteLoopExecutor.class, Object.class, "state");
    private final Thread thread;
    private final Task task;
    private volatile Object state = NORMAL;
    private final Consumer<Thread> interruptHandler;
    private final Condition isTerminated = newOneTimeCondition();

    public InfiniteLoopExecutor(String name, Task task, Daemon daemon)
    {
        this(ExecutorFactory.Global.executorFactory(), name, task, daemon, UNSYNCHRONIZED);
    }

    public InfiniteLoopExecutor(ExecutorFactory factory, String name, Task task, Daemon daemon)
    {
        this(factory, name, task, daemon, UNSYNCHRONIZED);
    }

    public InfiniteLoopExecutor(ExecutorFactory factory, String name, Task task, Daemon daemon, Interrupts interrupts)
    {
        this.task = task;
        this.thread = factory.startThread(name, this::loop, daemon);
        this.interruptHandler = interrupts == Interrupts.SYNCHRONIZED
                                ? interruptHandler(task)
                                : Thread::interrupt;
    }

    public InfiniteLoopExecutor(BiFunction<String, Runnable, Thread> threadStarter, String name, Task task, Interrupts interrupts)
    {
        this.task = task;
        this.thread = threadStarter.apply(name, this::loop);
        this.interruptHandler = interrupts == Interrupts.SYNCHRONIZED
                                ? interruptHandler(task)
                                : Thread::interrupt;
    }

    private static Consumer<Thread> interruptHandler(final Object monitor)
    {
        return thread -> {
            synchronized (monitor)
            {
                thread.interrupt();
            }
        };
    }


    private void loop()
    {
        boolean interrupted = false;
        try
        {
            while (true)
            {
                try
                {
                    Object cur = state;
                    if (cur == SHUTTING_DOWN_NOW) break;

                    interrupted |= Thread.interrupted();
                    if (cur == NORMAL && interrupted) cur = INTERRUPTED;
                    task.run((State) cur);

                    interrupted = false;
                    if (cur == SHUTTING_DOWN) break;
                }
                catch (TerminateException ignore)
                {
                    break;
                }
                catch (UncheckedInterruptedException | InterruptedException ignore)
                {
                    interrupted = true;
                }
                catch (Throwable t)
                {
                    logger.error("Exception thrown by runnable, continuing with loop", t);
                }
            }
        }
        finally
        {
            state = TERMINATED;
            isTerminated.signal();
        }
    }

    public void interrupt()
    {
        interruptHandler.accept(thread);
    }

    public void shutdown()
    {
        stateUpdater.updateAndGet(this, cur -> cur != TERMINATED && cur != SHUTTING_DOWN_NOW ? SHUTTING_DOWN : cur);
        // TODO: InfiniteLoopExecutor should let the threads quiesce themselves rather then send interrupts
        //interruptHandler.accept(thread);
    }

    public Object shutdownNow()
    {
        stateUpdater.updateAndGet(this, cur -> cur != TERMINATED ? SHUTTING_DOWN_NOW : TERMINATED);
        interruptHandler.accept(thread);
        return null;
    }

    @Override
    public boolean isTerminated()
    {
        return state == TERMINATED;
    }

    public boolean awaitTermination(long time, TimeUnit unit) throws InterruptedException
    {
        if (isTerminated())
            return true;

        long deadlineNanos = nanoTime() + unit.toNanos(time);
        isTerminated.awaitUntil(deadlineNanos);
        return isTerminated();
    }

    public boolean isAlive()
    {
        return this.thread.isAlive();
    }
}
