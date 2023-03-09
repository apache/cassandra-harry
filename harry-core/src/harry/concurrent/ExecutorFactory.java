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

import static harry.concurrent.InfiniteLoopExecutor.Daemon.DAEMON;
import static harry.concurrent.InfiniteLoopExecutor.Interrupts.UNSYNCHRONIZED;
import static harry.concurrent.NamedThreadFactory.createThread;
import static harry.concurrent.NamedThreadFactory.setupThread;

/**
 * Entry point for configuring and creating new executors.
 *
 *
 * This class was borrowed from Apache Cassandra, org.cassandra.utils.concurrent, until there's a shared concurrency lib.
 */
public interface ExecutorFactory
{
    public enum SimulatorSemantics
    {
        NORMAL, DISCARD
    }

    /**
     * Create and start a new thread to execute {@code runnable}
     * @param name the name of the thread
     * @param runnable the task to execute
     * @param daemon flag to indicate whether the thread should be a daemon or not
     * @return the new thread
     */
    Thread startThread(String name, Runnable runnable, InfiniteLoopExecutor.Daemon daemon);

    /**
     * Create and start a new thread to execute {@code runnable}; this thread will be a daemon thread.
     * @param name the name of the thread
     * @param runnable the task to execute
     * @return the new thread
     */
    default Thread startThread(String name, Runnable runnable)
    {
        return startThread(name, runnable, DAEMON);
    }

    /**
     * Create and start a new InfiniteLoopExecutor to repeatedly invoke {@code runnable}.
     * On shutdown, the executing thread will be interrupted; to support clean shutdown
     * {@code runnable} should propagate {@link InterruptedException}
     *
     * @param name the name of the thread used to invoke the task repeatedly
     * @param task the task to execute repeatedly
     * @param simulatorSafe flag indicating if the loop thread can be intercepted / rescheduled during cluster simulation
     * @param daemon flag to indicate whether the loop thread should be a daemon thread or not
     * @param interrupts flag to indicate whether to synchronize interrupts of the task execution thread
     *                   using the task's monitor this can be used to prevent interruption while performing
     *                   IO operations which forbid interrupted threads.
     *                   See: {@link org.apache.cassandra.db.commitlog.AbstractCommitLogSegmentManager::start}
     * @return the new thread
     */
    Interruptible infiniteLoop(String name, Interruptible.Task task, InfiniteLoopExecutor.SimulatorSafe simulatorSafe, InfiniteLoopExecutor.Daemon daemon, InfiniteLoopExecutor.Interrupts interrupts);

    /**
     * Create and start a new InfiniteLoopExecutor to repeatedly invoke {@code runnable}.
     * On shutdown, the executing thread will be interrupted; to support clean shutdown
     * {@code runnable} should propagate {@link InterruptedException}
     *
     * @param name the name of the thread used to invoke the task repeatedly
     * @param task the task to execute repeatedly
     * @param simulatorSafe flag indicating if the loop thread can be intercepted / rescheduled during cluster simulation
     * @return the new thread
     */
    default Interruptible infiniteLoop(String name, Interruptible.SimpleTask task, InfiniteLoopExecutor.SimulatorSafe simulatorSafe)
    {
        return infiniteLoop(name, Interruptible.Task.from(task), simulatorSafe, DAEMON, UNSYNCHRONIZED);
    }

    /**
     * Create a new thread group for use with builders - this thread group will be situated within
     * this factory's parent thread group, and may be supplied to multiple executor builders.
     */
    ThreadGroup newThreadGroup(String name);

    public static final class Global
    {
        // deliberately not volatile to ensure zero overhead outside of testing;
        // depend on other memory visibility primitives to ensure visibility
        private static ExecutorFactory FACTORY = new ExecutorFactory.Default(Global.class.getClassLoader(), null, (t, e) -> e.printStackTrace());
        private static boolean modified;

        public static ExecutorFactory executorFactory()
        {
            return FACTORY;
        }

        public static synchronized void unsafeSet(ExecutorFactory executorFactory)
        {
            FACTORY = executorFactory;
            modified = true;
        }

        public static synchronized boolean tryUnsafeSet(ExecutorFactory executorFactory)
        {
            if (modified)
                return false;
            unsafeSet(executorFactory);
            return true;
        }
    }

    public static final class Default extends NamedThreadFactory.MetaFactory implements ExecutorFactory
    {
        public Default(ClassLoader contextClassLoader, ThreadGroup threadGroup, Thread.UncaughtExceptionHandler uncaughtExceptionHandler)
        {
            super(contextClassLoader, threadGroup, uncaughtExceptionHandler);
        }


        @Override
        public Thread startThread(String name, Runnable runnable, InfiniteLoopExecutor.Daemon daemon)
        {
            Thread thread = setupThread(createThread(threadGroup, runnable, name, daemon == DAEMON),
                                        Thread.NORM_PRIORITY,
                                        contextClassLoader,
                                        uncaughtExceptionHandler);
            thread.start();
            return thread;
        }

        @Override
        public Interruptible infiniteLoop(String name, Interruptible.Task task, InfiniteLoopExecutor.SimulatorSafe simulatorSafe, InfiniteLoopExecutor.Daemon daemon, InfiniteLoopExecutor.Interrupts interrupts)
        {
            return new InfiniteLoopExecutor(this, name, task, daemon, interrupts);
        }

        @Override
        public ThreadGroup newThreadGroup(String name)
        {
            return threadGroup == null ? null : new ThreadGroup(threadGroup, name);
        }
    }
}