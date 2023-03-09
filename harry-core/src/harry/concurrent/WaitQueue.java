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

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import harry.concurrent.Awaitable.AbstractAwaitable;

import static harry.concurrent.Clock.Global.nanoTime;

/**
 * This class was borrowed from Apache Cassandra, org.cassandra.utils.concurrent, until there's a shared concurrency lib.
 */
public interface WaitQueue
{
    /**
     * A Signal is a one-time-use mechanism for a thread to wait for notification that some condition
     * state has transitioned that it may be interested in (and hence should check if it is).
     * It is potentially transient, i.e. the state can change in the meantime, it only indicates
     * that it should be checked, not necessarily anything about what the expected state should be.
     *
     * Signal implementations should never wake up spuriously, they are always woken up by a
     * signal() or signalAll().
     *
     * This abstract definition of Signal does not need to be tied to a WaitQueue.
     * Whilst RegisteredSignal is the main building block of Signals, this abstract
     * definition allows us to compose Signals in useful ways. The Signal is 'owned' by the
     * thread that registered itself with WaitQueue(s) to obtain the underlying RegisteredSignal(s);
     * only the owning thread should use a Signal.
     */
    public static interface Signal extends Condition
    {
        /**
         * @return true if cancelled; once cancelled, must be discarded by the owning thread.
         */
        public boolean isCancelled();

        /**
         * @return isSignalled() || isCancelled(). Once true, the state is fixed and the Signal should be discarded
         * by the owning thread.
         */
        public boolean isSet();

        /**
         * atomically: cancels the Signal if !isSet(), or returns true if isSignalled()
         *
         * @return true if isSignalled()
         */
        public boolean checkAndClear();

        /**
         * Should only be called by the owning thread. Indicates the signal can be retired,
         * and if signalled propagates the signal to another waiting thread
         */
        public abstract void cancel();
    }

    /**
     * The calling thread MUST be the thread that uses the signal
     */
    public Signal register();

    /**
     * The calling thread MUST be the thread that uses the signal.
     * If the Signal is waited on, context.stop() will be called when the wait times out, the Signal is signalled,
     * or the waiting thread is interrupted.
     */
    public <V> Signal register(V supplyOnDone, Consumer<V> receiveOnDone);

    /**
     * Signal one waiting thread
     */
    public boolean signal();

    /**
     * Signal all waiting threads
     */
    public void signalAll();

    /** getWaiting() > 0 */
    public boolean hasWaiters();

    /** Return how many threads are waiting */
    public int getWaiting();

    /**
     * Factory method used to capture and redirect instantiations for simulation
     */
    public static WaitQueue newWaitQueue()
    {
        return new Standard();
    }

    class Standard implements WaitQueue
    {
        private static final int CANCELLED = -1;
        private static final int SIGNALLED = 1;
        private static final int NOT_SET = 0;

        private static final AtomicIntegerFieldUpdater<RegisteredSignal> signalledUpdater = AtomicIntegerFieldUpdater.newUpdater(RegisteredSignal.class, "state");

        // the waiting signals
        private final ConcurrentLinkedQueue<RegisteredSignal> queue = new ConcurrentLinkedQueue<>();

        protected Standard() {}

        /**
         * The calling thread MUST be the thread that uses the signal
         * @return                                x
         */
        public Signal register()
        {
            RegisteredSignal signal = new RegisteredSignal();
            queue.add(signal);
            return signal;
        }

        /**
         * The calling thread MUST be the thread that uses the signal.
         * If the Signal is waited on, context.stop() will be called when the wait times out, the Signal is signalled,
         * or the waiting thread is interrupted.
         */
        public <V> Signal register(V supplyOnDone, Consumer<V> receiveOnDone)
        {
            RegisteredSignal signal = new SignalWithListener<>(supplyOnDone, receiveOnDone);
            queue.add(signal);
            return signal;
        }

        /**
         * Signal one waiting thread
         */
        public boolean signal()
        {
            while (true)
            {
                RegisteredSignal s = queue.poll();
                if (s == null || s.doSignal() != null)
                    return s != null;
            }
        }

        /**
         * Signal all waiting threads
         */
        public void signalAll()
        {
            if (!hasWaiters())
                return;

            // to avoid a race where the condition is not met and the woken thread managed to wait on the queue before
            // we finish signalling it all, we pick a random thread we have woken-up and hold onto it, so that if we encounter
            // it again we know we're looping. We reselect a random thread periodically, progressively less often.
            // the "correct" solution to this problem is to use a queue that permits snapshot iteration, but this solution is sufficient
            // TODO: this is only necessary because we use CLQ - which is only for historical any-NIH reasons
            int i = 0, s = 5;
            Thread randomThread = null;
            Iterator<RegisteredSignal> iter = queue.iterator();
            while (iter.hasNext())
            {
                RegisteredSignal signal = iter.next();
                Thread signalled = signal.doSignal();

                if (signalled != null)
                {
                    if (signalled == randomThread)
                        break;

                    if (++i == s)
                    {
                        randomThread = signalled;
                        s <<= 1;
                    }
                }

                iter.remove();
            }
        }

        private void cleanUpCancelled()
        {
            // TODO: attempt to remove the cancelled from the beginning only (need atomic cas of head)
            queue.removeIf(RegisteredSignal::isCancelled);
        }

        public boolean hasWaiters()
        {
            return !queue.isEmpty();
        }

        /**
         * @return how many threads are waiting
         */
        public int getWaiting()
        {
            if (!hasWaiters())
                return 0;
            Iterator<RegisteredSignal> iter = queue.iterator();
            int count = 0;
            while (iter.hasNext())
            {
                Signal next = iter.next();
                if (!next.isCancelled())
                    count++;
            }
            return count;
        }

        /**
         * An abstract signal implementation
         *
         * TODO: use intrusive linked list
         */
        public static abstract class AbstractSignal extends AbstractAwaitable implements Signal
        {
            public Signal await() throws InterruptedException
            {
                while (!isSignalled())
                {
                    checkInterrupted();
                    LockSupport.park();
                }
                checkAndClear();
                return this;
            }

            public boolean awaitUntil(long nanoTimeDeadline) throws InterruptedException
            {
                long now;
                while (nanoTimeDeadline > (now = nanoTime()) && !isSignalled())
                {
                    checkInterrupted();
                    long delta = nanoTimeDeadline - now;
                    LockSupport.parkNanos(delta);
                }
                return checkAndClear();
            }

            private void checkInterrupted() throws InterruptedException
            {
                if (Thread.interrupted())
                {
                    cancel();
                    throw new InterruptedException();
                }
            }
        }

        /**
         * A signal registered with this WaitQueue
         */
        private class RegisteredSignal extends AbstractSignal
        {
            private volatile Thread thread = Thread.currentThread();
            volatile int state;

            public boolean isSignalled()
            {
                return state == SIGNALLED;
            }

            public boolean isCancelled()
            {
                return state == CANCELLED;
            }

            public boolean isSet()
            {
                return state != NOT_SET;
            }

            private Thread doSignal()
            {
                if (!isSet() && signalledUpdater.compareAndSet(this, NOT_SET, SIGNALLED))
                {
                    Thread thread = this.thread;
                    LockSupport.unpark(thread);
                    this.thread = null;
                    return thread;
                }
                return null;
            }

            public void signal()
            {
                doSignal();
            }

            public boolean checkAndClear()
            {
                if (!isSet() && signalledUpdater.compareAndSet(this, NOT_SET, CANCELLED))
                {
                    thread = null;
                    cleanUpCancelled();
                    return false;
                }
                // must now be signalled assuming correct API usage
                return true;
            }

            /**
             * Should only be called by the registered thread. Indicates the signal can be retired,
             * and if signalled propagates the signal to another waiting thread
             */
            public void cancel()
            {
                if (isCancelled())
                    return;
                if (!signalledUpdater.compareAndSet(this, NOT_SET, CANCELLED))
                {
                    // must already be signalled - switch to cancelled and
                    state = CANCELLED;
                    // propagate the signal
                    WaitQueue.Standard.this.signal();
                }
                thread = null;
                cleanUpCancelled();
            }
        }

        /**
         * A RegisteredSignal that stores a TimerContext, and stops the timer when either cancelled or
         * finished waiting. i.e. if the timer is started when the signal is registered it tracks the
         * time in between registering and invalidating the signal.
         */
        private final class SignalWithListener<V> extends RegisteredSignal
        {
            private final V supplyOnDone;
            private final Consumer<V> receiveOnDone;

            private SignalWithListener(V supplyOnDone, Consumer<V> receiveOnDone)
            {
                this.receiveOnDone = receiveOnDone;
                this.supplyOnDone = supplyOnDone;
            }


            @Override
            public boolean checkAndClear()
            {
                receiveOnDone.accept(supplyOnDone);
                return super.checkAndClear();
            }

            @Override
            public void cancel()
            {
                if (!isCancelled())
                {
                    receiveOnDone.accept(supplyOnDone);
                    super.cancel();
                }
            }
        }
    }

    /**
     * Loops waiting on the supplied condition and WaitQueue and will not return until the condition is true
     */
    public static void waitOnCondition(BooleanSupplier condition, WaitQueue queue) throws InterruptedException
    {
        while (!condition.getAsBoolean())
        {
            Signal s = queue.register();
            if (!condition.getAsBoolean()) s.await();
            else s.cancel();
        }
    }
}
