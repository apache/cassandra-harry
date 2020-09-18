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

package harry.model;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataTracker
{
    private static final Logger logger = LoggerFactory.getLogger(DataTracker.class);

    private final AtomicLong maxSeenLts;
    // TODO: This is a trivial implementation that can be significantly improved upon
    // for example, we could use a bitmap that records `1`s for all lts that are after
    // the consective, and "collapse" the bitmap state into the long as soon as we see
    // consecutive `1` on the left side.
    private final AtomicLong maxCompleteLts;
    private final PriorityBlockingQueue<Long> reorderBuffer;

    DataTracker()
    {
        this.maxSeenLts = new AtomicLong(-1);
        this.maxCompleteLts = new AtomicLong(-1);
        this.reorderBuffer = new PriorityBlockingQueue<>(100);
    }

    // TODO: there's also some room for improvement in terms of concurrency
    // TODO: remove pd?
    public void recordEvent(long lts, boolean quorumAchieved)
    {
        // all seen LTS are allowed to be "in-flight"
        maxSeenLts.getAndUpdate((old) -> Math.max(lts, old));

        if (!quorumAchieved)
            return;

        long maxAchievedConsecutive = drainReorderQueue();

        if (maxAchievedConsecutive + 1 == lts)
            maxCompleteLts.compareAndSet(maxAchievedConsecutive, lts);
        else
            reorderBuffer.offer(lts);
    }

    public long drainReorderQueue()
    {
        long expected = maxCompleteLts.get();
        long maxAchievedConsecutive = expected;
        if (reorderBuffer.isEmpty())
            return maxAchievedConsecutive;

        boolean catchingUp = false;

        Long smallest = reorderBuffer.poll();
        while (smallest != null && smallest == maxAchievedConsecutive + 1)
        {
            maxAchievedConsecutive++;
            catchingUp = true;
            smallest = reorderBuffer.poll();
        }

        // put back
        if (smallest != null)
            reorderBuffer.offer(smallest);

        if (catchingUp)
            maxCompleteLts.compareAndSet(expected, maxAchievedConsecutive);

        int bufferSize = reorderBuffer.size();
        if (bufferSize > 100)
            logger.warn("Reorder buffer size has grown up to " + reorderBuffer.size());
        return maxAchievedConsecutive;
    }

    public long maxSeenLts()
    {
        return maxSeenLts.get();
    }

    public long maxCompleteLts()
    {
        return maxCompleteLts.get();
    }

    @VisibleForTesting
    public void forceLts(long maxSeen, long maxComplete)
    {
        this.maxSeenLts.set(maxSeen);
        this.maxCompleteLts.set(maxComplete);
    }

    public String toString()
    {
        List<Long> buf = new ArrayList<>(reorderBuffer);
        return "DataTracker{" +
               "maxSeenLts=" + maxSeenLts +
               ", maxCompleteLts=" + maxCompleteLts +
               ", reorderBuffer=" + buf +
               '}';
    }
}
