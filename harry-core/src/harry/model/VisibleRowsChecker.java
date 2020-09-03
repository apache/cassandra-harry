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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import harry.core.Configuration;
import harry.data.ResultSetRow;
import harry.ddl.SchemaSpec;
import harry.model.sut.SystemUnderTest;
import harry.runner.Query;
import harry.runner.QuerySelector;

/**
 * A simple model to check whether or not the rows reported as visible by the database are reflected in
 * the model.
 */
public class VisibleRowsChecker implements Model
{
    protected final Map<Long, TreeMap<Long, Event>> eventLog;
    protected final OpSelectors.DescriptorSelector descriptorSelector;
    protected final OpSelectors.PdSelector pdSelector;
    protected final OpSelectors.MonotonicClock clock;
    protected final SystemUnderTest sut;
    protected final AtomicLong maxLts;
    protected final SchemaSpec schema;

    public VisibleRowsChecker(SchemaSpec schema,
                              OpSelectors.PdSelector pdSelector,
                              OpSelectors.DescriptorSelector descriptorSelector,
                              OpSelectors.MonotonicClock clock,
                              QuerySelector querySelector,
                              SystemUnderTest sut)
    {
        this.pdSelector = pdSelector;
        this.descriptorSelector = descriptorSelector;
        this.eventLog = new HashMap<>();
        this.maxLts = new AtomicLong();
        this.schema = schema;
        this.clock = clock;
        this.sut = sut;
    }


    public synchronized void recordEvent(long lts, boolean quorumAchieved)
    {
        maxLts.updateAndGet((old) -> Math.max(old, lts));
        long pd = pdSelector.pd(lts);

        // TODO: This is definitely not optimal, but we probably use a better, potentially off-heap sorted structure for that anyways
        TreeMap<Long, Event> events = eventLog.get(pd);
        if (events == null)
        {
            events = new TreeMap<>();
            eventLog.put(pd, events);
        }

        Event event = events.get(lts);
        assert event == null || !event.quorumAchieved : "Operation should be partially visible before it is fully visible";
        events.put(lts, new Event(lts, quorumAchieved));
    }


    public void validatePartitionState(long validationLts, Query query)
    {
        validatePartitionState(validationLts,
                               query,
                               () -> SelectHelper.execute(sut, clock, query));
    }

    public Configuration.ModelConfiguration toConfig()
    {
        throw new RuntimeException("not implemented");
    }

    synchronized void validatePartitionState(long validationLts, Query query, Supplier<List<ResultSetRow>> rowsSupplier)
    {
        // TODO: Right now, we ignore Query here!
        long pd = pdSelector.pd(validationLts, schema);
        List<ResultSetRow> rows = rowsSupplier.get();
        TreeMap<Long, Event> events = eventLog.get(pd);
        if (!rows.isEmpty() && (events == null || events.isEmpty()))
        {
            throw new ValidationException(String.format("Returned rows are not empty, but there were no records in the event log.\nRows: %s\nSeen pds: %s",
                                                        rows, eventLog.keySet()));
        }

        for (ResultSetRow row : rows)
        {
            LongIterator rowLtsIter = descendingIterator(row.lts);
            PeekingIterator<Event> modelLtsIter = Iterators.peekingIterator(events.subMap(0L, true, maxLts.get(), true)
                                                                                  .descendingMap()
                                                                                  .values()
                                                                                  .iterator());
            outer:
            while (rowLtsIter.hasNext())
            {
                long rowLts = rowLtsIter.nextLong();

                if (rowLts == NO_TIMESTAMP)
                    continue;

                if (!modelLtsIter.hasNext())
                    throw new ValidationException(String.format("Model iterator is exhausted, could not verify %d lts for the row: \n%s %s",
                                                                rowLts, row, query));

                while (modelLtsIter.hasNext())
                {
                    Event event = modelLtsIter.next();
                    if (event.lts > rowLts)
                        continue;
                    if (event.lts < rowLts)
                        throw new RuntimeException("Can't find a corresponding event id in the model for: " + rowLts + " " + event);
                    for (int col = 0; col < row.lts.length; col++)
                    {
                        if (row.lts[col] != rowLts)
                            continue;
                        long m = descriptorSelector.modificationId(pd, row.cd, rowLts, row.vds[col], col);
                        long vd = descriptorSelector.vd(pd, row.cd, rowLts, m, col);
                        if (vd != row.vds[col])
                            throw new RuntimeException("Can't verify the row");
                    }
                    continue outer;
                }
            }
        }
    }

    public interface LongIterator extends Iterator<Long>
    {
        long nextLong();
    }


    public static LongIterator descendingIterator(OpSelectors.PdSelector pdSelector, long verificationLts)
    {
        return new VisibleRowsChecker.LongIterator()
        {
            long next = pdSelector.maxLts(verificationLts);

            public long nextLong()
            {
                long ret = next;
                next = pdSelector.prevLts(next);
                return ret;
            }

            public boolean hasNext()
            {
                return next >= 0;
            }

            public Long next()
            {
                return null;
            }
        };
    }

    public static LongIterator descendingIterator(long[] ltss)
    {
        long[] sorted = Arrays.copyOf(ltss, ltss.length);
        Arrays.sort(sorted);

        return new LongIterator()
        {
            private int lastUniqueIdx = -1;

            public long nextLong()
            {
                if (lastUniqueIdx == -1)
                    throw new RuntimeException("No elements left or hasNext hasn't been called");
                return sorted[lastUniqueIdx];
            }

            public boolean hasNext()
            {
                if (lastUniqueIdx == -1 && sorted.length > 0)
                {
                    lastUniqueIdx = ltss.length - 1;
                    return true;
                }

                long lastUnique = sorted[lastUniqueIdx];
                while (lastUniqueIdx >= 0)
                {
                    if (sorted[lastUniqueIdx] != lastUnique)
                        return true;
                    lastUniqueIdx--;
                }

                lastUniqueIdx = -1;
                return false;
            }

            public Long next()
            {
                return nextLong();
            }
        };
    }

    protected static class Event
    {
        final long lts;
        volatile boolean quorumAchieved;

        public Event(long lts, boolean quorumAchieved)
        {
            this.lts = lts;
            this.quorumAchieved = quorumAchieved;
        }

        public String toString()
        {
            return "Event{" +
                   "lts=" + lts +
                   ", quorumAchieved=" + quorumAchieved +
                   '}';
        }
    }
}