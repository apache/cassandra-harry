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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.MetricReporter;
import harry.core.Run;
import harry.generators.Surjections;
import harry.model.Model;
import harry.model.OpSelectors;
import harry.operations.Query;
import harry.operations.QueryGenerator;

public class RecentValidator implements Visitor
{
    private final BufferedWriter validationLog;
    private static final Logger logger = LoggerFactory.getLogger(RecentValidator.class);
    private final Model model;

    private final OpSelectors.PdSelector pdSelector;
    private final QueryGenerator.TypedQueryGenerator querySelector;
    private final MetricReporter metricReporter;
    private final OpSelectors.MonotonicClock clock;

    private final AtomicBoolean condition;
    private final AtomicLong maxPos;

    private final int partitionCount;
    private final int queries;

    public RecentValidator(int partitionCount,
                           int queries,
                           int triggerAfter,
                           Run run,
                           Model.ModelFactory modelFactory)
    {
        this.partitionCount = partitionCount;
        this.queries = queries;
        this.metricReporter = run.metricReporter;
        this.pdSelector = run.pdSelector;
        this.clock = run.clock;

        this.condition = new AtomicBoolean();
        this.maxPos = new AtomicLong(-1);

        run.tracker.onLtsStarted((lts) -> {
            maxPos.updateAndGet(current -> Math.max(pdSelector.positionFor(lts), current));
            if (triggerAfter == 0 || (triggerAfter > 0 && lts % triggerAfter == 0))
                condition.set(true);
        });
        this.querySelector = new QueryGenerator.TypedQueryGenerator(run.rng,
                                                                    // TODO: make query kind configurable
                                                                    Surjections.enumValues(Query.QueryKind.class),
                                                                    run.rangeSelector);
        this.model = modelFactory.make(run);
        File f = new File("validation.log");
        try
        {
            validationLog = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f)));
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    // TODO: expose metric, how many times validated recent partitions
    private int validateRecentPartitions()
    {
        long pos = maxPos.get();

        int maxPartitions = partitionCount;
        while (pos >= 0 && maxPartitions > 0 && !Thread.currentThread().isInterrupted())
        {
            long visitLts = pdSelector.minLtsAt(pos);
            for (int i = 0; i < queries; i++)
            {
                metricReporter.validateRandomQuery();
                Query query = querySelector.inflate(visitLts, i);
                // TODO: add pd skipping from shrinker here, too
                log(i, query);
                model.validate(query);
            }

            pos--;
            maxPartitions--;
        }
        
        return partitionCount - maxPartitions;
    }

    @Override
    public void visit()
    {
        if (condition.compareAndSet(true, false))
        {
            long lts = clock.peek();
            logger.info("Validating (up to) {} recent partitions as of lts {}...", partitionCount, lts);
            int count = validateRecentPartitions();
            logger.info("...finished validating {} recent partitions as of lts {}.", count, lts);
        }
    }

    private void log(int modifier, Query query)
    {
        try
        {
            validationLog.write(String.format("PD: %d. Modifier: %d.", query.pd, modifier));
            validationLog.write("\t");
            validationLog.write(query.toSelectStatement().toString());
            validationLog.write("\n");
            validationLog.flush();
        }
        catch (IOException e)
        {
            // ignore
        }
    }
}