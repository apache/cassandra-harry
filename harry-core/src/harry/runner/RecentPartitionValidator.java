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

import harry.core.MetricReporter;
import harry.core.Run;
import harry.generators.Surjections;
import harry.model.Model;
import harry.model.OpSelectors;

public class RecentPartitionValidator implements PartitionVisitor
{
    private final Model model;

    private final OpSelectors.MonotonicClock clock;
    private final OpSelectors.PdSelector pdSelector;
    private final QueryGenerator.TypedQueryGenerator querySelector;
    private final MetricReporter metricReporter;
    private final int partitionCount;
    private final int triggerAfter;

    public RecentPartitionValidator(int partitionCount,
                                    int triggerAfter,
                                    Run run,
                                    Model.ModelFactory modelFactory)
    {
        this.partitionCount = partitionCount;
        this.triggerAfter = triggerAfter;
        this.metricReporter = run.metricReporter;
        this.clock = run.clock;
        this.pdSelector = run.pdSelector;

        this.querySelector = new QueryGenerator.TypedQueryGenerator(run.rng,
                                                                    // TODO: make query kind configurable
                                                                    Surjections.enumValues(Query.QueryKind.class),
                                                                    run.rangeSelector);
        this.model = modelFactory.make(run);
    }

    // TODO: expose metric, how many times validated recent partitions
    private void validateRecentPartitions(int partitionCount)
    {
        long maxLts = clock.maxLts();
        long pos = pdSelector.positionFor(maxLts);

        int maxPartitions = partitionCount;
        while (pos > 0 && maxPartitions > 0 && !Thread.currentThread().isInterrupted())
        {
            long visitLts = pdSelector.minLtsAt(pos);

            metricReporter.validateRandomQuery();
            model.validate(querySelector.inflate(visitLts, 0));

            pos--;
            maxPartitions--;
        }
    }

    public void visitPartition(long lts)
    {
        if (lts % triggerAfter == 0)
            validateRecentPartitions(partitionCount);
    }

    public void shutdown() throws InterruptedException
    {
    }
}