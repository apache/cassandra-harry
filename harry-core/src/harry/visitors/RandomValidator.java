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

import java.util.concurrent.atomic.AtomicLong;

import harry.core.MetricReporter;
import harry.core.Run;
import harry.ddl.SchemaSpec;
import harry.generators.Surjections;
import harry.model.Model;
import harry.model.OpSelectors;
import harry.operations.Query;
import harry.operations.QueryGenerator;
import harry.runner.DataTracker;

public class RandomValidator implements Visitor
{
    private final QueryLogger logger;
    private final Model model;

    private final OpSelectors.DefaultPdSelector pdSelector;
    private final QueryGenerator.TypedQueryGenerator querySelector;
    private final MetricReporter metricReporter;
    private final DataTracker tracker;
    private final AtomicLong modifier;
    private final SchemaSpec schemaSpec;

    private final int partitionCount;
    private final int queries;

    public RandomValidator(int partitionCount,
                           int queries,
                           Run run,
                           Model.ModelFactory modelFactory,
                           QueryLogger logger)
    {
        this.partitionCount = partitionCount;
        this.queries = Math.max(queries, 1);
        this.metricReporter = run.metricReporter;
        this.pdSelector = (OpSelectors.DefaultPdSelector) run.pdSelector;
        this.querySelector = new QueryGenerator.TypedQueryGenerator(run.rng,
                                                                    Surjections.pick(Query.QueryKind.SINGLE_PARTITION),
                                                                    run.rangeSelector);
        this.model = modelFactory.make(run);
        this.logger = logger;
        this.tracker = run.tracker;
        this.schemaSpec = run.schemaSpec;

        this.modifier = new AtomicLong();
    }

    // TODO: expose metric, how many times validated recent partitions
    private int validateRandomPartitions()
    {
        for (int i = 0; i < partitionCount && !Thread.currentThread().isInterrupted(); i++)
        {
            metricReporter.validateRandomQuery();
            long modifier = this.modifier.incrementAndGet();
            long pd = pdSelector.randomVisitedPd(tracker.maxStarted(), modifier, schemaSpec);
            for (int j = 0; j < queries && !Thread.currentThread().isInterrupted(); j++)
            {
                Query query = querySelector.inflate(pdSelector.maxLtsFor(pd), j);
                logger.logSelectQuery(j, query);
                model.validate(query);
            }
        }

        return partitionCount;
    }

    @Override
    public void visit()
    {
        validateRandomPartitions();
    }
}