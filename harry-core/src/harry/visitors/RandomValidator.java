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
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.MetricReporter;
import harry.core.Run;
import harry.generators.RngUtils;
import harry.generators.Surjections;
import harry.model.Model;
import harry.model.OpSelectors;
import harry.operations.Query;
import harry.operations.QueryGenerator;

public class RandomValidator implements Visitor
{
    private final BufferedWriter validationLog;
    private static final Logger logger = LoggerFactory.getLogger(RandomValidator.class);
    private final Model model;

    private final OpSelectors.PdSelector pdSelector;
    private final QueryGenerator.TypedQueryGenerator querySelector;
    private final MetricReporter metricReporter;
    private final OpSelectors.MonotonicClock clock;

    private final int partitionCount;
    private final int queries;

    public RandomValidator(int partitionCount,
                           int queries,
                           Run run,
                           Model.ModelFactory modelFactory)
    {
        this.partitionCount = partitionCount;
        this.queries = Math.max(queries, 1);
        this.metricReporter = run.metricReporter;
        this.pdSelector = run.pdSelector;
        this.clock = run.clock;
        this.querySelector = new QueryGenerator.TypedQueryGenerator(run.rng,
                                                                    Surjections.pick(Query.QueryKind.SINGLE_PARTITION),
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
    private int validateRandomPartitions()
    {
        Random rng = new Random();
        long maxPos = pdSelector.maxPosition(clock.peek());

        int cnt = 0;
        for (int i = 0; i < partitionCount && !Thread.currentThread().isInterrupted(); i++)
        {
            metricReporter.validateRandomQuery();
            long pos = RngUtils.trim(rng.nextLong(), maxPos);
            long visitLts = pdSelector.minLtsAt(pos);
            for (int j = 0; j < queries && !Thread.currentThread().isInterrupted(); j++)
            {
                Query query = querySelector.inflate(visitLts, cnt);
                log(j, query);
                model.validate(query);
                cnt++;
            }
        }

        return partitionCount;
    }

    @Override
    public void visit()
    {
        validateRandomPartitions();
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