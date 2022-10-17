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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Configuration;
import harry.core.Run;
import harry.generators.Surjections;
import harry.model.Model;
import harry.operations.Query;
import harry.operations.QueryGenerator;

public class ParallelRecentValidator extends ParallelValidator<ParallelRecentValidator.State>
{
    private static final Logger logger = LoggerFactory.getLogger(ParallelRecentValidator.class);

    private final int partitionCount;
    private final int queries;
    private final QueryGenerator.TypedQueryGenerator querySelector;
    private final Model model;
    private final BufferedWriter validationLog;

    public ParallelRecentValidator(int partitionCount, int concurrency, int triggerAfter, int queries,
                                   Run run,
                                   Model.ModelFactory modelFactory)
    {
        super(concurrency, triggerAfter, run);
        this.partitionCount = partitionCount;
        this.queries = Math.max(queries, 1);
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

    protected void doOne(State state)
    {
        long claim = state.claim();
        if (claim < 0)
            return;

        long visitLts = run.pdSelector.minLtsAt(state.position - claim);
        for (int i = 0; i < queries; i++)
        {
            run.metricReporter.validateRandomQuery();
            Query query = querySelector.inflate(visitLts, i);
            model.validate(query);
            log(visitLts, i, query);
        }
    }

    protected CompletableFuture<Void> startThreads(ExecutorService executor, int parallelism)
    {
        logger.info("Validating {} recent partitions", partitionCount);
        return super.startThreads(executor, parallelism);
    }

    protected State initialState()
    {
        return new State(maxPos.get());
    }

    public class State extends ParallelValidator.State
    {
        private final long position;
        private final AtomicLong counter;

        public State(long maxPos)
        {
            this.position = maxPos;
            this.counter = new AtomicLong(partitionCount);
        }

        public long claim()
        {
            long v = counter.getAndDecrement();
            if (v <= 0)
                signal();

            return v;
        }
    }

    private void log(long lts, int modifier, Query query)
    {
        try
        {
            validationLog.write("LTS: " + lts + ". Modifier: " + modifier + ". PD: " + query.pd);
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

    @JsonTypeName("parallel_validate_recent_partitions")
    public static class ParallelRecentValidatorConfig implements Configuration.VisitorConfiguration
    {
        public final int partition_count;
        public final int trigger_after;
        public final int queries;
        public final int concurrency;
        public final Configuration.ModelConfiguration modelConfiguration;

        // TODO: make query selector configurable
        @JsonCreator
        public ParallelRecentValidatorConfig(@JsonProperty("partition_count") int partition_count,
                                             @JsonProperty("concurrency") int concurrency,
                                             @JsonProperty("trigger_after") int trigger_after,
                                             @JsonProperty("queries_per_partition") int queries,
                                             @JsonProperty("model") Configuration.ModelConfiguration model)
        {
            this.partition_count = partition_count;
            this.concurrency = concurrency;
            this.queries = Math.max(queries, 1);
            this.trigger_after = trigger_after;
            this.modelConfiguration = model;
        }

        @Override
        public Visitor make(Run run)
        {
            return new ParallelRecentValidator(partition_count, concurrency, trigger_after, queries, run, modelConfiguration);
        }
    }

}
