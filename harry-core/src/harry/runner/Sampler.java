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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.SchemaSpec;
import harry.generators.RngUtils;
import harry.model.OpSelectors;
import harry.model.SelectHelper;
import harry.model.sut.SystemUnderTest;

public class Sampler implements PartitionVisitor
{
    private static final Logger logger = LoggerFactory.getLogger(AllPartitionsValidator.class);

    private final SystemUnderTest sut;
    // TODO: move maxPos to data tracker since we seem to use quite a lot?
    private final AtomicLong maxPos = new AtomicLong(-1);
    private final OpSelectors.PdSelector pdSelector;
    private final SchemaSpec schema;
    private final int triggerAfter;
    private final int samplePartitions;
    public Sampler(Run run,
                   int triggerAfter,
                   int samplePartitions)
    {
        this.sut = run.sut;
        this.pdSelector = run.pdSelector;
        this.schema = run.schemaSpec;
        this.triggerAfter = triggerAfter;
        this.samplePartitions = samplePartitions;
    }

    public void visitPartition(long lts)
    {
        maxPos.updateAndGet(current -> Math.max(pdSelector.positionFor(lts), current));

        if (triggerAfter > 0 && lts % triggerAfter == 0)
        {
            long max = maxPos.get();
            DescriptiveStatistics ds = new DescriptiveStatistics();
            int empty = 0;

            long n = RngUtils.next(lts);
            for (long i = 0; i < this.samplePartitions; i++)
            {
                long posLts = pdSelector.minLtsAt(RngUtils.asInt(n, (int) max));
                n = RngUtils.next(n);
                // TODO: why not just pd at pos?
                long pd = pdSelector.pd(posLts, schema);
                long count = (long) sut.execute(SelectHelper.count(schema, pd), SystemUnderTest.ConsistencyLevel.ONE)[0][0];
                if (count == 0)
                    empty++;
                ds.addValue(count);
            }
            logger.info("Visited {} partitions (sampled {} empty out of {}), with mean size of {}. Median: {}. Min: {}. Max: {}",
                        max, empty, samplePartitions, ds.getMean(), ds.getPercentile(0.5), ds.getMin(), ds.getMax());
        }
    }

    public void shutdown() throws InterruptedException
    {
    }

    @JsonTypeName("sampler")
    public static class SamplerConfiguration implements Configuration.PartitionVisitorConfiguration
    {
        public final int trigger_after;
        public final int sample_partitions;

        @JsonCreator
        public SamplerConfiguration(@JsonProperty(value = "trigger_after", defaultValue = "1000") int trigger_after,
                                    @JsonProperty(value = "sample_partitions", defaultValue = "10") int sample_partitions)
        {
            this.trigger_after = trigger_after;
            this.sample_partitions = sample_partitions;
        }

        public PartitionVisitor make(Run run)
        {
            return new Sampler(run, trigger_after, sample_partitions);
        }
    }
}
