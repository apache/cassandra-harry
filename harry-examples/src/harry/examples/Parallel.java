/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package harry.examples;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import harry.core.Configuration;
import harry.model.OpSelectors;
import harry.model.sut.SystemUnderTest;
import harry.model.sut.external.ExternalClusterSut;
import harry.model.sut.injvm.InJvmSutConfiguration;
import harry.runner.HarryRunner;
import harry.runner.external.HarryRunnerExternal;
import harry.visitors.MutatingRowVisitor;
import harry.visitors.ParallelRecentValidator;
import harry.visitors.QueryLogger;

import java.util.concurrent.TimeUnit;

/**
 * This is an example of the parallel runner, where each Harry runner gets a subset of the PartitionDescriptor space.
 *
 * See run instructions in harry.examples.Basic.
 */
public class Parallel
{
    private static int PARALLELISM = 4;
    private static SystemUnderTest SUT;


    public static void main(String[] args) throws Throwable
    {
        SUT = new InJvmSutConfiguration(3, 10, "/tmp/harry").make();

        for (int i = 0; i < PARALLELISM; i++)
        {
            HarryRunner runner = new HarryRunnerExternal();
            Configuration config = Configurations.makeLocalParallelConfiguration(i, PARALLELISM);
            runner.run(config);
        }
    }

    public static class Configurations
    {
        private static Configuration.ConfigurationBuilder IN_JVM_PARALLEL = new Configuration.ConfigurationBuilder()
                .setSeed(1L)
                .setSchemaProvider(new Configuration.FixedSchemaProviderConfiguration(
                        "harry",
                        "test_table",
                        ImmutableMap.of(
                                "pk1", "bigint",
                                "pk2", "ascii"),
                        ImmutableMap.of(
                                "ck1", "ascii",
                                "ck2", "bigint"),
                        ImmutableMap.of(
                                "v1", "ascii",
                                "v2", "bigint",
                                "v3", "ascii",
                                "v4", "bigint"),
                        ImmutableMap.of(
                                "s1", "ascii",
                                "s2", "bigint",
                                "s3", "ascii",
                                "s4", "bigint")
                        ))
                .setDropSchema(false)
                .setCreateSchema(true)
                .setTruncateTable(true)
                .setClock(new Configuration.ApproximateMonotonicClockConfiguration(7300, 1, TimeUnit.SECONDS))
                .setSUT(() -> SUT)
                // This is missing a PartitionDescriptorSelector, since that requires the parallel-worker-index, and
                // total number of workers
                .setClusteringDescriptorSelector(
                        new Configuration.DefaultCDSelectorConfiguration(
                                new Configuration.ConstantDistributionConfig(4),
                                new Configuration.ConstantDistributionConfig(2),
                                1000,
                                ImmutableMap.<OpSelectors.OperationKind, Integer>builder()
                                        .put(OpSelectors.OperationKind.DELETE_RANGE, 1)
                                        .put(OpSelectors.OperationKind.DELETE_SLICE, 1)
                                        .put(OpSelectors.OperationKind.DELETE_ROW, 1)
                                        .put(OpSelectors.OperationKind.DELETE_COLUMN, 1)
                                        .put(OpSelectors.OperationKind.DELETE_PARTITION, 1)
                                        .put(OpSelectors.OperationKind.DELETE_COLUMN_WITH_STATICS, 1)
                                        .put(OpSelectors.OperationKind.INSERT_WITH_STATICS, 50)
                                        .put(OpSelectors.OperationKind.INSERT, 50)
                                        .put(OpSelectors.OperationKind.UPDATE_WITH_STATICS, 50)
                                        .put(OpSelectors.OperationKind.UPDATE, 50)
                                        .build(),
                                null))
                .setRunner(new Configuration.SequentialRunnerConfig(ImmutableList.of(
                        new Configuration.LoggingVisitorConfiguration(MutatingRowVisitor::new),
                        new ParallelRecentValidator.ParallelRecentValidatorConfig(100, 20, 10_000, new Configuration.QuiescentCheckerConfig(), QueryLogger.NoOpQueryLogger::new),
                        new Configuration.AllPartitionsValidatorConfiguration(20, new Configuration.QuiescentCheckerConfig(), QueryLogger.NoOpQueryLogger::new)
                ),
                        5,
                        TimeUnit.MINUTES
                ))
                .setDataTracker(new Configuration.DefaultLockingDataTrackerConfiguration())
                .setMetricReporter(new Configuration.NoOpMetricReporterConfiguration());

        public static Configuration makeLocalParallelConfiguration(int workerIndex, int totalWorkers)
        {
            Configuration.DefaultPDSelectorConfiguration pdSelectorConfig = new Configuration.DefaultPDSelectorConfiguration(10, 100, (long) workerIndex, (long) totalWorkers, null, null);
            return IN_JVM_PARALLEL.setPartitionDescriptorSelector(pdSelectorConfig).build();
        }
    }
}
