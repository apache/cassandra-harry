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

import java.util.function.Supplier;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import harry.core.Configuration;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.model.clock.OffsetClock;
import harry.model.sut.InJvmSut;
import org.apache.cassandra.distributed.Cluster;

public class IntegrationTestBase extends TestBaseImpl
{
    protected static Cluster cluster;
    protected static InJvmSut sut;

    @BeforeClass
    public static void before() throws Throwable
    {
        cluster = init(Cluster.build()
                              .withNodes(3)
                              .start());
        sut = new InJvmSut(cluster, 1);
    }

    @AfterClass
    public static void afterClass()
    {
        sut.shutdown();
    }

    @Before
    public void beforeEach()
    {
        cluster.schemaChange("DROP KEYSPACE IF EXISTS harry");
        cluster.schemaChange("CREATE KEYSPACE harry WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
    }

    private static long seed = 0;
    public static Supplier<Configuration.ConfigurationBuilder> sharedConfiguration()
    {
        Supplier<SchemaSpec> specGenerator = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        return () -> {
            SchemaSpec schemaSpec = specGenerator.get();
            return sharedConfiguration(seed, schemaSpec);
        };
    }

    public static Configuration.ConfigurationBuilder sharedConfiguration(long seed, SchemaSpec schema)
    {
        return new Configuration.ConfigurationBuilder().setSeed(seed)
                                                       .setClock(() -> new OffsetClock(100000))
                                                       .setCreateSchema(true)
                                                       .setTruncateTable(false)
                                                       .setDropSchema(true)
                                                       .setSchemaProvider(seed1 -> schema)
                                                       .setClusteringDescriptorSelector((builder) -> {
                                                           builder
                                                           .setNumberOfModificationsDistribution(new Configuration.ConstantDistributionConfig(1))
                                                           .setRowsPerModificationDistribution(new Configuration.ConstantDistributionConfig(1))
                                                           .setOperationKindWeights(new Configuration.OperationKindSelectorBuilder()
                                                                                    .addWeight(OpSelectors.OperationKind.DELETE_ROW, 10)
                                                                                    .addWeight(OpSelectors.OperationKind.DELETE_COLUMN, 10)
                                                                                    .addWeight(OpSelectors.OperationKind.WRITE, 80)
                                                                                    .build())
                                                           .setMaxPartitionSize(100);
                                                       })
                                                       .setPartitionDescriptorSelector(new Configuration.DefaultPDSelectorConfiguration(1, 200))
                                                       .setSUT(() -> sut);
    }
}
