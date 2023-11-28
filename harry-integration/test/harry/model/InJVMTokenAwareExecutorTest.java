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

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.model.sut.SystemUnderTest;
import harry.model.sut.TokenPlacementModel;
import harry.model.sut.injvm.InJVMTokenAwareVisitExecutor;
import harry.model.sut.injvm.InJvmSut;
import harry.model.sut.injvm.InJvmSutBase;
import harry.runner.Runner;
import harry.runner.UpToLtsRunner;
import harry.visitors.MutatingVisitor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;

public class InJVMTokenAwareExecutorTest extends IntegrationTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(InJVMTokenAwareExecutorTest.class);

    @BeforeClass
    public static void before() throws Throwable
    {
        cluster = Cluster.build()
                         .withNodes(5)
                         .withConfig((cfg) -> InJvmSutBase.defaultConfig().accept(cfg.with(Feature.GOSSIP, Feature.NETWORK)))
                         .createWithoutStarting();
        cluster.setUncaughtExceptionsFilter(t -> {
            logger.error("Caught exception, reporting during shutdown. Ignoring.", t);
            return true;
        });
        cluster.startup();
        cluster = init(cluster);
        sut = new InJvmSut(cluster, 20);
    }

    @Override
    @Before
    public void beforeEach()
    {
        cluster.schemaChange("DROP KEYSPACE IF EXISTS harry");
        cluster.schemaChange("CREATE KEYSPACE harry WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
    }

    @Test
    public void testRepair() throws Throwable
    {
        Supplier<SchemaSpec> schemaGen = SchemaGenerators.progression(1);
        for (int cnt = 0; cnt < SchemaGenerators.DEFAULT_RUNS; cnt++)
        {
            SchemaSpec schema = schemaGen.get();
            Configuration.ConfigurationBuilder builder = sharedConfiguration(cnt, schema);

            Configuration configuration = builder.build();
            Run run = configuration.createRun();
            run.sut.schemaChange(run.schemaSpec.compile().cql());

            Runner.chain(configuration,
                         UpToLtsRunner.factory(MutatingVisitor.factory(InJVMTokenAwareVisitExecutor.factory(new Configuration.MutatingRowVisitorConfiguration(),
                                                                                                            SystemUnderTest.ConsistencyLevel.NODE_LOCAL,
                                                                                                            new TokenPlacementModel.SimpleReplicationFactor(3))),
                                               10_000, 2, TimeUnit.SECONDS),
                         Runner.single(RepairingLocalStateValidator.factoryForTests(5, QuiescentChecker::new)))
                  .run();
        }
    }
}
