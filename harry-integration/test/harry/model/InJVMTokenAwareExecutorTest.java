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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.model.sut.InJVMTokenAwareVisitExecutor;
import harry.model.sut.InJvmSut;
import harry.model.sut.SystemUnderTest;
import harry.runner.RepairingLocalStateValidator;
import harry.visitors.MutatingVisitor;
import harry.visitors.Visitor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;

public class InJVMTokenAwareExecutorTest extends IntegrationTestBase
{
    @BeforeClass
    public static void before() throws Throwable
    {
        cluster = init(Cluster.build()
                              .withNodes(5)
                              .withConfig((cfg) -> cfg.with(Feature.GOSSIP, Feature.NETWORK))
                              .start());
        sut = new InJvmSut(cluster, 1);
    }

    @Override
    @Before
    public void beforeEach()
    {
        cluster.schemaChange("DROP KEYSPACE IF EXISTS harry");
        cluster.schemaChange("CREATE KEYSPACE harry WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};");
    }

    @Test
    public void testRepair()
    {
        Supplier<SchemaSpec> schemaGen = SchemaGenerators.progression(1);
        for (int cnt = 0; cnt < SchemaGenerators.DEFAULT_RUNS; cnt++)
        {
            SchemaSpec schema = schemaGen.get();
            Configuration.ConfigurationBuilder builder = sharedConfiguration(cnt, schema);

            Configuration configuration = builder.build();
            Run run = configuration.createRun();
            run.sut.schemaChange(run.schemaSpec.compile().cql());

            Visitor visitor = new MutatingVisitor(run, new InJVMTokenAwareVisitExecutor(run,
                                                                                        new Configuration.MutatingRowVisitorConfiguration(),
                                                                                        SystemUnderTest.ConsistencyLevel.NODE_LOCAL));

            OpSelectors.MonotonicClock clock = run.clock;
            long maxPd = 0;
            for (int i = 0; i < 10000; i++)
            {
                visitor.visit();
                maxPd = Math.max(maxPd, run.pdSelector.positionFor(clock.peek()));
            }

            RepairingLocalStateValidator validator = new RepairingLocalStateValidator(5, 1, run, new Configuration.QuiescentCheckerConfig());
            validator.visit();
        }

    }
}
