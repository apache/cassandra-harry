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

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import harry.core.Configuration;
import harry.core.Run;
import harry.corruptor.AddExtraRowCorruptor;
import harry.corruptor.ChangeValueCorruptor;
import harry.corruptor.HideRowCorruptor;
import harry.corruptor.HideValueCorruptor;
import harry.corruptor.QueryResponseCorruptor;
import harry.corruptor.ShowValueCorruptor;
import harry.ddl.SchemaGenerators;
import harry.runner.MutatingPartitionVisitor;
import harry.runner.MutatingRowVisitor;
import harry.runner.PartitionVisitor;
import harry.runner.Query;
import harry.runner.QueryGenerator;

import static harry.corruptor.QueryResponseCorruptor.SimpleQueryResponseCorruptor;

@RunWith(Parameterized.class)
public class QuerySelectorNegativeTest extends IntegrationTestBase
{
    private final int ltss = 1000;

    private final Random rnd = new Random();

    private final QueryResponseCorruptorFactory corruptorFactory;

    public QuerySelectorNegativeTest(QueryResponseCorruptorFactory corruptorFactory)
    {
        this.corruptorFactory = corruptorFactory;
    }

    @Parameterized.Parameters
    public static Collection<QueryResponseCorruptorFactory> source()
    {
        return Arrays.asList((run) -> new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                       run.clock,
                                                                       ChangeValueCorruptor::new),
                             (run) -> new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                       run.clock,
                                                                       HideValueCorruptor::new),
                             (run) -> new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                       run.clock,
                                                                       ShowValueCorruptor::new),
                             (run) -> new SimpleQueryResponseCorruptor(run.schemaSpec,
                                                                       run.clock,
                                                                       HideRowCorruptor::new),
                             (run) -> new AddExtraRowCorruptor(run.schemaSpec,
                                                               run.clock,
                                                               run.descriptorSelector));
    }

    interface QueryResponseCorruptorFactory
    {
        QueryResponseCorruptor create(Run run);
    }

    @Test
    public void selectRows()
    {
        Map<Query.QueryKind, Integer> stats = new HashMap<>();
        Supplier<Configuration.ConfigurationBuilder> gen = sharedConfiguration();

        int rounds = SchemaGenerators.DEFAULT_RUNS;
        int failureCounter = 0;
        outer:
        for (int counter = 0; counter < rounds; counter++)
        {
            beforeEach();
            Configuration config = gen.get()
                                      .setClusteringDescriptorSelector((builder) -> {
                                          builder.setMaxPartitionSize(2000);
                                      })
                                      .build();
            Run run = config.createRun();
            run.sut.schemaChange(run.schemaSpec.compile().cql());

            OpSelectors.MonotonicClock clock = run.clock;

            PartitionVisitor partitionVisitor = new MutatingPartitionVisitor(run, MutatingRowVisitor::new);
            Model model = new ExhaustiveChecker(run);

            QueryResponseCorruptor corruptor = this.corruptorFactory.create(run);

            for (int i = 0; i < ltss; i++)
            {
                long lts = clock.nextLts();
                partitionVisitor.visitPartition(lts);
            }

            while (true)
            {
                long verificationLts = rnd.nextInt(1000);
                QueryGenerator queryGen = new QueryGenerator(run.schemaSpec,
                                                             run.pdSelector,
                                                             run.descriptorSelector,
                                                             run.rng);

                QueryGenerator.TypedQueryGenerator querySelector = new QueryGenerator.TypedQueryGenerator(run.rng, queryGen);

                Query query = querySelector.inflate(verificationLts, counter);

                model.validate(query);

                if (!corruptor.maybeCorrupt(query, run.sut))
                    continue;

                try
                {
                    model.validate(query);
                    Assert.fail("Should've failed");
                }
                catch (Throwable t)
                {
                    // expected
                    failureCounter++;
                    stats.compute(query.queryKind, (Query.QueryKind kind, Integer cnt) -> cnt == null ? 1 : (cnt + 1));
                    continue outer;
                }
            }
        }

        Assert.assertTrue(String.format("Seen only %d failures", failureCounter),
                          failureCounter > (rounds * 0.8));
    }
}