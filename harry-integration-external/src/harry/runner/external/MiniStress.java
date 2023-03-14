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

package harry.runner.external;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import harry.core.Configuration;
import harry.core.Run;
import harry.model.Model;
import harry.model.OpSelectors;
import harry.model.QuiescentChecker;
import harry.model.sut.external.QuiescentLocalStateChecker;
import harry.operations.Query;
import harry.runner.HarryRunner;
import harry.runner.LockingDataTracker;
import harry.runner.Runner;
import harry.visitors.AllPartitionsValidator;
import harry.visitors.MutatingVisitor;
import harry.visitors.QueryLogger;
import harry.visitors.Visitor;

public class MiniStress
{
    public static void main(String... args) throws Throwable
    {
        File configFile = HarryRunner.loadConfig(args);
        Configuration config = Configuration.fromFile(configFile);

        List<Configuration.VisitorPoolConfiguration> concurrentPools = new ArrayList<>();
        List<Visitor.VisitorFactory> postRunValidation = new ArrayList<>();
        int duration = 60_000;
        for (String arg : args)
        {
            if (arg.startsWith("--write"))
            {
                int concurrency = Integer.parseInt(arg.split("=")[1]);
                concurrentPools.add(new Configuration.VisitorPoolConfiguration("Writer", concurrency, MutatingVisitor::new));
            }
            else if (arg.startsWith("--read"))
            {
                int concurrency = Integer.parseInt(arg.split("=")[1]);
                AtomicLong modifier = new AtomicLong(0);
                concurrentPools.add(new Configuration.VisitorPoolConfiguration("Reader", concurrency, (r) -> {
                    Model model = new QuiescentChecker(r);
                    OpSelectors.DefaultPdSelector pdSelector = (OpSelectors.DefaultPdSelector) r.pdSelector;

                    return () -> {
                        // Wait until we have something to validate
                        if (r.tracker.maxStarted() == 0)
                            return;

                        long randomPd = pdSelector.randomVisitedPd(r.tracker.maxStarted(), modifier.incrementAndGet(), r.schemaSpec);
                        Query query = Query.selectPartition(r.schemaSpec, randomPd, true);
                        model.validate(query);
                        query = Query.selectPartition(r.schemaSpec, randomPd, false);
                        model.validate(query);
                    };
                }));
            }
            else if (arg.startsWith("--duration"))
            {
                duration = Integer.parseInt(arg.split("=")[1]);
            }
            else if (arg.startsWith("--validate-all-local"))
            {
                postRunValidation.add((r) -> new AllPartitionsValidator(r, 10, QuiescentLocalStateChecker::new, QueryLogger.NO_OP));
            }
            else if (arg.startsWith("--validate-all"))
            {
                postRunValidation.add((r) -> new AllPartitionsValidator(r, 10, QuiescentChecker::new, QueryLogger.NO_OP));
            }
        }

        Configuration.ConfigurationBuilder builder = config.unbuild()
                                                           .setRunner(null);
        config = builder.build();
        Run run = config.createRun();

        if (!(run.tracker instanceof LockingDataTracker))
        {
            System.err.println("Concurrent read/write workloads only work with a locking data tracker.");
            System.exit(1);
        }

        System.out.println(Configuration.toYamlString(config));

        int finalDuration = duration;
        new Runner.ChainRunner(run, config,
                               Arrays.asList((r, c) -> new Runner.ConcurrentRunner(r, c, concurrentPools, finalDuration, TimeUnit.MILLISECONDS),
                                             (r, c) -> new Runner.SingleVisitRunner(r, c, postRunValidation)))
        .run();

        run.sut.shutdown();

        Configuration.toFile(new File(String.format("resume-%d.yaml", config.seed)),
                             config.unbuild()
                                   .setClock(run.clock.toConfig())
                                   .setDataTracker(run.tracker.toConfig())
                                   .setDropSchema(false)
                                   .setTruncateTable(false)
                                   .setCreateSchema(false)
                                   .build());
        System.exit(0);
    }

}