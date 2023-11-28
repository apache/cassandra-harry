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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Configuration;
import harry.core.Run;
import harry.model.Model;
import harry.model.OpSelectors;
import harry.model.QuiescentChecker;
import harry.model.sut.SystemUnderTest;
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
    private static final Logger logger = LoggerFactory.getLogger(Logger.class);

    public static void main(String... args) throws Throwable
    {
        File configFile = HarryRunner.loadConfig(args);
        Configuration config = Configuration.fromFile(configFile);

        List<Configuration.VisitorPoolConfiguration> concurrentPools = new ArrayList<>();
        List<Visitor.VisitorFactory> sequentialPools = new ArrayList<>();

        List<Visitor.VisitorFactory> postRunValidation = new ArrayList<>();
        int duration = 60_000;
        for (String arg : args)
        {
            if (arg.startsWith("--write-and-log"))
            {
                assert sequentialPools.isEmpty();
                int concurrency = Integer.parseInt(arg.split("=")[1]);
                concurrentPools.add(new Configuration.VisitorPoolConfiguration("Writer", concurrency, new Configuration.LoggingVisitorConfiguration(new Configuration.MutatingRowVisitorConfiguration())));
            }
            if (arg.startsWith("--write"))
            {
                assert sequentialPools.isEmpty();
                int concurrency = Integer.parseInt(arg.split("=")[1]);
                concurrentPools.add(new Configuration.VisitorPoolConfiguration("Writer", concurrency, MutatingVisitor::new));
            }
            else if (arg.startsWith("--read"))
            {
                assert sequentialPools.isEmpty();
                int concurrency = Integer.parseInt(arg.split("=")[1]);
                concurrentPools.add(new Configuration.VisitorPoolConfiguration("Reader", concurrency, makeValidator()));
            }
            else if (arg.startsWith("--write-before-read"))
            {
                assert concurrentPools.isEmpty();
                sequentialPools.add(MutatingVisitor::new);
                sequentialPools.add(makeValidator());
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

        List<Configuration.RunnerConfiguration> runners = new ArrayList<>();
        if (!concurrentPools.isEmpty())
            runners.add((r, c) -> new Runner.ConcurrentRunner(r, c, concurrentPools, finalDuration, TimeUnit.MILLISECONDS));
        if (!sequentialPools.isEmpty())
            runners.add((r, c) -> new Runner.SequentialRunner(r, c, sequentialPools, finalDuration, TimeUnit.MILLISECONDS));
        runners.add((r, c) -> new Runner.SingleVisitRunner(r, c, postRunValidation));

        new Runner.ChainRunner(run, config, runners).run();

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

    private static Configuration.VisitorConfiguration makeValidator()
    {
        AtomicLong modifier = new AtomicLong(0);
        return (r) -> {
            Model model = new QuiescentChecker(r);
            OpSelectors.DefaultPdSelector pdSelector = (OpSelectors.DefaultPdSelector) r.pdSelector;

            return () -> {
                long mod = modifier.incrementAndGet();
                // Wait until we have something to validate
                if (mod % 10_000 == 0)
                    logger.info("Validated {} times", mod);

                long randomPd = pdSelector.randomVisitedPd(r.tracker.maxStarted(), mod, r.schemaSpec);
                Query query = Query.selectPartition(r.schemaSpec, randomPd, true);
                model.validate(query);
                query = Query.selectPartition(r.schemaSpec, randomPd, false);
                model.validate(query);
            };
        };
    }
}