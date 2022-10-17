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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Run;
import harry.model.OpSelectors;
import harry.model.sut.SystemUnderTest;
import harry.operations.CompiledStatement;
import harry.runner.DataTracker;

public class MutatingVisitor extends GeneratingVisitor
{
    private static final Logger logger = LoggerFactory.getLogger(MutatingVisitor.class);

    public MutatingVisitor(Run run,
                           OperationExecutor.RowVisitorFactory rowVisitorFactory)
    {
        this(run, new MutatingVisitExecutor(run, rowVisitorFactory.make(run)));
    }

    public MutatingVisitor(Run run,
                           VisitExecutor visitExecutor)
    {
        super(run, visitExecutor);
    }

    public static class MutatingVisitExecutor extends VisitExecutor
    {
        private final List<String> statements = new ArrayList<>();
        private final List<Object> bindings = new ArrayList<>();

        private final List<CompletableFuture<?>> futures = new ArrayList<>();

        protected final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

        protected final OpSelectors.DescriptorSelector descriptorSelector;
        protected final DataTracker tracker;
        protected final SystemUnderTest sut;
        protected final OperationExecutor rowVisitor;
        private final int maxRetries = 10;

        public MutatingVisitExecutor(Run run, OperationExecutor rowVisitor)
        {
            this.descriptorSelector = run.descriptorSelector;
            this.tracker = run.tracker;
            this.sut = run.sut;
            this.rowVisitor = rowVisitor;
        }

        @Override
        public void beforeLts(long lts, long pd)
        {
            tracker.started(lts);
        }

        @Override
        public void afterLts(long lts, long pd)
        {
            for (CompletableFuture<?> future : futures)
            {
                try
                {
                    future.get(10, TimeUnit.SECONDS);
                }
                catch (Throwable t)
                {
                    throw new IllegalStateException("Couldn't repeat operations within timeout bounds.", t);
                }
            }
            futures.clear();
            tracker.finished(lts);
        }

        @Override
        public void beforeBatch(long lts, long pd, long m)
        {
            statements.clear();
            bindings.clear();
        }

        @Override
        public void operation(long lts, long pd, long cd, long m, long opId, OpSelectors.OperationKind opType)
        {
            CompiledStatement statement = operationInternal(lts, pd, cd, m, opId, opType);

            statements.add(statement.cql());
            Collections.addAll(bindings, statement.bindings());
        }

        protected CompiledStatement operationInternal(long lts, long pd, long cd, long m, long opId, OpSelectors.OperationKind opType)
        {
            return rowVisitor.perform(opType, lts, pd, cd, opId);
        }

        @Override
        public void afterBatch(long lts, long pd, long m)
        {
            if (statements.isEmpty())
            {
                logger.warn("Encountered an empty batch on {}", lts);
                return;
            }

            String query = String.join(" ", statements);

            if (statements.size() > 1)
                query = String.format("BEGIN UNLOGGED BATCH\n%s\nAPPLY BATCH;", query);

            Object[] bindingsArray = new Object[bindings.size()];
            bindings.toArray(bindingsArray);

            CompletableFuture<Object[][]> future = new CompletableFuture<>();
            executeAsyncWithRetries(lts, pd, future, new CompiledStatement(query, bindingsArray));
            futures.add(future);

            statements.clear();
            bindings.clear();
        }

        protected void executeAsyncWithRetries(long lts, long pd, CompletableFuture<Object[][]> future, CompiledStatement statement)
        {
            executeAsyncWithRetries(future, statement, 0);
        }

        private void executeAsyncWithRetries(CompletableFuture<Object[][]> future, CompiledStatement statement, int retries)
        {
            if (sut.isShutdown())
                throw new IllegalStateException("System under test is shut down");

            if (retries > this.maxRetries)
                throw new IllegalStateException(String.format("Can not execute statement %s after %d retries", statement, retries));

            sut.executeAsync(statement.cql(), SystemUnderTest.ConsistencyLevel.QUORUM, statement.bindings())
               .whenComplete((res, t) -> {
                   if (t != null)
                   {
                       logger.error("Caught message while trying to execute " +  statement, t);
                       int delaySecs = 1;
                       executor.schedule(() -> executeAsyncWithRetries(future, statement, retries + 1), delaySecs, TimeUnit.SECONDS);
                       logger.info("Scheduled retry to happen with delay {} seconds", delaySecs);
                   }else
                       future.complete(res);
               });
        }

        public void shutdown() throws InterruptedException
        {
            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
    }
}
