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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.concurrent.Uninterruptibles;
import harry.core.Configuration;
import harry.core.Run;
import harry.model.OpSelectors;
import harry.model.sut.SystemUnderTest;
import harry.operations.CompiledStatement;
import harry.runner.DataTracker;

public class MutatingVisitor extends GeneratingVisitor
{
    private static final Logger logger = LoggerFactory.getLogger(MutatingVisitor.class);

    public MutatingVisitor(Run run)
    {
        this(run, MutatingRowVisitor::new);
    }

    public MutatingVisitor(Run run,
                           OperationExecutor.RowVisitorFactory rowVisitorFactory)
    {
        this(run, new MutatingVisitExecutor(run, rowVisitorFactory.make(run)));
    }

    public static Configuration.VisitorConfiguration factory()
    {
        return MutatingVisitor::new;
    }

    public static Configuration.VisitorConfiguration factory(Function<Run, VisitExecutor> rowVisitorFactory)
    {
        return (r) -> new MutatingVisitor(r, rowVisitorFactory.apply(r));
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

        protected final OpSelectors.DescriptorSelector descriptorSelector;
        protected final DataTracker tracker;
        protected final SystemUnderTest sut;
        protected final OperationExecutor rowVisitor;
        protected final SystemUnderTest.ConsistencyLevel consistencyLevel;
        private final int maxRetries = 10;

        public MutatingVisitExecutor(Run run, OperationExecutor rowVisitor)
        {
            this(run, rowVisitor, SystemUnderTest.ConsistencyLevel.QUORUM);
        }

        public MutatingVisitExecutor(Run run, OperationExecutor rowVisitor, SystemUnderTest.ConsistencyLevel consistencyLevel)
        {
            this.descriptorSelector = run.descriptorSelector;
            this.tracker = run.tracker;
            this.sut = run.sut;
            this.rowVisitor = rowVisitor;
            this.consistencyLevel = consistencyLevel;
        }

        @Override
        public void beforeLts(long lts, long pd)
        {
            tracker.beginModification(lts);
        }

        @Override
        public void afterLts(long lts, long pd)
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
            statements.clear();
            bindings.clear();

            executeWithRetries(lts, pd, new CompiledStatement(query, bindingsArray));
            tracker.endModification(lts);
        }

        @Override
        public void operation(long lts, long pd, long cd, long opId, OpSelectors.OperationKind opType)
        {
            CompiledStatement statement = operationInternal(lts, pd, cd, opId, opType);
            statements.add(statement.cql());
            Collections.addAll(bindings, statement.bindings());
        }

        protected CompiledStatement operationInternal(long lts, long pd, long cd, long opId, OpSelectors.OperationKind opType)
        {
            return rowVisitor.perform(opType, lts, pd, cd, opId);
        }

        protected Object[][] executeWithRetries(long lts, long pd, CompiledStatement statement)
        {
            if (sut.isShutdown())
                throw new IllegalStateException("System under test is shut down");

            int retries = 0;

            while (retries++ < maxRetries)
            {
                try
                {
                    return sut.execute(statement.cql(), consistencyLevel, statement.bindings());
                }
                catch (Throwable t)
                {
                    int delaySecs = 1;
                    logger.error(String.format("Caught message while trying to execute: %s. Scheduled to retry in %s seconds", statement, delaySecs), t);
                    Uninterruptibles.sleepUninterruptibly(delaySecs, TimeUnit.SECONDS);
                }
            }

            throw new IllegalStateException(String.format("Can not execute statement %s after %d retries", statement, retries));
        }

        public void shutdown() throws InterruptedException
        {
        }
    }
}
