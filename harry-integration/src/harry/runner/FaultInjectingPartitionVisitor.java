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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Configuration;
import harry.core.Run;
import harry.model.sut.InJvmSut;
import harry.model.sut.MixedVersionInJvmSut;
import harry.model.sut.SystemUnderTest;
import harry.operations.CompiledStatement;

public class FaultInjectingPartitionVisitor extends LoggingPartitionVisitor
{
    public static void init()
    {
        Configuration.registerSubtypes(FaultInjectingPartitionVisitorConfiguration.class);
    }

    @JsonTypeName("fault_injecting")
    public static class FaultInjectingPartitionVisitorConfiguration extends Configuration.MutatingPartitionVisitorConfiguation
    {
        @JsonCreator
        public FaultInjectingPartitionVisitorConfiguration(@JsonProperty("row_visitor") Configuration.RowVisitorConfiguration row_visitor)
        {
            super(row_visitor);
        }

        @Override
        public PartitionVisitor make(Run run)
        {
            return new FaultInjectingPartitionVisitor(run, row_visitor);
        }
    }

    private final AtomicInteger cnt = new AtomicInteger();

    private final SystemUnderTest.FaultInjectingSut sut;

    public FaultInjectingPartitionVisitor(Run run, Operation.RowVisitorFactory rowVisitorFactory)
    {
        super(run, rowVisitorFactory);
        this.sut = (SystemUnderTest.FaultInjectingSut) run.sut;
    }

    void executeAsyncWithRetries(CompletableFuture<Object[][]> originator, CompiledStatement statement)
    {
        executeAsyncWithRetries(originator, statement, true);
    }

    void executeAsyncWithRetries(CompletableFuture<Object[][]> originator, CompiledStatement statement, boolean allowFailures)
    {
        if (sut.isShutdown())
            throw new IllegalStateException("System under test is shut down");

        CompletableFuture<Object[][]> future;
        if (allowFailures && cnt.getAndIncrement() % 2 == 0)
        {
            future = sut.executeAsyncWithWriteFailure(statement.cql(), SystemUnderTest.ConsistencyLevel.QUORUM, statement.bindings());
        }
        else
        {
            future = sut.executeAsync(statement.cql(), SystemUnderTest.ConsistencyLevel.QUORUM, statement.bindings());
        }

        future.whenComplete((res, t) -> {
               if (t != null)
                   executor.schedule(() -> executeAsyncWithRetries(originator, statement, false), 1, TimeUnit.SECONDS);
               else
                   originator.complete(res);
           });
    }
}
