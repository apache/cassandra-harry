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

package harry.model.sut.injvm;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Run;
import harry.ddl.SchemaSpec;
import harry.model.sut.SystemUnderTest;
import harry.model.sut.TokenPlacementModel;
import harry.operations.CompiledStatement;
import harry.util.ByteUtils;
import harry.util.TokenUtil;
import harry.visitors.GeneratingVisitor;
import harry.visitors.LoggingVisitor;
import harry.visitors.OperationExecutor;
import harry.visitors.VisitExecutor;
import harry.visitors.Visitor;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstance;

import static harry.model.sut.TokenPlacementModel.peerStateToNodes;
import static harry.model.sut.TokenPlacementModel.getReplicas;

public class InJVMTokenAwareVisitExecutor extends LoggingVisitor.LoggingVisitorExecutor
{
    private final InJvmSut sut;
    private final int rf;
    private final SystemUnderTest.ConsistencyLevel cl;
    private final SchemaSpec schema;
    private final int MAX_RETRIES = 10;

    public static Function<Run, VisitExecutor> factory(OperationExecutor.RowVisitorFactory rowVisitorFactory,
                                                       SystemUnderTest.ConsistencyLevel cl,
                                                       int rf)
    {
        return (run) -> new InJVMTokenAwareVisitExecutor(run, rowVisitorFactory, cl, rf);
    }

    public InJVMTokenAwareVisitExecutor(Run run,
                                        OperationExecutor.RowVisitorFactory rowVisitorFactory,
                                        SystemUnderTest.ConsistencyLevel cl,
                                        int rf)
    {
        super(run, rowVisitorFactory.make(run));
        this.sut = (InJvmSut) run.sut;
        this.schema = run.schemaSpec;
        this.cl = cl;
        this.rf = rf;
    }

    @Override
    protected void executeAsyncWithRetries(long lts, long pd, CompletableFuture<Object[][]> future, CompiledStatement statement)
    {
        executeAsyncWithRetries(lts, pd, future, statement, 0);
    }

    private void executeAsyncWithRetries(long lts, long pd, CompletableFuture<Object[][]> future, CompiledStatement statement, int retries)
    {
        if (sut.isShutdown())
            throw new IllegalStateException("System under test is shut down");

        if (retries > this.MAX_RETRIES)
            throw new IllegalStateException(String.format("Can not execute statement %s after %d retries", statement, retries));

        Object[] pk =  schema.inflatePartitionKey(pd);
        List<TokenPlacementModel.Node> replicas = getReplicas(getRing(), TokenUtil.token(ByteUtils.compose(ByteUtils.objectsToBytes(pk))));

        TokenPlacementModel.Node replica = replicas.get((int) (lts % replicas.size()));
        if (cl == SystemUnderTest.ConsistencyLevel.NODE_LOCAL)
        {
            future.complete(executeNodeLocal(statement.cql(), replica, statement.bindings()));
        }
        else
        {
            CompletableFuture.supplyAsync(() ->  sut.cluster
                                                 .stream()
                                                 .filter((n) -> n.config().broadcastAddress().toString().contains(replica.id))
                                                 .findFirst()
                                                 .get()
                                                 .coordinator()
                                                 .execute(statement.cql(), InJvmSut.toApiCl(cl), statement.bindings()), executor)
                             .whenComplete((res, t) ->
                                           {
                                               if (t != null)
                                                   executor.schedule(() -> executeAsyncWithRetries(lts, pd, future, statement, retries + 1), 1, TimeUnit.SECONDS);
                                               else
                                                   future.complete(res);
                                           });
        }
    }

    protected NavigableMap<TokenPlacementModel.Range, List<TokenPlacementModel.Node>> getRing()
    {
        List<TokenPlacementModel.Node> other = peerStateToNodes(sut.cluster.coordinator(1).execute("select peer, tokens from system.peers", ConsistencyLevel.ONE));
        List<TokenPlacementModel.Node> self = peerStateToNodes(sut.cluster.coordinator(1).execute("select broadcast_address, tokens from system.local", ConsistencyLevel.ONE));
        List<TokenPlacementModel.Node> all = new ArrayList<>();
        all.addAll(self);
        all.addAll(other);
        return TokenPlacementModel.replicate(all, rf);
    }

    protected Object[][] executeNodeLocal(String statement, TokenPlacementModel.Node node, Object... bindings)
    {
        IInstance instance = sut.cluster
                             .stream()
                             .filter((n) -> n.config().broadcastAddress().toString().contains(node.id))
                             .findFirst()
                             .get();
        return instance.executeInternal(statement, bindings);
    }


    @JsonTypeName("in_jvm_token_aware")
    public static class Configuration implements harry.core.Configuration.VisitorConfiguration
    {
        public final harry.core.Configuration.RowVisitorConfiguration row_visitor;
        public final SystemUnderTest.ConsistencyLevel consistency_level;
        public final int rf;
        @JsonCreator
        public Configuration(@JsonProperty("row_visitor") harry.core.Configuration.RowVisitorConfiguration rowVisitor,
                             @JsonProperty("consistency_level") SystemUnderTest.ConsistencyLevel consistencyLevel,
                             @JsonProperty("rf") int rf)
        {
            this.row_visitor = rowVisitor;
            this.consistency_level = consistencyLevel;
            this.rf = rf;
        }

        @Override
        public Visitor make(Run run)
        {
            return new GeneratingVisitor(run, new InJVMTokenAwareVisitExecutor(run, row_visitor, consistency_level, rf));
        }
    }
}