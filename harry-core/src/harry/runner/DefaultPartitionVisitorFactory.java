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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.ddl.SchemaSpec;
import harry.model.Model;
import harry.model.OpSelectors;
import harry.model.sut.SystemUnderTest;
import harry.operations.CompiledStatement;

public class DefaultPartitionVisitorFactory implements Supplier<PartitionVisitor>
{
    private static final Logger logger = LoggerFactory.getLogger(DefaultPartitionVisitorFactory.class);

    private final Model model;
    private final SystemUnderTest sut;
    private final RowVisitor rowVisitor;
    private final BufferedWriter operationLog;

    // TODO: shutdown properly
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
    private final Supplier<DefaultPartitionVisitor> factory;

    public DefaultPartitionVisitorFactory(Model model,
                                          SystemUnderTest sut,
                                          OpSelectors.PdSelector pdSelector,
                                          OpSelectors.DescriptorSelector descriptorSelector,
                                          SchemaSpec schema,
                                          RowVisitor rowVisitor)
    {
        this.model = model;
        this.sut = sut;
        this.rowVisitor = rowVisitor;
        this.factory = () -> new DefaultPartitionVisitor(pdSelector, descriptorSelector, schema);

        File f = new File("operation.log");
        try
        {
            operationLog = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f)));
        }
        catch (FileNotFoundException e)
        {
            throw new RuntimeException(e);
        }
    }

    public PartitionVisitor get()
    {
        return factory.get();
    }

    private class DefaultPartitionVisitor extends AbstractPartitionVisitor
    {
        private List<String> statements = new ArrayList<>();
        private List<Object> bindings = new ArrayList<>();
        private List<CompletableFuture> futures = new ArrayList<>();

        public DefaultPartitionVisitor(OpSelectors.PdSelector pdSelector,
                                       OpSelectors.DescriptorSelector descriptorSelector,
                                       SchemaSpec schema)
        {
            super(pdSelector, descriptorSelector, schema);
        }

        public void beforeLts(long lts, long pd)
        {
            model.recordEvent(lts, false);
        }

        public void afterLts(long lts, long pd)
        {
            for (CompletableFuture future : futures)
            {
                try
                {
                    // TODO: currently, Cassandra keeps timing out; we definitely need to investigate that, but we need to focus on other things first
                    future.get();
                }
                catch (Throwable t)
                {
                    throw new Model.ValidationException("Couldn't repeat operations within timeout bounds.", t);
                }
            }

            log("LTS: %d. Pd %d. Finished\n", lts, pd);
            model.recordEvent(lts, true);
        }

        public void beforeBatch(long lts, long pd, long m)
        {
            statements = new ArrayList<>();
            bindings = new ArrayList<>();
        }

        public void operation(long lts, long pd, long cd, long m, long opId)
        {
            OpSelectors.OperationKind op = descriptorSelector.operationType(pd, lts, opId);
            CompiledStatement statement = rowVisitor.visitRow(op, lts, pd, cd, opId);

            log(String.format("LTS: %d. Pd %d. Cd %d. M %d. OpId: %d Statement %s\n",
                              lts, pd, cd, m, opId, statement));

            statements.add(statement.cql());
            bindings.addAll(Arrays.asList(statement.bindings()));
        }

        public void afterBatch(long lts, long pd, long m)
        {
            String query = String.join(" ", statements);

            if (statements.size() > 1)
                query = String.format("BEGIN UNLOGGED BATCH\n%s\nAPPLY BATCH;", query);

            Object[] bindingsArray = new Object[bindings.size()];
            bindings.toArray(bindingsArray);

            CompletableFuture<Object[][]> future = new CompletableFuture<>();
            executeAsyncWithRetries(future, new CompiledStatement(query, bindingsArray));
            futures.add(future);

            statements = null;
            bindings = null;
        }
    }

    void executeAsyncWithRetries(CompletableFuture<Object[][]> future, CompiledStatement statement)
    {
        if (sut.isShutdown())
            throw new IllegalStateException("System under test is shut down");

        sut.executeAsync(statement.cql(), statement.bindings())
           .whenComplete((res, t) -> {
               if (t != null)
                   executor.schedule(() -> executeAsyncWithRetries(future, statement), 1, TimeUnit.SECONDS);
               else
                   future.complete(res);
           });
    }

    private void log(String format, Object... objects)
    {
        try
        {
            operationLog.write(String.format(format, objects));
        }
        catch (IOException e)
        {
            // ignore
        }
    }

    public void shutdown()
    {
        executor.shutdown();
        try
        {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}