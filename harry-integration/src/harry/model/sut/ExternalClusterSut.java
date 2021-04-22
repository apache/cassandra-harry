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

package harry.model.sut;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Configuration;

public class ExternalClusterSut implements SystemUnderTest
{
    public static void init()
    {
        Configuration.registerSubtypes(ExternalClusterSutConfiguration.class);
    }

    private final Session session;
    private final ExecutorService executor;

    // TODO: pass cluster, not session
    public ExternalClusterSut(Session session)
    {
        this(session, 10);
    }

    public ExternalClusterSut(Session session, int threads)
    {
        this.session = session;
        this.executor = Executors.newFixedThreadPool(threads);
    }

    public static ExternalClusterSut create()
    {
        return new ExternalClusterSut(Cluster.builder()
                                             .withQueryOptions(new QueryOptions().setConsistencyLevel(toDriverCl(ConsistencyLevel.QUORUM)))
                                             .addContactPoints("127.0.0.1")
                                             .build()
                                             .connect());
    }

    public boolean isShutdown()
    {
        return session.isClosed();
    }

    public void shutdown()
    {
        session.close();
        executor.shutdown();
        try
        {
            executor.awaitTermination(60, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    // TODO: this is rather simplistic
    public Object[][] execute(String statement, ConsistencyLevel cl, Object... bindings)
    {
        int repeat = 10;
        while (true)
        {
            try
            {
                Statement st = new SimpleStatement(statement, bindings);
                st.setConsistencyLevel(toDriverCl(cl));
                return resultSetToObjectArray(session.execute(st));
            }
            catch (Throwable t)
            {
                if (repeat < 0)
                    throw t;

                t.printStackTrace();
                repeat--;
                // retry unconditionally
            }
        }
    }

    public static Object[][] resultSetToObjectArray(ResultSet rs)
    {
        List<Row> rows = rs.all();
        if (rows.size() == 0)
            return new Object[0][];
        Object[][] results = new Object[rows.size()][];
        for (int i = 0; i < results.length; i++)
        {
            Row row = rows.get(i);
            ColumnDefinitions cds = row.getColumnDefinitions();
            Object[] result = new Object[cds.size()];
            for (int j = 0; j < cds.size(); j++)
            {
                if (!row.isNull(j))
                    result[j] = row.getObject(j);
            }
            results[i] = result;
        }
        return results;
    }

    public CompletableFuture<Object[][]> executeAsync(String statement, ConsistencyLevel cl, Object... bindings)
    {
        CompletableFuture<Object[][]> future = new CompletableFuture<>();
        Statement st = new SimpleStatement(statement, bindings);
        st.setConsistencyLevel(toDriverCl(cl));
        Futures.addCallback(session.executeAsync(st),
                            new FutureCallback<ResultSet>()
                            {
                                public void onSuccess(ResultSet rows)
                                {
                                    future.complete(resultSetToObjectArray(rows));
                                }

                                public void onFailure(Throwable throwable)
                                {
                                    future.completeExceptionally(throwable);
                                }
                            },
                            executor);

        return future;
    }

    public static com.datastax.driver.core.ConsistencyLevel toDriverCl(SystemUnderTest.ConsistencyLevel cl)
    {
        switch (cl)
        {
            case ALL:    return com.datastax.driver.core.ConsistencyLevel.ALL;
            case QUORUM: return com.datastax.driver.core.ConsistencyLevel.QUORUM;
        }
        throw new IllegalArgumentException("Don't know a CL: " + cl);
    }

    @JsonTypeName("external")
    public static class ExternalClusterSutConfiguration implements Configuration.SutConfiguration
    {
        public final String[] hosts;

        public ExternalClusterSutConfiguration(@JsonProperty(value = "hosts") String[] hosts)
        {
            this.hosts = hosts;
        }

        public SystemUnderTest make()
        {
            Cluster cluster = Cluster.builder().addContactPoints(hosts).build();
            Session session = cluster.newSession().init();
            return new ExternalClusterSut(session);
        }
    }
}