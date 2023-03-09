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

package harry.model.sut.external;

import com.datastax.driver.core.*;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import harry.core.Configuration;
import harry.ddl.SchemaSpec;
import harry.model.sut.SystemUnderTest;
import harry.model.sut.TokenPlacementModel;
import harry.util.ByteUtils;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class ExternalClusterSut implements SystemUnderTest
{
    private final Session session;
    private final ExecutorService executor;

    public ExternalClusterSut(Session session)
    {
        this(session, 10);
    }

    public ExternalClusterSut(Session session, int threads)
    {
        this.session = session;
        this.executor = Executors.newFixedThreadPool(threads);
    }

    public Session session()
    {
        return session;
    }

    public static ExternalClusterSut create(ExternalSutConfiguration config)
    {
        // TODO: close Cluster and Session!
        return new ExternalClusterSut(Cluster.builder()
                                             .withQueryOptions(new QueryOptions().setConsistencyLevel(toDriverCl(ConsistencyLevel.QUORUM)))
                                             .addContactPoints(config.contactPoints)
                                             .withPort(config.port)
                                             .withCredentials(config.username, config.password)
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
        executor.shutdownNow();
        try
        {
            executor.awaitTermination(10, TimeUnit.SECONDS);
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
                return resultSetToObjectArray(session.execute(statement, bindings));
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

    public Object[][] executeNodeLocal(String statement, Predicate<Host> selector, Object... bindings)
    {
        int repeat = 10;
        while (true)
        {
            try
            {
                Statement st = new SimpleStatement(statement, bindings);
                boolean selected = false;
                for (Host host : session.getCluster().getMetadata().getAllHosts())
                {
                    if (selector.test(host))
                    {
                        st.setHost(host);
                        selected = true;
                        break;
                    }
                }
                assert selected;
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
        Futures.addCallback(session.executeAsync(statement, bindings),
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

    @JsonTypeName("external")
    public static class ExternalSutConfiguration implements Configuration.SutConfiguration
    {

        private final String contactPoints;
        private final int port;
        private final String username;
        private final String password;

        @JsonCreator
        public ExternalSutConfiguration(@JsonProperty(value = "contact_points") String contactPoints,
                                        @JsonProperty(value = "port") int port,
                                        @JsonProperty(value = "username") String username,
                                        @JsonProperty(value = "password") String password)
        {
            this.contactPoints = contactPoints;
            this.port = port;
            this.username = username;
            this.password = password;
        }

        public SystemUnderTest make()
        {
            return ExternalClusterSut.create(this);
        }
    }

    public static com.datastax.driver.core.ConsistencyLevel toDriverCl(SystemUnderTest.ConsistencyLevel cl)
    {
        switch (cl)
        {
            case ALL:
                return com.datastax.driver.core.ConsistencyLevel.ALL;
            case QUORUM:
                return com.datastax.driver.core.ConsistencyLevel.QUORUM;
        }
        throw new IllegalArgumentException("Don't know a CL: " + cl);
    }
}