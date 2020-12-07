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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import harry.model.sut.SystemUnderTest;

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

    public static ExternalClusterSut create()
    {
        // TODO: close Cluster and Session!
        return new ExternalClusterSut(Cluster.builder()
                                             .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.QUORUM))
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
    public Object[][] execute(String statement, Object... bindings)
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


    private static Object[][] resultSetToObjectArray(ResultSet rs)
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

    public CompletableFuture<Object[][]> executeAsync(String statement, Object... bindings)
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
}