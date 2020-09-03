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

package harry.operations;

import java.util.List;

import harry.ddl.ColumnSpec;
import harry.ddl.SchemaSpec;
import harry.generators.DataGenerators;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

public class WriteHelper
{
    public static CompiledStatement inflateInsert(SchemaSpec schema,
                                                  long pd,
                                                  long cd,
                                                  long[] vds,
                                                  long timestamp)
    {
        Object[] partitionKey = schema.inflatePartitionKey(pd);
        Object[] clusteringKey = schema.inflateClusteringKey(cd);
        Object[] regularColumns = schema.inflateRegularColumns(vds);

        Object[] bindings = new Object[schema.allColumns.size()];
        int bindingsCount = 0;
        com.datastax.driver.core.querybuilder.Insert insert = insertInto(schema.keyspace,
                                                                         schema.table);

        bindingsCount += addValue(insert, bindings, schema.partitionKeys, partitionKey, bindingsCount);
        bindingsCount += addValue(insert, bindings, schema.clusteringKeys, clusteringKey, bindingsCount);
        bindingsCount += addValue(insert, bindings, schema.regularColumns, regularColumns, bindingsCount);

        insert.using(timestamp(timestamp));

        // Some of the values were unset
        if (bindingsCount != bindings.length)
        {
            Object[] tmp = new Object[bindingsCount];
            System.arraycopy(bindings, 0, tmp, 0, bindingsCount);
            bindings = tmp;
        }
        return CompiledStatement.create(insert.toString(), bindings);
    }

    private static int addValue(com.datastax.driver.core.querybuilder.Insert insert,
                                Object[] bindings,
                                List<ColumnSpec<?>> columns,
                                Object[] data,
                                int bound)
    {
        assert data.length == columns.size();

        int bindingsCount = 0;
        for (int i = 0; i < data.length; i++)
        {
            if (data[i] == DataGenerators.UNSET_VALUE)
                continue;

            insert.value(columns.get(i).name, bindMarker());
            bindings[bound + bindingsCount] = data[i];
            bindingsCount++;
        }

        return bindingsCount;
    }

    public static CompiledStatement inflateUpdate(SchemaSpec schema,
                                                  long pd,
                                                  long cd,
                                                  long[] vds,
                                                  long timestamp)
    {
        Object[] partitionKey = schema.inflatePartitionKey(pd);
        Object[] clusteringKey = schema.inflateClusteringKey(cd);
        Object[] regularColumns = schema.inflateRegularColumns(vds);

        Object[] bindings = new Object[schema.allColumns.size()];
        int bindingsCount = 0;
        com.datastax.driver.core.querybuilder.Update update = update(schema.keyspace,
                                                                     schema.table);

        bindingsCount += addWith(update, bindings, schema.regularColumns, regularColumns, bindingsCount);
        bindingsCount += addWhere(update, bindings, schema.partitionKeys, partitionKey, bindingsCount);
        bindingsCount += addWhere(update, bindings, schema.clusteringKeys, clusteringKey, bindingsCount);

        update.using(timestamp(timestamp));
        // TODO: TTL
        // ttl.ifPresent(ts -> update.using(ttl(ts)));

        return CompiledStatement.create(update.toString(), bindings);
    }

    private static int addWith(com.datastax.driver.core.querybuilder.Update update,
                               Object[] bindings,
                               List<ColumnSpec<?>> columns,
                               Object[] data,
                               int bound)
    {
        assert data.length == columns.size();

        for (int i = 0; i < data.length; i++)
        {
            update.with(set(columns.get(i).name, bindMarker()));
            bindings[bound + i] = data[i];
        }

        return data.length;
    }

    private static int addWhere(com.datastax.driver.core.querybuilder.Update update,
                                Object[] bindings,
                                List<ColumnSpec<?>> columns,
                                Object[] data,
                                int bound)
    {
        assert data.length == columns.size();

        for (int i = 0; i < data.length; i++)
        {
            update.where().and(eq(columns.get(i).name, bindMarker()));
            bindings[bound + i] = data[i];
        }

        return data.length;
    }
}