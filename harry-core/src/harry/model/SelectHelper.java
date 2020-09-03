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

package harry.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import harry.data.ResultSetRow;
import harry.ddl.ColumnSpec;
import harry.ddl.SchemaSpec;
import harry.model.sut.SystemUnderTest;
import harry.operations.CompiledStatement;
import harry.operations.Relation;
import harry.runner.Query;

public class SelectHelper
{
    public static CompiledStatement select(SchemaSpec schema, long pd)
    {
        return select(schema, pd, Collections.emptyList(), false, true);
    }

    /**
     * Here, {@code reverse} should be understood not in ASC/DESC sense, but rather in terms
     * of how we're going to iterate through this partition (in other words, if first clustering component order
     * is DESC, we'll iterate in ASC order)
     */
    public static CompiledStatement select(SchemaSpec schema, long pd, List<Relation> relations, boolean reverse, boolean includeWriteTime)
    {
        Select.Selection select = QueryBuilder.select();
        for (ColumnSpec<?> column : schema.allColumns)
            select.column(column.name);

        if (includeWriteTime)
        {
            for (ColumnSpec<?> column : schema.regularColumns)
                select.writeTime(column.name);
        }

        Select.Where where = select.from(schema.keyspace, schema.table).where();
        List<Object> bindings = new ArrayList<>();

        addRelations(schema, where, bindings, pd, relations);
        addOrderBy(schema, where, reverse);

        Object[] bindingsArr = bindings.toArray(new Object[bindings.size()]);
        return new CompiledStatement(where.toString(), bindingsArr);
    }

    private static void addRelations(SchemaSpec schema, Select.Where where, List<Object> bindings, long pd, List<Relation> relations)
    {
        schema.inflateRelations(pd,
                                relations,
                                (spec, kind, value) -> {
                                    where.and(kind.getClause(spec));
                                    bindings.add(value);
                                });
    }

    private static void addOrderBy(SchemaSpec schema, Select.Where whereClause, boolean reverse)
    {
        if (reverse && schema.clusteringKeys.size() > 0)
        {
            Ordering[] ordering = new Ordering[schema.clusteringKeys.size()];
            for (int i = 0; i < schema.clusteringKeys.size(); i++)
            {
                ColumnSpec<?> c = schema.clusteringKeys.get(i);
                ordering[i] = c.isReversed() ? QueryBuilder.asc(c.name) : QueryBuilder.desc(c.name);
            }
            whereClause.orderBy(ordering);
        }
    }

    public static ResultSetRow resultSetToRow(SchemaSpec schema, OpSelectors.MonotonicClock clock, Object[] result)
    {
        Object[] partitionKey = new Object[schema.partitionKeys.size()];
        Object[] clusteringKey = new Object[schema.clusteringKeys.size()];
        Object[] regularColumns = new Object[schema.regularColumns.size()];

        System.arraycopy(result, 0, partitionKey, 0, partitionKey.length);
        System.arraycopy(result, partitionKey.length, clusteringKey, 0, clusteringKey.length);
        System.arraycopy(result, partitionKey.length + clusteringKey.length, regularColumns, 0, regularColumns.length);

        long[] lts = new long[schema.regularColumns.size()];
        for (int i = 0; i < lts.length; i++)
        {
            Object v = result[schema.allColumns.size() + i];
            lts[i] = v == null ? Model.NO_TIMESTAMP : clock.lts((long) v);
        }

        return new ResultSetRow(schema.deflatePartitionKey(partitionKey),
                                schema.deflateClusteringKey(clusteringKey),
                                schema.deflateRegularColumns(regularColumns),
                                lts);
    }

    public static List<ResultSetRow> execute(SystemUnderTest sut, OpSelectors.MonotonicClock clock, Query query)
    {
        CompiledStatement compiled = query.toSelectStatement();
        Object[][] objects = sut.execute(compiled.cql(), compiled.bindings());
        List<ResultSetRow> result = new ArrayList<>();
        for (Object[] obj : objects)
            result.add(resultSetToRow(query.schemaSpec, clock, obj));

        return result;
    }
}
