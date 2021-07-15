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

import harry.data.ResultSetRow;
import harry.ddl.ColumnSpec;
import harry.ddl.SchemaSpec;
import harry.model.sut.SystemUnderTest;
import harry.operations.CompiledStatement;
import harry.operations.Relation;
import harry.operations.Query;

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
        StringBuilder b = new StringBuilder();
        b.append("SELECT ");

        for (int i = 0; i < schema.allColumns.size(); i++)
        {
            ColumnSpec<?> spec = schema.allColumns.get(i);
            if (i > 0)
                b.append(", ");
            b.append(spec.name);
        }

        if (includeWriteTime)
        {
            for (ColumnSpec<?> column : schema.staticColumns)
                b.append(", ")
                 .append("writetime(")
                 .append(column.name)
                 .append(")");

            for (ColumnSpec<?> column : schema.regularColumns)
                b.append(", ")
                 .append("writetime(")
                 .append(column.name)
                 .append(")");
        }

        b.append(" FROM ")
         .append(schema.keyspace)
         .append(".")
         .append(schema.table)
         .append(" WHERE ");

        List<Object> bindings = new ArrayList<>();

        schema.inflateRelations(pd,
                                relations,
                                new SchemaSpec.AddRelationCallback()
                                {
                                    boolean isFirst = true;
                                    public void accept(ColumnSpec<?> spec, Relation.RelationKind kind, Object value)
                                    {
                                        if (isFirst)
                                            isFirst = false;
                                        else
                                            b.append(" AND ");
                                        b.append(kind.getClause(spec));
                                        bindings.add(value);
                                    }
                                });
        addOrderBy(schema, b, reverse);
        b.append(";");
        Object[] bindingsArr = bindings.toArray(new Object[bindings.size()]);
        return new CompiledStatement(b.toString(), bindingsArr);
    }

    public static CompiledStatement count(SchemaSpec schema, long pd)
    {
        StringBuilder b = new StringBuilder();
        b.append("SELECT count(*) ");

        b.append(" FROM ")
         .append(schema.keyspace)
         .append(".")
         .append(schema.table)
         .append(" WHERE ");

        List<Object> bindings = new ArrayList<>(schema.partitionKeys.size());

        schema.inflateRelations(pd,
                                Collections.emptyList(),
                                new SchemaSpec.AddRelationCallback()
                                {
                                    boolean isFirst = true;
                                    public void accept(ColumnSpec<?> spec, Relation.RelationKind kind, Object value)
                                    {
                                        if (isFirst)
                                            isFirst = false;
                                        else
                                            b.append(" AND ");
                                        b.append(kind.getClause(spec));
                                        bindings.add(value);
                                    }
                                });

        Object[] bindingsArr = bindings.toArray(new Object[bindings.size()]);
        return new CompiledStatement(b.toString(), bindingsArr);
    }

    private static void addOrderBy(SchemaSpec schema, StringBuilder b, boolean reverse)
    {
        if (reverse && schema.clusteringKeys.size() > 0)
        {
            b.append(" ORDER BY ");
            for (int i = 0; i < schema.clusteringKeys.size(); i++)
            {
                ColumnSpec<?> c = schema.clusteringKeys.get(i);
                if (i > 0)
                    b.append(", ");
                b.append(c.isReversed() ? asc(c.name) : desc(c.name));
            }
        }
    }

    public static String asc(String name)
    {
        return name + " ASC";
    }

    public static String desc(String name)
    {
        return name + " DESC";
    }

    public static ResultSetRow resultSetToRow(SchemaSpec schema, OpSelectors.MonotonicClock clock, Object[] result)
    {
        Object[] partitionKey = new Object[schema.partitionKeys.size()];
        Object[] clusteringKey = new Object[schema.clusteringKeys.size()];
        Object[] staticColumns = new Object[schema.staticColumns.size()];
        Object[] regularColumns = new Object[schema.regularColumns.size()];

        System.arraycopy(result, 0, partitionKey, 0, partitionKey.length);
        System.arraycopy(result, partitionKey.length, clusteringKey, 0, clusteringKey.length);
        System.arraycopy(result, partitionKey.length + clusteringKey.length, staticColumns, 0, staticColumns.length);
        System.arraycopy(result, partitionKey.length + clusteringKey.length + staticColumns.length, regularColumns, 0, regularColumns.length);

        long[] slts = new long[schema.staticColumns.size()];
        for (int i = 0; i < slts.length; i++)
        {
            Object v = result[schema.allColumns.size() + i];
            slts[i] = v == null ? Model.NO_TIMESTAMP : clock.lts((long) v);
        }

        long[] lts = new long[schema.regularColumns.size()];
        for (int i = 0; i < lts.length; i++)
        {
            Object v = result[schema.allColumns.size() + slts.length + i];
            lts[i] = v == null ? Model.NO_TIMESTAMP : clock.lts((long) v);
        }

        return new ResultSetRow(schema.deflatePartitionKey(partitionKey),
                                schema.deflateClusteringKey(clusteringKey),
                                schema.staticColumns.isEmpty() ? null : schema.deflateStaticColumns(staticColumns),
                                schema.staticColumns.isEmpty() ? null : slts,
                                schema.deflateRegularColumns(regularColumns),
                                lts);
    }

    public static List<ResultSetRow> execute(SystemUnderTest sut, OpSelectors.MonotonicClock clock, Query query)
    {
        CompiledStatement compiled = query.toSelectStatement();
        Object[][] objects = sut.execute(compiled.cql(), SystemUnderTest.ConsistencyLevel.QUORUM, compiled.bindings());
        List<ResultSetRow> result = new ArrayList<>();
        for (Object[] obj : objects)
            result.add(resultSetToRow(query.schemaSpec, clock, obj));

        return result;
    }
}
