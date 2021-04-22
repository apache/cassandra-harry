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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.IntConsumer;

import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import harry.ddl.ColumnSpec;
import harry.ddl.SchemaSpec;
import harry.runner.LoggingPartitionVisitor;
import harry.util.BitSet;

public class DeleteHelper
{
    public static CompiledStatement deleteColumn(SchemaSpec schema,
                                                 long pd,
                                                 long cd,
                                                 BitSet columns,
                                                 BitSet mask,
                                                 long rts)
    {
        if (columns == null || columns.allUnset(mask))
            throw new IllegalArgumentException("Can't have a delete column query with no columns set. Column mask: " + columns);

        return delete(schema, pd, cd, columns, mask, rts);
    }

    public static CompiledStatement deleteColumn(SchemaSpec schema,
                                                 long pd,
                                                 BitSet columns,
                                                 BitSet mask,
                                                 long rts)
    {
        if (columns == null || columns.allUnset(mask))
            throw new IllegalArgumentException("Can't have a delete column query with no columns set. Column mask: " + columns);

        return delete(schema, pd, columns, mask, rts);
    }

    public static CompiledStatement deleteRow(SchemaSpec schema,
                                              long pd,
                                              long cd,
                                              long rts)
    {
        return delete(schema, pd, cd, null, null, rts);
    }

    public static CompiledStatement delete(SchemaSpec schema,
                                           long pd,
                                           List<Relation> relations,
                                           BitSet columnsToDelete,
                                           BitSet mask,
                                           long rts)
    {
        assert (columnsToDelete == null && mask == null) || (columnsToDelete != null && mask != null);
        return compile(schema,
                       pd,
                       relations,
                       columnsToDelete,
                       mask,
                       rts);
    }

    private static CompiledStatement delete(SchemaSpec schema,
                                            long pd,
                                            long cd,
                                            BitSet columnsToDelete,
                                            BitSet mask,
                                            long rts)
    {
        return compile(schema,
                       pd,
                       Relation.eqRelations(schema.ckGenerator.slice(cd),
                                            schema.clusteringKeys),
                       columnsToDelete,
                       mask,
                       rts);
    }

    private static CompiledStatement delete(SchemaSpec schema,
                                            long pd,
                                            BitSet columnsToDelete,
                                            BitSet mask,
                                            long rts)
    {
        return compile(schema,
                       pd,
                       new ArrayList<>(),
                       columnsToDelete,
                       mask,
                       rts);
    }

    public static CompiledStatement delete(SchemaSpec schema,
                                           long pd,
                                           long rts)
    {
        return compile(schema,
                       pd,
                       Collections.emptyList(),
                       null,
                       null,
                       rts);
    }

    private static CompiledStatement compile(SchemaSpec schema,
                                             long pd,
                                             List<Relation> relations,
                                             BitSet columnsToDelete,
                                             BitSet mask,
                                             long ts)
    {
        Delete delete;
        if (columnsToDelete == null)
            delete = QueryBuilder.delete().from(schema.keyspace, schema.table);
        else
        {
            assert mask != null;
            assert relations == null || relations.stream().allMatch((r) -> r.kind == Relation.RelationKind.EQ);
            delete = QueryBuilder.delete(columnNames(schema.allColumns, columnsToDelete, mask))
                                 .from(schema.keyspace, schema.table);
        }

        Delete.Where where = delete.where();
        List<Object> bindings = new ArrayList<>();

        addRelations(schema, where, bindings, pd, relations);
        delete.using(QueryBuilder.timestamp(ts));
        delete.setForceNoValues(true);
        Object[] bindingsArr = bindings.toArray(new Object[bindings.size()]);
        String compiled = delete.getQueryString();
        if (compiled.contains("built query (could not generate with default codec registry:"))
            throw new IllegalArgumentException(String.format("Could not generate the query: %s. Bindings: (%s)",
                                                             delete,
                                                             CompiledStatement.bindingsToString(bindingsArr)));
        return new CompiledStatement(compiled, bindingsArr);
    }

    private static void addRelations(SchemaSpec schema, Delete.Where where, List<Object> bindings, long pd, List<Relation> relations)
    {
        schema.inflateRelations(pd,
                                relations,
                                (spec, kind, value) -> {
                                    where.and(kind.getClause(spec));
                                    bindings.add(value);
                                });
    }

    private static String[] columnNames(List<ColumnSpec<?>> columns, BitSet selectedColumns, BitSet mask)
    {
        String[] columnNames = new String[selectedColumns.setCount(mask)];
        selectedColumns.eachSetBit(new IntConsumer()
        {
            int i = 0;

            public void accept(int idx)
            {
                columnNames[i++] = columns.get(idx).name;
            }
        }, mask);
        return columnNames;
    }
}
