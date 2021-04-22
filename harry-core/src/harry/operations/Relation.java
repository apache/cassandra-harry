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
import java.util.List;

import com.datastax.driver.core.querybuilder.Clause;
import harry.ddl.ColumnSpec;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

public class Relation
{
    public final RelationKind kind;
    public final ColumnSpec<?> columnSpec;
    // Theoretically, in model, we'll just be able to compare stuff according to relation, and pass it to DB
    public long descriptor;

    Relation(RelationKind kind,
             ColumnSpec<?> columnSpec,
             long descriptor)
    {
        this.kind = kind;
        this.columnSpec = columnSpec;
        this.descriptor = descriptor;
    }

    public boolean match(long l)
    {
        return kind.match(columnSpec.type.generator()::compare, l, descriptor);
    }

    public Object value()
    {
        return columnSpec.inflate(descriptor);
    }

    public String column()
    {
        return columnSpec.name;
    }

    public Clause toClause()
    {
        return kind.getClause(column(), bindMarker());
    }

    public String toString()
    {
        return "Relation{" +
               "kind=" + kind +
               ", columnSpec=" + columnSpec +
               ", descriptor=" + descriptor + " (" + Long.toHexString(descriptor) + ")" +
               '}';
    }

    public static Relation relation(RelationKind kind, ColumnSpec<?> columnSpec, long descriptor)
    {
        return new Relation(kind, columnSpec, descriptor);
    }

    public static Relation eqRelation(ColumnSpec<?> columnSpec, long descriptor)
    {
        return new Relation(RelationKind.EQ, columnSpec, descriptor);
    }

    public static List<Relation> eqRelations(long[] key, List<ColumnSpec<?>> columnSpecs)
    {
        List<Relation> relations = new ArrayList<>(key.length);
        addEqRelation(key, columnSpecs, relations);
        return relations;
    }

    public static void addEqRelation(long[] key, List<ColumnSpec<?>> columnSpecs, List<Relation> relations)
    {
        addRelation(key, columnSpecs, relations, RelationKind.EQ);
    }

    public static void addRelation(long[] key, List<ColumnSpec<?>> columnSpecs, List<Relation> relations, RelationKind kind)
    {
        assert key.length == columnSpecs.size() :
        String.format("Key size (%d) should equal to column spec size (%d)", key.length, columnSpecs.size());
        for (int i = 0; i < key.length; i++)
        {
            ColumnSpec<?> spec = columnSpecs.get(i);
            relations.add(relation(kind, spec, key[i]));
        }
    }

    public enum RelationKind
    {
        LT
        {
            @Override
            public Clause getClause(String name, Object obj)
            {
                return lt(name, obj);
            }

            public Clause getClause(List<String> name, List<Object> obj)
            {
                return lt(name, obj);
            }

            public boolean isNegatable()
            {
                return true;
            }

            public boolean isInclusive()
            {
                return false;
            }

            public RelationKind negate()
            {
                return GT;
            }

            public long nextMatch(long n)
            {
                return Math.subtractExact(n, 1);
            }

            public String toString()
            {
                return "<";
            }

            public boolean match(LongComparator comparator, long l, long r)
            {
                return comparator.compare(l, r) < 0;
            }
        },
        GT
        {
            @Override
            public Clause getClause(String name, Object obj)
            {
                return gt(name, obj);
            }

            public Clause getClause(List<String> name, List<Object> obj)
            {
                return gt(name, obj);
            }

            public boolean isNegatable()
            {
                return true;
            }

            public boolean isInclusive()
            {
                return false;
            }

            public RelationKind negate()
            {
                return LT;
            }

            public String toString()
            {
                return ">";
            }

            public boolean match(LongComparator comparator, long l, long r)
            {
                return comparator.compare(l, r) > 0;
            }

            public long nextMatch(long n)
            {
                return Math.addExact(n, 1);
            }
        },
        LTE
        {
            @Override
            public Clause getClause(String name, Object obj)
            {
                return lte(name, obj);
            }

            public Clause getClause(List<String> name, List<Object> obj)
            {
                return lt(name, obj);
            }

            public boolean isNegatable()
            {
                return true;
            }

            public boolean isInclusive()
            {
                return true;
            }

            public RelationKind negate()
            {
                return GTE;
            }

            public String toString()
            {
                return "<=";
            }

            public boolean match(LongComparator comparator, long l, long r)
            {
                return comparator.compare(l, r) <= 0;
            }

            public long nextMatch(long n)
            {
                return Math.subtractExact(n, 1);
            }
        },
        GTE
        {
            @Override
            public Clause getClause(String name, Object obj)
            {
                return gte(name, obj);
            }

            public Clause getClause(List<String> name, List<Object> obj)
            {
                return gte(name, obj);
            }

            public boolean isNegatable()
            {
                return true;
            }

            public boolean isInclusive()
            {
                return true;
            }

            public RelationKind negate()
            {
                return LTE;
            }

            public String toString()
            {
                return ">=";
            }

            public boolean match(LongComparator comparator, long l, long r)
            {
                return comparator.compare(l, r) >= 0;
            }

            public long nextMatch(long n)
            {
                return Math.addExact(n, 1);
            }
        },
        EQ
        {
            @Override
            public Clause getClause(String name, Object obj)
            {
                return eq(name, obj);
            }

            public Clause getClause(List<String> name, List<Object> obj)
            {
                return eq(name, obj);
            }

            public boolean isNegatable()
            {
                return false;
            }

            public boolean isInclusive()
            {
                return true;
            }

            public RelationKind negate()
            {
                throw new IllegalArgumentException("Cannot negate EQ");
            }

            public long nextMatch(long n)
            {
                return n;
            }

            public String toString()
            {
                return "=";
            }

            public boolean match(LongComparator comparator, long l, long r)
            {
                return comparator.compare(l, r) == 0;
            }
        };

        public abstract boolean match(LongComparator comparator, long l, long r);

        public abstract Clause getClause(String name, Object obj);

        public Clause getClause(ColumnSpec<?> spec)
        {
            return getClause(spec.name, bindMarker());
        }

        public abstract Clause getClause(List<String> name, List<Object> obj);

        public abstract boolean isNegatable();

        public abstract boolean isInclusive();

        public abstract RelationKind negate();

        public abstract long nextMatch(long n);
    }

    public static interface LongComparator
    {
        public int compare(long l, long r);
    }

    public static LongComparator FORWARD_COMPARATOR = Long::compare;
}
