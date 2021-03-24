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

package harry.ddl;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import harry.generators.DataGenerators;
import harry.operations.CompiledStatement;
import harry.operations.Relation;
import harry.util.BitSet;

// TODO: improve API of this class
// TODO: forbid schemas where pk and cks don't add up to 64 bits (for now)
public class SchemaSpec
{
    public interface SchemaSpecFactory
    {
        public SchemaSpec make(long seed);
    }

    public final DataGenerators.KeyGenerator pkGenerator;
    public final DataGenerators.KeyGenerator ckGenerator;

    private final boolean isCompactStorage;

    // These fields are immutable, and are safe as public
    public final String keyspace;
    public final String table;

    public final List<ColumnSpec<?>> partitionKeys;
    public final List<ColumnSpec<?>> clusteringKeys;
    public final List<ColumnSpec<?>> regularColumns;
    public final List<ColumnSpec<?>> allColumns;

    public final BitSet ALL_COLUMNS_BITSET;

    // TODO: forbid this constructor; add the one where column specs would be initialized through builder and have indexes
    public SchemaSpec(String keyspace,
                      String table,
                      List<ColumnSpec<?>> partitionKeys,
                      List<ColumnSpec<?>> clusteringKeys,
                      List<ColumnSpec<?>> regularColumns)
    {
        this(keyspace, table, partitionKeys, clusteringKeys, regularColumns, false);
    }

    public SchemaSpec(String keyspace,
                      String table,
                      List<ColumnSpec<?>> partitionKeys,
                      List<ColumnSpec<?>> clusteringKeys,
                      List<ColumnSpec<?>> regularColumns,
                      boolean isCompactStorage)
    {
        assert !isCompactStorage || clusteringKeys.size() == 0 || regularColumns.size() <= 1;

        this.keyspace = keyspace;
        this.table = table;
        this.isCompactStorage = isCompactStorage;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        for (int i = 0; i < partitionKeys.size(); i++)
            partitionKeys.get(i).setColumnIndex(i);
        this.clusteringKeys = ImmutableList.copyOf(clusteringKeys);
        for (int i = 0; i < clusteringKeys.size(); i++)
            clusteringKeys.get(i).setColumnIndex(i);
        this.regularColumns = ImmutableList.copyOf(regularColumns);
        for (int i = 0; i < regularColumns.size(); i++)
            regularColumns.get(i).setColumnIndex(i);
        this.allColumns = ImmutableList.copyOf(Iterables.concat(partitionKeys,
                                                                clusteringKeys,
                                                                regularColumns));
        this.pkGenerator = DataGenerators.createKeyGenerator(partitionKeys);
        this.ckGenerator = DataGenerators.createKeyGenerator(clusteringKeys);

        this.ALL_COLUMNS_BITSET = BitSet.allSet(regularColumns.size());
    }

    public void validate()
    {
        assert pkGenerator.byteSize() == Long.BYTES : partitionKeys.toString();
        assert ckGenerator.byteSize() == Long.BYTES : clusteringKeys.toString();
    }

    public static interface AddRelationCallback
    {
        public void accept(ColumnSpec spec, Relation.RelationKind kind, Object value);
    }

    public void inflateRelations(long pd,
                                 List<Relation> clusteringRelations,
                                 AddRelationCallback consumer)
    {
        Object[] pk = inflatePartitionKey(pd);
        for (int i = 0; i < pk.length; i++)
            consumer.accept(partitionKeys.get(i), Relation.RelationKind.EQ, pk[i]);

        for (Relation r : clusteringRelations)
            consumer.accept(r.columnSpec, r.kind, r.value());
    }

    public Object[] inflatePartitionKey(long pd)
    {
        return pkGenerator.inflate(pd);
    }

    public Object[] inflateClusteringKey(long cd)
    {
        return ckGenerator.inflate(cd);
    }

    public Object[] inflateRegularColumns(long[] vds)
    {
        return DataGenerators.inflateData(regularColumns, vds);
    }

    // TODO: remove indirection; call directly
    public long adjustPdEntropy(long descriptor)
    {
        return pkGenerator.adjustEntropyDomain(descriptor);
    }

    public long adjustCdEntropy(long descriptor)
    {
        return ckGenerator.adjustEntropyDomain(descriptor);
    }

    public long deflatePartitionKey(Object[] pk)
    {
        return pkGenerator.deflate(pk);
    }

    public long deflateClusteringKey(Object[] ck)
    {
        return ckGenerator.deflate(ck);
    }

    public long[] deflateRegularColumns(Object[] regulars)
    {
        return DataGenerators.deflateData(regularColumns, regulars);
    }

    public CompiledStatement compile()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ");
        sb.append(keyspace)
          .append(".")
          .append(table)
          .append(" (");

        SeparatorAppender commaAppender = new SeparatorAppender();
        for (ColumnSpec<?> cd : partitionKeys)
        {
            commaAppender.accept(sb);
            sb.append(cd.toCQL());
            if (partitionKeys.size() == 1 && clusteringKeys.size() == 0)
                sb.append(" PRIMARY KEY");
        }

        Stream.concat(clusteringKeys.stream(),
                      regularColumns.stream())
              .forEach((cd) -> {
                  commaAppender.accept(sb);
                  sb.append(cd.toCQL());
              });

        if (clusteringKeys.size() > 0 || partitionKeys.size() > 1)
        {
            sb.append(", ").append(getPrimaryKeyCql());
        }

        sb.append(')');

        Runnable appendWith = doOnce(() -> sb.append(" WITH "));

        if (isCompactStorage)
        {
            appendWith.run();
            sb.append("COMPACT STORAGE AND");
        }

        if (clusteringKeys.size() > 0)
        {
            appendWith.run();
            sb.append(getClusteringOrderCql())
              .append(';');
        }

        return new CompiledStatement(sb.toString());
    }

    private String getClusteringOrderCql()
    {
        StringBuilder sb = new StringBuilder();
        if (clusteringKeys.size() > 0)
        {
            sb.append(" CLUSTERING ORDER BY (");

            SeparatorAppender commaAppender = new SeparatorAppender();
            for (ColumnSpec<?> column : clusteringKeys)
            {
                commaAppender.accept(sb);
                sb.append(column.name).append(' ').append(column.isReversed() ? "DESC" : "ASC");
            }

            // TODO: test for this
//            sb.append(") AND read_repair='none'");
            sb.append(")");
        }

        return sb.toString();
    }

    private String getPrimaryKeyCql()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("PRIMARY KEY (");
        if (partitionKeys.size() > 1)
        {
            sb.append('(');
            SeparatorAppender commaAppender = new SeparatorAppender();
            for (ColumnSpec<?> cd : partitionKeys)
            {
                commaAppender.accept(sb);
                sb.append(cd.name);
            }
            sb.append(')');
        }
        else
        {
            sb.append(partitionKeys.get(0).name);
        }

        for (ColumnSpec<?> cd : clusteringKeys)
            sb.append(", ").append(cd.name);

        return sb.append(')').toString();
    }

    public String toString()
    {
        return String.format("schema {cql=%s, columns=%s}", compile().toString(), allColumns);
    }

    private static Runnable doOnce(Runnable r)
    {
        return new Runnable()
        {
            boolean executed = false;

            public void run()
            {
                if (executed)
                    return;

                executed = true;
                r.run();
            }
        };
    }

    public static class SeparatorAppender implements Consumer<StringBuilder>
    {
        boolean isFirst = true;
        private final String separator;

        public SeparatorAppender()
        {
            this(",");
        }

        public SeparatorAppender(String separator)
        {
            this.separator = separator;
        }

        public void accept(StringBuilder stringBuilder)
        {
            if (isFirst)
                isFirst = false;
            else
                stringBuilder.append(separator);
        }

        public void accept(StringBuilder stringBuilder, String s)
        {
            accept(stringBuilder);
            stringBuilder.append(s);
        }


        public void reset()
        {
            isFirst = true;
        }
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaSpec that = (SchemaSpec) o;
        return Objects.equals(keyspace, that.keyspace) &&
               Objects.equals(table, that.table) &&
               Objects.equals(partitionKeys, that.partitionKeys) &&
               Objects.equals(clusteringKeys, that.clusteringKeys) &&
               Objects.equals(regularColumns, that.regularColumns);
    }

    public int hashCode()
    {
        return Objects.hash(keyspace, table, partitionKeys, clusteringKeys, regularColumns);
    }
}
