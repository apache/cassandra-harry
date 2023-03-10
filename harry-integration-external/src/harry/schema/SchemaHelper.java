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

package harry.schema;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import harry.ddl.ColumnSpec;
import harry.ddl.SchemaSpec;

public final class SchemaHelper {
    private static final Map<String, ColumnSpec.DataType<?>> DATA_TYPES = ColumnSpec.DATA_TYPES.stream()
                                                                                               .collect(Collectors.toMap(ColumnSpec.DataType::toString, a -> a));

    private SchemaHelper()
    {
        throw new UnsupportedOperationException("This class shall not be instantiated");
    }

    private static List<ColumnSpec<?>> createColumnSpecs(List<ColumnMetadata> columns, ColumnSpec.Kind kind, boolean ignoreUnknownTypes)
    {
        return columns.stream()
                      .sequential()
                      .filter(column -> {
                          if (kind == ColumnSpec.Kind.STATIC)
                              return column.isStatic();

                          if (kind == ColumnSpec.Kind.REGULAR)
                              return !column.isStatic();

                          assert !column.isStatic();
                          return true;
                      })
                      .map(column -> {
                          String typeName = column.getType().getName().toString();
                          ColumnSpec.DataType<?> type = DATA_TYPES.get(typeName);
                          if (type == null)
                          {
                              if (ignoreUnknownTypes)
                                  return null;

                              throw new AssertionError(String.format("Data type with name '%s' is not currently supported", typeName));
                          }

                          return new ColumnSpec<>(column.getName(), type, kind);
                      })
                      .filter(Objects::nonNull)
                      .collect(Collectors.toList());
    }

    public static SchemaSpec createSchemaSpec(Metadata metadata, String keyspace, String table, boolean ignoreUnkownTypes)
    {
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspace);
        TableMetadata tableMetadata = keyspaceMetadata.getTable(table);

        List<ColumnMetadata> partitionMetadata = tableMetadata.getPartitionKey();
        List<ColumnMetadata> clusteringMetadata = tableMetadata.getClusteringColumns();
        List<ColumnMetadata> columnsMetadata = tableMetadata.getColumns();
        columnsMetadata.removeAll(partitionMetadata);
        columnsMetadata.removeAll(clusteringMetadata);

        List<ColumnSpec<?>> partitionKey = createColumnSpecs(partitionMetadata, ColumnSpec.Kind.PARTITION_KEY, false);
        List<ColumnSpec<?>> clusteringKey = createColumnSpecs(clusteringMetadata, ColumnSpec.Kind.CLUSTERING, ignoreUnkownTypes);
        List<ColumnSpec<?>> regularColumns = createColumnSpecs(columnsMetadata, ColumnSpec.Kind.REGULAR, ignoreUnkownTypes);
        List<ColumnSpec<?>> staticColumns = createColumnSpecs(columnsMetadata, ColumnSpec.Kind.STATIC, ignoreUnkownTypes);

        return new SchemaSpec(keyspace, table, partitionKey, clusteringKey, regularColumns, staticColumns);
    }
}
