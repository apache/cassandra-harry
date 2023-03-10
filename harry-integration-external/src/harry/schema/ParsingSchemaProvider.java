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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Configuration;
import harry.ddl.SchemaSpec;
import harry.model.sut.SystemUnderTest;
import harry.model.sut.external.ExternalClusterSut;

@JsonTypeName("parsing")
public class ParsingSchemaProvider implements Configuration.SchemaProviderConfiguration
{
    private static final Logger logger = LoggerFactory.getLogger(ParsingSchemaProvider.class);

    public static void init()
    {
        Configuration.registerSubtypes(ParsingSchemaProvider.class);
    }

    public final String keyspace;
    public final String table;
    public final String ddl;
    public final boolean ignore_unknown_types;

    @JsonCreator
    public ParsingSchemaProvider(@JsonProperty("keyspace") String keyspace,
                                 @JsonProperty("table") String table,
                                 @JsonProperty("ddl") String ddl,
                                 @JsonProperty("ignore_unknown_types") Boolean ignoreUnknown)
    {
        this.keyspace = keyspace;
        this.table = table;
        this.ddl = ddl;
        this.ignore_unknown_types = ignoreUnknown != null && ignoreUnknown;
    }
    public SchemaSpec make(long seed, SystemUnderTest sut)
    {
        assert sut instanceof ExternalClusterSut;
        if (ddl != null)
            sut.schemaChange(ddl);

        ExternalClusterSut externalSut = (ExternalClusterSut) sut;
        SchemaSpec schemaSpec = SchemaHelper.createSchemaSpec(externalSut.metadata(), keyspace, table, ignore_unknown_types);
        logger.info("Using schema: " + schemaSpec.compile().cql());
        return schemaSpec;
    }
}
