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

package harry.generators;

import java.util.Random;

import org.junit.Test;

import harry.ddl.ColumnSpec;
import org.apache.cassandra.cql3.CQLTester;

public class DataGeneratorsIntegrationTest extends CQLTester
{
    @Test
    public void testTimestampTieResolution() throws Throwable
    {
        Random rng = new Random(1);
        for (ColumnSpec.DataType<?> dataType : new ColumnSpec.DataType[]{ ColumnSpec.int8Type,
                                                                       ColumnSpec.int16Type,
                                                                       ColumnSpec.int32Type,
                                                                       ColumnSpec.int64Type,
                                                                       ColumnSpec.asciiType,
                                                                       ColumnSpec.floatType,
                                                                       ColumnSpec.doubleType })
        {
            createTable(String.format("CREATE TABLE %%s (pk int PRIMARY KEY, v %s)",
                                      dataType.toString()));
            for (int i = 0; i < 10_000; i++)
            {
                long d1 = dataType.generator().adjustEntropyDomain(rng.nextLong());
                long d2 = dataType.generator().adjustEntropyDomain(rng.nextLong());
                for (long d : new long[]{ d1, d2 })
                {
                    execute("INSERT INTO %s (pk, v) VALUES (?,?) USING TIMESTAMP 1",
                            i, dataType.generator().inflate(d));
                }

                if (dataType.compareLexicographically(d1, d2) > 0)
                    assertRows(execute("SELECT v FROM %s WHERE pk=?", i),
                               row(dataType.generator().inflate(d1)));
                else
                    assertRows(execute("SELECT v FROM %s WHERE pk=?", i),
                               row(dataType.generator().inflate(d2)));
            }
        }
    }
}
