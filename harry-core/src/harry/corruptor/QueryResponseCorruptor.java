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

package harry.corruptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.data.ResultSetRow;
import harry.ddl.SchemaSpec;
import harry.model.OpSelectors;
import harry.model.SelectHelper;
import harry.model.sut.SystemUnderTest;
import harry.operations.CompiledStatement;
import harry.runner.HarryRunner;
import harry.runner.Query;

public interface QueryResponseCorruptor
{
    Logger logger = LoggerFactory.getLogger(QueryResponseCorruptor.class);

    boolean maybeCorrupt(Query query, SystemUnderTest sut);

    class SimpleQueryResponseCorruptor implements QueryResponseCorruptor
    {
        private final RowCorruptor rowCorruptor;
        private final SchemaSpec schema;
        private final OpSelectors.MonotonicClock clock;

        public SimpleQueryResponseCorruptor(SchemaSpec schema,
                                            OpSelectors.MonotonicClock clock,
                                            RowCorruptor.RowCorruptorFactory factory)
        {
            this.rowCorruptor = factory.create(schema, clock);
            this.schema = schema;
            this.clock = clock;
        }

        public boolean maybeCorrupt(Query query, SystemUnderTest sut)
        {
            List<ResultSetRow> result = new ArrayList<>();
            CompiledStatement statement = query.toSelectStatement();
            Object[][] before = sut.execute(statement.cql(), SystemUnderTest.ConsistencyLevel.ALL, statement.bindings());
            for (Object[] obj : before)
                result.add(SelectHelper.resultSetToRow(schema, clock, obj));

            // TODO: technically, we can do this just depends on corruption strategy
            // we just need to corrupt results of the current query.
            if (result.isEmpty())
                return false;

            for (ResultSetRow row : result)
            {
                if (rowCorruptor.maybeCorrupt(row, sut))
                {
                    Object[][] after = sut.execute(statement.cql(), SystemUnderTest.ConsistencyLevel.ALL, statement.bindings());
                    boolean mismatch = false;
                    for (int i = 0; i < before.length && i < after.length; i++)
                    {
                        if (!Arrays.equals(before[i], after[i]))
                        {
                            logger.info("Corrupted: \nBefore: {}\n" +
                                        "After:  {}\n",
                                        Arrays.toString(before[i]),
                                        Arrays.toString(after[i]));
                            mismatch = true;
                        }
                    }
                    assert mismatch || before.length != after.length;
                    return true;
                }
            }
            return false;
        }
    }
}
