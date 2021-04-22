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

import harry.data.ResultSetRow;
import harry.ddl.SchemaSpec;
import harry.generators.PcgRSUFast;
import harry.generators.RandomGenerator;
import harry.model.Model;
import harry.model.OpSelectors;
import harry.operations.CompiledStatement;
import harry.operations.DeleteHelper;
import harry.util.BitSet;

// removes/hides the value of one of the columns that was previously set
public class HideValueCorruptor implements RowCorruptor
{
    private final SchemaSpec schema;
    private final OpSelectors.MonotonicClock clock;
    private final RandomGenerator rng;

    public HideValueCorruptor(SchemaSpec schemaSpec,
                              OpSelectors.MonotonicClock clock)
    {
        this.schema = schemaSpec;
        this.clock = clock;
        this.rng = new PcgRSUFast(1, 1);
    }

    // Can corrupt any row that has at least one written non-null value
    public boolean canCorrupt(ResultSetRow row)
    {
        for (int idx = 0; idx < row.lts.length; idx++)
        {
            if (row.lts[idx] != Model.NO_TIMESTAMP)
                return true;
        }
        return false;
    }

    public CompiledStatement corrupt(ResultSetRow row)
    {
        BitSet mask;
        if (row.slts != null && rng.nextBoolean())
        {
            int cnt = 0;
            int idx;
            do
            {
                idx = rng.nextInt(row.slts.length - 1);
                cnt++;
            }
            while (row.slts[idx] == Model.NO_TIMESTAMP && cnt < 10);

            if (row.slts[idx] != Model.NO_TIMESTAMP)
            {
                mask = BitSet.allUnset(schema.allColumns.size());
                mask.set(schema.staticColumnsOffset + idx);

                return DeleteHelper.deleteColumn(schema,
                                                 row.pd,
                                                 mask,
                                                 schema.regularAndStaticColumnsMask(),
                                                 clock.rts(clock.maxLts()) + 1);
            }
        }

        int idx;
        do
        {
            idx = rng.nextInt(row.lts.length - 1);
        }
        while (row.lts[idx] == Model.NO_TIMESTAMP);

        mask = BitSet.allUnset(schema.allColumns.size());
        mask.set(schema.regularColumnsOffset + idx);

        return DeleteHelper.deleteColumn(schema,
                                         row.pd,
                                         row.cd,
                                         mask,
                                         schema.regularAndStaticColumnsMask(),
                                         clock.rts(clock.maxLts()) + 1);
    }
}
