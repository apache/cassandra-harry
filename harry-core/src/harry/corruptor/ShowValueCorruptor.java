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

import java.util.Arrays;

import harry.data.ResultSetRow;
import harry.ddl.SchemaSpec;
import harry.generators.DataGenerators;
import harry.generators.PcgRSUFast;
import harry.generators.RandomGenerator;
import harry.model.Model;
import harry.model.OpSelectors;
import harry.operations.CompiledStatement;
import harry.operations.WriteHelper;

public class ShowValueCorruptor implements RowCorruptor
{
    private final SchemaSpec schema;
    private final OpSelectors.MonotonicClock clock;
    private final RandomGenerator rng;

    public ShowValueCorruptor(SchemaSpec schemaSpec,
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
            if (row.lts[idx] == Model.NO_TIMESTAMP)
                return true;
        }
        return false;
    }

    public CompiledStatement corrupt(ResultSetRow row)
    {
        long[] corruptedVds = new long[row.lts.length];
        Arrays.fill(corruptedVds, DataGenerators.UNSET_DESCR);

        int idx;
        do
        {
            idx = rng.nextInt(corruptedVds.length - 1);
        }
        while (row.lts[idx] != Model.NO_TIMESTAMP);

        corruptedVds[idx] = rng.next();

        // We do not know LTS of the deleted row. We could try inferring it, but that
        // still won't help since we can't use it anyways, since collisions between a
        // written value and tombstone are resolved in favour of tombstone.
        return WriteHelper.inflateInsert(schema, row.pd, row.cd, corruptedVds, null, clock.rts(clock.maxLts()) + 1);
    }
}
