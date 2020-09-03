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

import java.util.HashSet;
import java.util.Set;

import harry.data.ResultSetRow;
import harry.ddl.SchemaSpec;
import harry.model.OpSelectors;
import harry.model.SelectHelper;
import harry.model.sut.SystemUnderTest;
import harry.operations.WriteHelper;
import harry.runner.Query;

public class AddExtraRowCorruptor implements QueryResponseCorruptor
{
    private final SchemaSpec schema;
    private final OpSelectors.MonotonicClock clock;
    private final OpSelectors.DescriptorSelector descriptorSelector;

    public AddExtraRowCorruptor(SchemaSpec schema,
                                OpSelectors.MonotonicClock clock,
                                OpSelectors.DescriptorSelector descriptorSelector)
    {
        this.schema = schema;
        this.clock = clock;
        this.descriptorSelector = descriptorSelector;
    }

    public boolean maybeCorrupt(Query query, SystemUnderTest sut)
    {
        Set<Long> cds = new HashSet<>();
        long maxLts = 0;
        for (Object[] obj : sut.execute(query.toSelectStatement()))
        {
            ResultSetRow row = SelectHelper.resultSetToRow(schema, clock, obj);
            // TODO: extract CD cheaper
            cds.add(row.cd);
            for (int i = 0; i < row.lts.length; i++)
                maxLts = Math.max(maxLts, row.lts[i]);
        }

        if (cds.size() >= descriptorSelector.maxPartitionSize())
            return false;

        long cd;
        long attempt = 0;
        do
        {
            cd = descriptorSelector.randomCd(query.pd, attempt, schema);
            if (attempt++ == 1000)
                return false;
        }
        while (!query.match(cd) || cds.contains(cd));

        long[] vds = descriptorSelector.vds(query.pd, cd, maxLts, 0, schema);

        // We do not know if the row was deleted. We could try inferring it, but that
        // still won't help since we can't use it anyways, since collisions between a
        // written value and tombstone are resolved in favour of tombstone, so we're
        // just going to take the next lts.
        sut.execute(WriteHelper.inflateInsert(schema, query.pd, cd, vds, clock.rts(maxLts) + 1));
        return true;
    }
}