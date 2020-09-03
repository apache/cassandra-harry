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

package harry.runner;

import harry.ddl.SchemaSpec;
import harry.model.OpSelectors;
import harry.operations.CompiledStatement;
import harry.operations.DeleteHelper;
import harry.operations.WriteHelper;
import harry.util.BitSet;

public class DefaultRowVisitor implements RowVisitor
{
    private final SchemaSpec schema;
    private final OpSelectors.MonotonicClock clock;
    private final OpSelectors.DescriptorSelector descriptorSelector;
    private final QuerySelector querySelector;

    public DefaultRowVisitor(SchemaSpec schema,
                             OpSelectors.MonotonicClock clock,
                             OpSelectors.DescriptorSelector descriptorSelector,
                             QuerySelector querySelector)
    {
        this.schema = schema;
        this.clock = clock;
        this.descriptorSelector = descriptorSelector;
        this.querySelector = querySelector;
    }

    public CompiledStatement write(long lts, long pd, long cd, long opId)
    {
        long[] vds = descriptorSelector.vds(pd, cd, lts, opId, schema);
        return WriteHelper.inflateInsert(schema, pd, cd, vds, clock.rts(lts));
    }

    public CompiledStatement deleteColumn(long lts, long pd, long cd, long opId)
    {
        BitSet mask = descriptorSelector.columnMask(pd, lts, opId);
        return DeleteHelper.deleteColumn(schema, pd, cd, mask, clock.rts(lts));
    }


    public CompiledStatement deleteRow(long lts, long pd, long cd, long opId)
    {
        return DeleteHelper.deleteRow(schema, pd, cd, clock.rts(lts));
    }

    public CompiledStatement deleteRange(long lts, long pd, long opId)
    {
        return querySelector.inflate(lts, opId).toDeleteStatement(clock.rts(lts));
    }
}