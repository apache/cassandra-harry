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

import harry.core.MetricReporter;
import harry.core.Run;
import harry.ddl.SchemaSpec;
import harry.model.OpSelectors;
import harry.operations.CompiledStatement;
import harry.operations.DeleteHelper;
import harry.operations.WriteHelper;
import harry.util.BitSet;

public class MutatingRowVisitor implements Operation
{
    protected final SchemaSpec schema;
    protected final OpSelectors.MonotonicClock clock;
    protected final OpSelectors.DescriptorSelector descriptorSelector;
    protected final QueryGenerator rangeSelector;
    protected final MetricReporter metricReporter;

    public MutatingRowVisitor(Run run)
    {
        this.metricReporter = run.metricReporter;
        this.schema = run.schemaSpec;
        this.clock = run.clock;
        this.descriptorSelector = run.descriptorSelector;
        this.rangeSelector = run.rangeSelector;
    }

    public CompiledStatement write(long lts, long pd, long cd, long opId)
    {
        metricReporter.insert();
        long[] vds = descriptorSelector.vds(pd, cd, lts, opId, schema);
        return WriteHelper.inflateInsert(schema, pd, cd, vds, null, clock.rts(lts));
    }

    public CompiledStatement writeWithStatics(long lts, long pd, long cd, long opId)
    {
        metricReporter.insert();
        long[] vds = descriptorSelector.vds(pd, cd, lts, opId, schema);
        long[] sds = descriptorSelector.sds(pd, cd, lts, opId, schema);
        return WriteHelper.inflateInsert(schema, pd, cd, vds, sds, clock.rts(lts));
    }

    public CompiledStatement deleteColumn(long lts, long pd, long cd, long opId)
    {
        metricReporter.columnDelete();
        BitSet columns = descriptorSelector.columnMask(pd, lts, opId);
        BitSet mask = schema.regularColumnsMask();
        return DeleteHelper.deleteColumn(schema, pd, cd, columns, mask, clock.rts(lts));
    }

    public CompiledStatement deleteColumnWithStatics(long lts, long pd, long cd, long opId)
    {
        metricReporter.columnDelete();
        BitSet columns = descriptorSelector.columnMask(pd, lts, opId);
        BitSet mask = schema.regularAndStaticColumnsMask();
        return DeleteHelper.deleteColumn(schema, pd, cd, columns, mask, clock.rts(lts));
    }

    public CompiledStatement deleteRow(long lts, long pd, long cd, long opId)
    {
        metricReporter.rowDelete();
        return DeleteHelper.deleteRow(schema, pd, cd, clock.rts(lts));
    }

    public CompiledStatement deletePartition(long lts, long pd, long opId)
    {
        metricReporter.partitionDelete();
        return DeleteHelper.delete(schema, pd, clock.rts(lts));
    }

    public CompiledStatement deleteRange(long lts, long pd, long opId)
    {
        metricReporter.rangeDelete();
        return rangeSelector.inflate(lts, opId, Query.QueryKind.CLUSTERING_RANGE).toDeleteStatement(clock.rts(lts));
    }

    public CompiledStatement deleteSlice(long lts, long pd, long opId)
    {
        metricReporter.rangeDelete();
        return rangeSelector.inflate(lts, opId, Query.QueryKind.CLUSTERING_SLICE).toDeleteStatement(clock.rts(lts));
    }
}