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

package harry.visitors;

import harry.core.MetricReporter;
import harry.core.Run;
import harry.core.VisibleForTesting;
import harry.ddl.SchemaSpec;
import harry.model.OpSelectors;
import harry.operations.CompiledStatement;
import harry.operations.DeleteHelper;
import harry.operations.WriteHelper;
import harry.operations.Query;
import harry.operations.QueryGenerator;
import harry.util.BitSet;

public class MutatingRowVisitor implements OperationExecutor
{
    protected final SchemaSpec schema;
    protected final OpSelectors.MonotonicClock clock;
    protected final OpSelectors.DescriptorSelector descriptorSelector;
    protected final QueryGenerator rangeSelector;
    protected final MetricReporter metricReporter;

    public MutatingRowVisitor(Run run)
    {
        this(run.schemaSpec,
             run.clock,
             run.descriptorSelector,
             run.rangeSelector,
             run.metricReporter);
    }

    @VisibleForTesting
    public MutatingRowVisitor(SchemaSpec schema,
                              OpSelectors.MonotonicClock clock,
                              OpSelectors.DescriptorSelector descriptorSelector,
                              QueryGenerator rangeSelector,
                              MetricReporter metricReporter)
    {
        this.metricReporter = metricReporter;
        this.schema = schema;
        this.clock = clock;
        this.descriptorSelector = descriptorSelector;
        this.rangeSelector = rangeSelector;
    }

    public CompiledStatement insert(long lts, long pd, long cd, long opId)
    {
        metricReporter.insert();
        long[] vds = descriptorSelector.vds(pd, cd, lts, opId, OpSelectors.OperationKind.INSERT, schema);
        return WriteHelper.inflateInsert(schema, pd, cd, vds, null, clock.rts(lts));
    }

    public CompiledStatement insertWithStatics(long lts, long pd, long cd, long opId)
    {
        metricReporter.insert();
        long[] vds = descriptorSelector.vds(pd, cd, lts, opId, OpSelectors.OperationKind.INSERT_WITH_STATICS, schema);
        long[] sds = descriptorSelector.sds(pd, cd, lts, opId, OpSelectors.OperationKind.INSERT_WITH_STATICS, schema);
        return WriteHelper.inflateInsert(schema, pd, cd, vds, sds, clock.rts(lts));
    }

    public CompiledStatement update(long lts, long pd, long cd, long opId)
    {
        metricReporter.insert();
        long[] vds = descriptorSelector.vds(pd, cd, lts, opId, OpSelectors.OperationKind.UPDATE, schema);
        return WriteHelper.inflateUpdate(schema, pd, cd, vds, null, clock.rts(lts));
    }

    public CompiledStatement updateWithStatics(long lts, long pd, long cd, long opId)
    {
        metricReporter.insert();
        long[] vds = descriptorSelector.vds(pd, cd, lts, opId, OpSelectors.OperationKind.UPDATE_WITH_STATICS, schema);
        long[] sds = descriptorSelector.sds(pd, cd, lts, opId, OpSelectors.OperationKind.UPDATE_WITH_STATICS, schema);
        return WriteHelper.inflateUpdate(schema, pd, cd, vds, sds, clock.rts(lts));
    }

    public CompiledStatement deleteColumn(long lts, long pd, long cd, long opId)
    {
        metricReporter.columnDelete();
        BitSet columns = descriptorSelector.columnMask(pd, lts, opId, OpSelectors.OperationKind.DELETE_COLUMN);
        BitSet mask = schema.regularColumnsMask();
        return DeleteHelper.deleteColumn(schema, pd, cd, columns, mask, clock.rts(lts));
    }

    public CompiledStatement deleteColumnWithStatics(long lts, long pd, long cd, long opId)
    {
        metricReporter.columnDelete();
        BitSet columns = descriptorSelector.columnMask(pd, lts, opId, OpSelectors.OperationKind.DELETE_COLUMN_WITH_STATICS);
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