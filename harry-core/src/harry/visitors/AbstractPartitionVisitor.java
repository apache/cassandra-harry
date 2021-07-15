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

import harry.ddl.SchemaSpec;
import harry.model.OpSelectors;

public abstract class AbstractPartitionVisitor implements PartitionVisitor
{
    protected final OpSelectors.PdSelector pdSelector;
    protected final OpSelectors.DescriptorSelector descriptorSelector;
    protected final SchemaSpec schema;

    public AbstractPartitionVisitor(AbstractPartitionVisitor visitor)
    {
        this(visitor.pdSelector, visitor.descriptorSelector, visitor.schema);
    }

    public AbstractPartitionVisitor(OpSelectors.PdSelector pdSelector,
                                    OpSelectors.DescriptorSelector descriptorSelector,
                                    SchemaSpec schema)
    {
        this.pdSelector = pdSelector;
        this.descriptorSelector = descriptorSelector;
        this.schema = schema;
    }

    public void visitPartition(long lts)
    {
        visitPartition(lts, pdSelector.pd(lts, schema));
    }

    private void visitPartition(long lts, long pd)
    {
        beforeLts(lts, pd);

        int modificationsCount = descriptorSelector.numberOfModifications(lts);
        int opsPerModification = descriptorSelector.opsPerModification(lts);

        for (int m = 0; m < modificationsCount; m++)
        {
            beforeBatch(lts, pd, m);
            for (int i = 0; i < opsPerModification; i++)
            {
                long opId = m * opsPerModification + i;
                long cd = descriptorSelector.cd(pd, lts, opId, schema);
                operation(lts, pd, cd, m, opId);
            }
            afterBatch(lts, pd, m);
        }

        afterLts(lts, pd);
    }

    protected void beforeLts(long lts, long pd)
    {
    }

    protected void afterLts(long lts, long pd)
    {
    }

    protected void beforeBatch(long lts, long pd, long m)
    {
    }

    protected void operation(long lts, long pd, long cd, long m, long opId)
    {

    }

    protected void afterBatch(long lts, long pd, long m)
    {
    }

    public void shutdown() throws InterruptedException
    {
    }
}