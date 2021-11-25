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

import harry.core.Run;
import harry.ddl.SchemaSpec;
import harry.model.OpSelectors;

public class GeneratingVisitor extends LtsVisitor
{
    private final OpSelectors.PdSelector pdSelector;
    private final OpSelectors.DescriptorSelector descriptorSelector;
    private final SchemaSpec schema;

    public GeneratingVisitor(Run run,
                             VisitExecutor delegate)
    {
        super(delegate, run.clock::nextLts);

        this.pdSelector = run.pdSelector;
        this.descriptorSelector = run.descriptorSelector;
        this.schema = run.schemaSpec;
    }

    @Override
    public void visit(long lts)
    {
        generate(lts, pdSelector.pd(lts, schema));
    }

    private void generate(long lts, long pd)
    {
        beforeLts(lts, pd);

        int modificationsCount = descriptorSelector.numberOfModifications(lts);
        int opsPerModification = descriptorSelector.opsPerModification(lts);

        for (long m = 0; m < modificationsCount; m++)
        {
            beforeBatch(lts, pd, m);
            for (long i = 0; i < opsPerModification; i++)
            {
                long opId = m * opsPerModification + i;
                long cd = descriptorSelector.cd(pd, lts, opId, schema);
                OpSelectors.OperationKind opType = descriptorSelector.operationType(pd, lts, opId);
                operation(lts, pd, cd, m, opId, opType);
            }
            afterBatch(lts, pd, m);
        }

        afterLts(lts, pd);
    }
}
