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
import harry.model.Model;
import harry.operations.Query;
import harry.operations.QueryGenerator;
import harry.visitors.PartitionVisitor;

public class SinglePartitionValidator implements PartitionVisitor
{
    protected final int iterations;
    protected final Model model;
    protected final QueryGenerator queryGenerator;
    protected final Run run;
    public SinglePartitionValidator(int iterations,
                                    Run run,
                                    Model.ModelFactory modelFactory)
    {
        this.iterations = iterations;
        this.model = modelFactory.make(run);
        this.queryGenerator = new QueryGenerator(run);
        this.run = run;
    }

    public void shutdown() throws InterruptedException
    {

    }

    public void visitPartition(long lts)
    {
        model.validate(queryGenerator.inflate(lts, 0, Query.QueryKind.SINGLE_PARTITION));

        for (boolean reverse : new boolean[]{ true, false })
        {
            model.validate(Query.selectPartition(run.schemaSpec, run.pdSelector.pd(lts, run.schemaSpec), reverse));
        }

        for (Query.QueryKind queryKind : new Query.QueryKind[]{ Query.QueryKind.CLUSTERING_RANGE, Query.QueryKind.CLUSTERING_SLICE, Query.QueryKind.SINGLE_CLUSTERING })
        {
            for (int i = 0; i < iterations; i++)
            {
                model.validate(queryGenerator.inflate(lts, i, queryKind));
            }
        }
    }
}
