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

import java.util.Arrays;

import harry.core.Run;
import harry.model.OpSelectors;

public abstract class ReplayingVisitor extends DelegatingVisitor
{
    public ReplayingVisitor(VisitExecutor delegate)
    {
        super(delegate);
    }

    public void visit(long lts)
    {
        replay(getVisit(lts));
    }

    public abstract Visit getVisit(long lts);

    public abstract void replayAll(Run run);
    private void replay(Visit visit)
    {
        beforeLts(visit.lts, visit.pd);

        for (Batch batch : visit.operations)
        {
            beforeBatch(visit.lts, visit.pd, batch.m);
            for (Operation operation : batch.operations)
                operation(visit.lts, visit.pd, operation.cd, batch.m, operation.opId, operation.opType);
            afterBatch(visit.lts, visit.pd, batch.m);
        }

        afterLts(visit.lts, visit.pd);
    }

    public static class Visit
    {
        public final long lts;
        public final long pd;
        public final Batch[] operations;

        public Visit(long lts, long pd, Batch[] operations)
        {
            this.lts = lts;
            this.pd = pd;
            this.operations = operations;
        }

        public String toString()
        {
            return "Visit{" +
                   "lts=" + lts +
                   ", pd=" + pd +
                   ", operations=[" + Arrays.toString(operations) +
                                            "]}";
        }
    }

    public static class Batch
    {
        public final long m;
        public final Operation[] operations;

        public Batch(long m, Operation[] operations)
        {
            this.m = m;
            this.operations = operations;
        }

        public String toString()
        {
            return "Batch{" +
                   "m=" + m +
                   ", operations=[" + Arrays.toString(operations) +
                   "]}";
        }
    }

    public static class Operation
    {
        public final long cd;
        public final long opId;
        public final OpSelectors.OperationKind opType;

        public Operation(long cd, long opId, OpSelectors.OperationKind opType)
        {
            this.cd = cd;
            this.opId = opId;
            this.opType = opType;
        }

        public String toString()
        {
            return "Operation{" +
                   "cd=" + cd +
                   ", opId=" + opId +
                   ", opType=" + opType +
                   '}';
        }
    }
}