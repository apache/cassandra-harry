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

public interface RowVisitor
{
    interface RowVisitorFactory
    {
        RowVisitor make(SchemaSpec schema,
                        OpSelectors.MonotonicClock clock,
                        OpSelectors.DescriptorSelector descriptorSelector,
                        QuerySelector querySelector);
    }

    default CompiledStatement visitRow(OpSelectors.OperationKind op, long lts, long pd, long cd, long opId)
    {
        switch (op)
        {
            // TODO: switch to EnumMap
            // TODO: pluggable capabilities; OperationKind can/should bear its own logic
            case WRITE:
                return write(lts, pd, cd, opId);
            case DELETE_ROW:
                return deleteRow(lts, pd, cd, opId);
            case DELETE_COLUMN:
                return deleteColumn(lts, pd, cd, opId);
            case DELETE_RANGE:
                return deleteRange(lts, pd, opId);
            default:
                throw new IllegalStateException();
        }
    }

    CompiledStatement write(long lts, long pd, long cd, long opId);

    CompiledStatement deleteColumn(long lts, long pd, long cd, long opId);

    CompiledStatement deleteRow(long lts, long pd, long cd, long opId);

    CompiledStatement deleteRange(long lts, long pd, long opId);
}