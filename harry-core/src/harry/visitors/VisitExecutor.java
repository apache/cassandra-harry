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

import harry.model.OpSelectors;

public abstract class VisitExecutor
{
    protected abstract void beforeLts(long lts, long pd);

    protected abstract void afterLts(long lts, long pd);

    protected abstract void beforeBatch(long lts, long pd, long m);

    protected abstract void operation(long lts, long pd, long cd, long m, long opId, OpSelectors.OperationKind kind);

    protected abstract void afterBatch(long lts, long pd, long m);

    public abstract void shutdown() throws InterruptedException;
}
