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

package harry.model;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import harry.core.Run;
import harry.data.ResultSetRow;
import harry.model.sut.SystemUnderTest;
import harry.operations.CompiledStatement;
import harry.operations.Query;

import static harry.model.SelectHelper.resultSetToRow;
import static harry.model.sut.TokenPlacementModel.Node;
import static harry.model.sut.TokenPlacementModel.Range;
import static harry.model.sut.TokenPlacementModel.getReplicas;

public abstract class QuiescentLocalStateCheckerBase extends QuiescentChecker
{
    private static final Logger logger = LoggerFactory.getLogger(QuiescentLocalStateCheckerBase.class);

    public final SystemUnderTest sut;
    public final int rf;
    private final OpSelectors.PdSelector pdSelector;

    public QuiescentLocalStateCheckerBase(Run run)
    {
        this(run, 3);
    }

    public QuiescentLocalStateCheckerBase(Run run, int rf)
    {
        super(run);
        this.sut = run.sut;
        this.rf = rf;
        this.pdSelector = run.pdSelector;
    }

    @SuppressWarnings("unused")
    public void validateAll()
    {
        NavigableMap<Range, List<Node>> ring = getRing();

        for (int lts = 0; lts < clock.peek(); lts++)
            validate(Query.selectPartition(schema, pdSelector.pd(lts, schema), false), ring);
    }

    @Override
    public void validate(Query query)
    {
        NavigableMap<Range, List<Node>> ring = getRing();
        tracker.beginValidation(query.pd);
        validate(query, ring);
        tracker.endValidation(query.pd);
    }

    protected void validate(Query query, NavigableMap<Range, List<Node>> ring)
    {
        CompiledStatement compiled = query.toSelectStatement();
        List<Node> replicas = getReplicas(ring, token(query.pd));

        logger.trace("Predicted {} as replicas for {}. Ring: {}", replicas, query.pd, ring);
        for (Node node : replicas)
        {
            try
            {
                validate(() -> {
                    Object[][] objects = executeNodeLocal(compiled.cql(), node, compiled.bindings());

                    List<ResultSetRow> result = new ArrayList<>();
                    for (Object[] obj : objects)
                        result.add(resultSetToRow(query.schemaSpec, clock, obj));

                    return result;
                }, query);
            }
            catch (ValidationException e)
            {
                throw new AssertionError(String.format("Caught error while validating replica %s of replica set %s",
                                                       node, replicas),
                                         e);
            }
        }
    }

    protected abstract NavigableMap<Range, List<Node>> getRing();
    protected abstract long token(long pd);
    protected abstract Object[][] executeNodeLocal(String statement, Node node, Object... bindings);
}
