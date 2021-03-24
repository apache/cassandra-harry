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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import harry.core.Configuration;
import harry.core.Run;
import harry.data.ResultSetRow;
import harry.ddl.SchemaSpec;
import harry.model.sut.SystemUnderTest;
import harry.reconciler.Reconciler;
import harry.runner.DataTracker;
import harry.runner.Query;
import harry.runner.QueryGenerator;

public class QuiescentChecker implements Model
{
    protected final OpSelectors.MonotonicClock clock;

    protected final DataTracker tracker;
    protected final SystemUnderTest sut;
    protected final Reconciler reconciler;

    public QuiescentChecker(Run run)
    {
        this.clock = run.clock;
        this.sut = run.sut;

        this.reconciler = new Reconciler(run.schemaSpec, run.pdSelector, run.descriptorSelector, run.rangeSelector);
        this.tracker = run.tracker;
    }

    public void validate(Query query)
    {
        validate(() -> SelectHelper.execute(sut, clock, query), query);
    }

    protected void validate(Supplier<List<ResultSetRow>> rowsSupplier, Query query)
    {
        long maxCompeteLts = tracker.maxConsecutiveFinished();
        long maxSeenLts = tracker.maxStarted();

        assert maxCompeteLts == maxSeenLts : "Runner hasn't settled down yet. " +
                                             "Quiescent model can't be reliably used in such cases.";

        List<ResultSetRow> actualRows = rowsSupplier.get();
        Iterator<ResultSetRow> actual = actualRows.iterator();
        Collection<Reconciler.RowState> expectedRows = reconciler.inflatePartitionState(query.pd, maxSeenLts, query).rows(query.reverse);
        Iterator<Reconciler.RowState> expected = expectedRows.iterator();

        while (actual.hasNext() && expected.hasNext())
        {
            ResultSetRow actualRowState = actual.next();
            Reconciler.RowState expectedRowState = expected.next();
            // TODO: this is not necessarily true. It can also be that ordering is incorrect.
            if (actualRowState.cd != expectedRowState.cd)
                throw new ValidationException("Found a row in the model that is not present in the resultset:\nExpected: %s\nActual: %s",
                                              expectedRowState, actualRowState);

            if (!Arrays.equals(actualRowState.vds, expectedRowState.vds))
                throw new ValidationException("Returned row state doesn't match the one predicted by the model:\nExpected: %s (%s)\nActual:   %s (%s).",
                                              Arrays.toString(expectedRowState.vds), expectedRowState,
                                              Arrays.toString(actualRowState.vds), actualRowState);

            if (!Arrays.equals(actualRowState.lts, expectedRowState.lts))
                throw new ValidationException("Timestamps in the row state don't match ones predicted by the model:\nExpected: %s (%s)\nActual:   %s (%s).",
                                              Arrays.toString(expectedRowState.lts), expectedRowState,
                                              Arrays.toString(actualRowState.lts), actualRowState);
        }

        if (actual.hasNext() || expected.hasNext())
        {
            throw new ValidationException("Expected results to have the same number of results, but %s result iterator has more results." +
                                          "\nExpected: %s" +
                                          "\nActual:   %s",
                                          actual.hasNext() ? "actual" : "expected",
                                          expectedRows,
                                          actualRows);
        }
    }
}