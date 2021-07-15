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

import harry.core.Run;
import harry.data.ResultSetRow;
import harry.ddl.SchemaSpec;
import harry.model.sut.SystemUnderTest;
import harry.reconciler.Reconciler;
import harry.runner.DataTracker;
import harry.operations.Query;

import static harry.generators.DataGenerators.NIL_DESCR;

public class QuiescentChecker implements Model
{
    protected final OpSelectors.MonotonicClock clock;

    protected final DataTracker tracker;
    protected final SystemUnderTest sut;
    protected final Reconciler reconciler;
    protected final SchemaSpec schemaSpec;

    public QuiescentChecker(Run run)
    {
        this.clock = run.clock;
        this.sut = run.sut;

        this.reconciler = new Reconciler(run);
        this.tracker = run.tracker;
        this.schemaSpec = run.schemaSpec;
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
        Reconciler.PartitionState partitionState = reconciler.inflatePartitionState(query.pd, maxSeenLts, query);
        Collection<Reconciler.RowState> expectedRows = partitionState.rows(query.reverse);

        Iterator<Reconciler.RowState> expected = expectedRows.iterator();

        // It is possible that we only get a single row in response, and it is equal to static row
        if (partitionState.isEmpty() && partitionState.staticRow() != null && actual.hasNext())
        {
            ResultSetRow actualRowState = actual.next();
            if (actualRowState.cd != partitionState.staticRow().cd)
                throw new ValidationException(partitionState.toString(schemaSpec),
                                              toString(actualRows, schemaSpec),
                                              "Found a row while model predicts statics only:" +
                                              "\nExpected: %s" +
                                              "\nActual: %s" +
                                              "\nQuery: %s",
                                              partitionState.staticRow().cd,
                                              actualRowState, query.toSelectStatement());

            for (int i = 0; i < actualRowState.vds.length; i++)
            {
                if (actualRowState.vds[i] != NIL_DESCR || actualRowState.lts[i] != NO_TIMESTAMP)
                    throw new ValidationException(partitionState.toString(schemaSpec),
                                                  toString(actualRows, schemaSpec),
                                                  "Found a row while model predicts statics only:" +
                                                  "\nActual: %s" +
                                                  "\nQuery: %s" +
                                                  "\nQuery: %s",
                                                  actualRowState, query.toSelectStatement());
            }

            assertStaticRow(partitionState, actualRows, partitionState.staticRow(), actualRowState, query, schemaSpec);
        }

        while (actual.hasNext() && expected.hasNext())
        {
            ResultSetRow actualRowState = actual.next();
            Reconciler.RowState expectedRowState = expected.next();
            // TODO: this is not necessarily true. It can also be that ordering is incorrect.
            if (actualRowState.cd != expectedRowState.cd)
                throw new ValidationException(partitionState.toString(schemaSpec),
                                              toString(actualRows, schemaSpec),
                                              "Found a row in the model that is not present in the resultset:" +
                                              "\nExpected: %s" +
                                              "\nActual: %s" +
                                              "\nQuery: %s",
                                              expectedRowState.toString(schemaSpec),
                                              actualRowState, query.toSelectStatement());

            if (!Arrays.equals(actualRowState.vds, expectedRowState.vds))
                throw new ValidationException(partitionState.toString(schemaSpec),
                                              toString(actualRows, schemaSpec),
                                              "Returned row state doesn't match the one predicted by the model:" +
                                              "\nExpected: %s (%s)" +
                                              "\nActual:   %s (%s)." +
                                              "\nQuery: %s",
                                              Arrays.toString(expectedRowState.vds), expectedRowState.toString(schemaSpec),
                                              Arrays.toString(actualRowState.vds), actualRowState,
                                              query.toSelectStatement());

            if (!Arrays.equals(actualRowState.lts, expectedRowState.lts))
                throw new ValidationException(partitionState.toString(schemaSpec),
                                              toString(actualRows, schemaSpec),
                                              "Timestamps in the row state don't match ones predicted by the model:" +
                                              "\nExpected: %s (%s)" +
                                              "\nActual:   %s (%s)." +
                                              "\nQuery: %s",
                                              Arrays.toString(expectedRowState.lts), expectedRowState.toString(schemaSpec),
                                              Arrays.toString(actualRowState.lts), actualRowState,
                                              query.toSelectStatement());

            if (partitionState.staticRow() != null || actualRowState.sds != null || actualRowState.slts != null)
                assertStaticRow(partitionState, actualRows, partitionState.staticRow(), actualRowState, query, schemaSpec);
        }

        if (actual.hasNext() || expected.hasNext())
        {
            throw new ValidationException(partitionState.toString(schemaSpec),
                                          toString(actualRows, schemaSpec),
                                          "Expected results to have the same number of results, but %s result iterator has more results." +
                                          "\nExpected: %s" +
                                          "\nActual:   %s" +
                                          "\nQuery: %s",
                                          actual.hasNext() ? "actual" : "expected",
                                          expectedRows,
                                          actualRows,
                                          query.toSelectStatement());
        }
    }

    public static void assertStaticRow(Reconciler.PartitionState partitionState,
                                       List<ResultSetRow> actualRows,
                                       Reconciler.RowState staticRow,
                                       ResultSetRow actualRowState,
                                       Query query,
                                       SchemaSpec schemaSpec)
    {
        if (!Arrays.equals(staticRow.vds, actualRowState.sds))
            throw new ValidationException(partitionState.toString(schemaSpec),
                                          toString(actualRows, schemaSpec),
                                          "Returned static row state doesn't match the one predicted by the model:" +
                                          "\nExpected: %s (%s)" +
                                          "\nActual:   %s (%s)." +
                                          "\nQuery: %s",
                                          Arrays.toString(staticRow.vds), staticRow.toString(schemaSpec),
                                          Arrays.toString(actualRowState.sds), actualRowState,
                                          query.toSelectStatement());

        if (!Arrays.equals(staticRow.lts, actualRowState.slts))
            throw new ValidationException(partitionState.toString(schemaSpec),
                                          toString(actualRows, schemaSpec),
                                          "Timestamps in the static row state don't match ones predicted by the model:" +
                                          "\nExpected: %s (%s)" +
                                          "\nActual:   %s (%s)." +
                                          "\nQuery: %s",
                                          Arrays.toString(staticRow.lts), staticRow.toString(schemaSpec),
                                          Arrays.toString(actualRowState.slts), actualRowState,
                                          query.toSelectStatement());
    }

    public static String toString(Collection<Reconciler.RowState> collection, SchemaSpec schema)
    {
        StringBuilder builder = new StringBuilder();

        for (Reconciler.RowState rowState : collection)
            builder.append(rowState.toString(schema)).append("\n");
        return builder.toString();
    }

    public static String toString(List<ResultSetRow> collection, SchemaSpec schema)
    {
        StringBuilder builder = new StringBuilder();

        for (ResultSetRow rowState : collection)
            builder.append(rowState.toString(schema)).append("\n");
        return builder.toString();
    }
}