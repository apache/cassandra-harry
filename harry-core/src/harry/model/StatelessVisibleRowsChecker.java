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

import java.util.List;
import java.util.function.Supplier;

import harry.core.Configuration;
import harry.core.Run;
import harry.data.ResultSetRow;
import harry.ddl.SchemaSpec;
import harry.model.sut.SystemUnderTest;
import harry.runner.Query;
import harry.runner.QueryGenerator;

import static harry.model.VisibleRowsChecker.descendingIterator;

/**
 * A simple model to check whether or not the rows reported as visible by the database are reflected in
 * the model.
 */
public class StatelessVisibleRowsChecker implements Model
{
    protected final OpSelectors.PdSelector pdSelector;
    protected final OpSelectors.DescriptorSelector descriptorSelector;
    protected final OpSelectors.MonotonicClock clock;
    protected final SystemUnderTest sut;

    protected final SchemaSpec schema;

    public StatelessVisibleRowsChecker(Run run)
    {
        this.pdSelector = run.pdSelector;
        this.descriptorSelector = run.descriptorSelector;
        this.schema = run.schemaSpec;
        this.clock = run.clock;
        this.sut = run.sut;
    }

    public void validate(Query query)
    {
        validatePartitionState(query,
                               () -> SelectHelper.execute(sut, clock, query));
    }

    public Configuration.ModelConfiguration toConfig()
    {
        throw new RuntimeException("not implemented");
    }

    void validatePartitionState(Query query, Supplier<List<ResultSetRow>> rowsSupplier)
    {
        // we ignore Query here, since our criteria for checking in this model is presence of the row in the resultset
        long pd = query.pd;

        List<ResultSetRow> rows = rowsSupplier.get();

        for (ResultSetRow row : rows)
        {
            VisibleRowsChecker.LongIterator rowLtsIter = descendingIterator(row.lts);
            VisibleRowsChecker.LongIterator modelLtsIter = descendingIterator(pdSelector, pd);

            outer:
            while (rowLtsIter.hasNext())
            {
                long rowLts = rowLtsIter.nextLong();

                if (rowLts == NO_TIMESTAMP)
                    continue;

                if (!modelLtsIter.hasNext())
                    throw new ValidationException(String.format("Model iterator is exhausted, could not verify %d lts for the row: \n%s %s",
                                                                rowLts, row, query));

                while (modelLtsIter.hasNext())
                {
                    long modelLts = modelLtsIter.nextLong();
                    if (modelLts > rowLts)
                        continue;
                    if (modelLts < rowLts)
                        throw new RuntimeException("Can't find a corresponding event id in the model for: " + rowLts + " " + modelLts);
                    for (int col = 0; col < row.lts.length; col++)
                    {
                        if (row.lts[col] != rowLts)
                            continue;

                        long m = descriptorSelector.modificationId(pd, row.cd, rowLts, row.vds[col], col);
                        long vd = descriptorSelector.vd(pd, row.cd, rowLts, m, col);

                        if (vd != row.vds[col])
                            throw new RuntimeException("Can't verify the row");
                    }
                    continue outer;
                }
            }
        }
    }
}