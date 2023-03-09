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

package harry.data;

import java.util.Arrays;

import harry.ddl.SchemaSpec;
import harry.util.StringUtils;

public class ResultSetRow
{
    public final long pd;
    public final long cd;
    public final long[] vds;
    public final long[] lts;

    public final long[] sds;
    public final long[] slts;

    public ResultSetRow(long pd,
                        long cd,
                        long[] sds,
                        long[] slts,
                        long[] vds,
                        long[] lts)
    {
        this.pd = pd;
        this.cd = cd;
        this.vds = vds;
        this.lts = lts;
        this.sds = sds;
        this.slts = slts;
    }

    public ResultSetRow clone()
    {
        return new ResultSetRow(pd, cd,
                                Arrays.copyOf(sds, sds.length), Arrays.copyOf(slts, slts.length),
                                Arrays.copyOf(vds, vds.length), Arrays.copyOf(lts, lts.length));
    }

    public String toString()
    {
        return "resultSetRow("
               + pd +
               "L, " + cd +
               (sds == null ? "" : "L, values(" + StringUtils.toString(sds) + ")") +
               (slts == null ? "" : ", lts(" + StringUtils.toString(slts) + ")") +
               ", values(" + StringUtils.toString(vds) + ")" +
               ", lts(" + StringUtils.toString(lts) + ")" +
               ")";
    }

    public String toString(SchemaSpec schema)
    {
        return "resultSetRow("
               + pd +
               "L, " + cd +
               (sds == null ? "" : "L, staticValues(" + StringUtils.toString(sds) + ")") +
               (slts == null ? "" : ", slts(" + StringUtils.toString(slts) + ")") +
               ", values(" + StringUtils.toString(vds) + ")" +
               ", lts(" + StringUtils.toString(lts) + ")" +
               ", clustering=" + Arrays.toString(schema.inflateClusteringKey(cd)) +
               ", values=" + Arrays.toString(schema.inflateRegularColumns(vds)) +
               (sds == null ? "" : ", statics=" + Arrays.toString(schema.inflateStaticColumns(sds))) +
               ")";
    }
}
