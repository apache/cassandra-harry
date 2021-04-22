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

    public String toString()
    {
        return "resultSetRow("
               + pd +
               "L, " + cd +
               (sds == null ? "" : "L, values(" + toString(sds) + ")") +
               (slts == null ? "" : ", lts(" + toString(slts) + ")") +
               ", values(" + toString(vds) + ")" +
               ", lts(" + toString(lts) + ")" +
               ")";
    }

    public String toString(long[] arr)
    {
        String s = "";
        for (int i = 0; i < arr.length; i++)
        {
            s += arr[i];
            s += "L";
            if (i < (arr.length - 1))
                s += ',';
        }
        return s;
    }

    public String toString(SchemaSpec schema)
    {
        return "resultSetRow("
               + pd +
               "L, " + cd +
               (sds == null ? "" : "L, staticValues(" + toString(sds) + ")") +
               (slts == null ? "" : ", slts(" + toString(slts) + ")") +
               ", values(" + toString(vds) + ")" +
               ", lts(" + toString(lts) + ")" +
               ", clustering=" + Arrays.toString(schema.inflateClusteringKey(cd)) +
               ", values=" + Arrays.toString(schema.inflateRegularColumns(vds)) +
               (sds == null ? "" : ", statics=" + Arrays.toString(schema.inflateStaticColumns(sds))) +
               ")";
    }
}
