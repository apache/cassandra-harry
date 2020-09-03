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

public class ResultSetRow
{
    public final long pd;
    public final long cd;
    public final long[] vds;
    public final long[] lts;

    public ResultSetRow(long pd,
                        long cd,
                        long[] vds,
                        long[] lts)
    {
        this.pd = pd;
        this.cd = cd;
        this.vds = vds;
        this.lts = lts;
    }

    public String toString()
    {
        return "resultSetRow("
               + pd +
               "L, " + cd +
               "L, values(" + toString(vds) + ")" +
               ", lts(" + toString(lts) + "))";
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
}
