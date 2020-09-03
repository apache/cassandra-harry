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

package harry.operations;

import java.util.Arrays;

public class CompiledStatement
{
    private final String cql;
    private final Object[] bindings;

    public CompiledStatement(String cql, Object... bindings)
    {
        this.cql = cql;
        this.bindings = bindings;
    }

    public String cql()
    {
        return cql;
    }

    public Object[] bindings()
    {
        return bindings;
    }

    public static CompiledStatement create(String cql, Object... bindings)
    {
        return new CompiledStatement(cql, bindings);
    }

    public String toString()
    {
        return "CompiledStatement{" +
               "cql='" + cql + '\'' +
               ", bindings=" + Arrays.toString(bindings) +
               '}';
    }
}
