/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.harry;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.cassandra.harry.DataType.booleanType;
import static org.apache.cassandra.harry.DataType.cumulativeEntropy;
import static org.apache.cassandra.harry.DataType.int64Type;
import static org.apache.cassandra.harry.DataType.int8Type;
import static org.apache.cassandra.harry.DataType.MAX_ENTROPY;

public class TestStateTest
{
    @Test
    public void testCumulativeEntropy()
    {
        Assert.assertEquals(512,
                            cumulativeEntropy(Arrays.asList(int8Type,
                                                            booleanType)));

        Assert.assertEquals(256*256,
                            cumulativeEntropy(Arrays.asList(int8Type,
                                                            int8Type)));

        Assert.assertEquals(MAX_ENTROPY,
                            cumulativeEntropy(Arrays.asList(int64Type,
                                                            int8Type)));
    }
}
