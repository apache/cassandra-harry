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

import harry.core.Run;
import harry.operations.Query;

public interface Model
{
    long NO_TIMESTAMP = Long.MIN_VALUE;

    void validate(Query query);

    interface ModelFactory
    {
        Model make(Run run);
    }

    class ValidationException extends RuntimeException
    {
        public ValidationException(String partitionState, String observedState, String format, Object... objects)
        {
            super(String.format(format, objects) +
                  "\nPartition state:\n" + partitionState +
                  "\nObserved state:\n" + observedState);
        }
    }


}