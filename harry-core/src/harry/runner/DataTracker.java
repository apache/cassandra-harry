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

package harry.runner;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Configuration;

public interface DataTracker
{
    void started(long lts);
    void finished(long lts);

    long maxStarted();
    long maxConsecutiveFinished();

    public Configuration.DataTrackerConfiguration toConfig();

    interface DataTrackerFactory {
        DataTracker make();
    }

    public static DataTracker NO_OP = new NoOpDataTracker();

    class NoOpDataTracker implements DataTracker
    {
        private NoOpDataTracker() {}

        public void started(long lts) {}
        public void finished(long lts) {}
        public long maxStarted() { return 0; }
        public long maxConsecutiveFinished() { return 0; }

        public Configuration.DataTrackerConfiguration toConfig()
        {
            return null;
        }
    }
}
