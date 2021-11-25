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

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;

import harry.core.Configuration;

public abstract class DataTracker
{
    protected List<LongConsumer> onStarted = new ArrayList<>();
    protected List<LongConsumer> onFinished = new ArrayList<>();

    public void onLtsStarted(LongConsumer onLts)
    {
        this.onStarted.add(onLts);
    }

    public void onLtsFinished(LongConsumer onLts)
    {
        this.onFinished.add(onLts);
    }

    public void started(long lts)
    {
        startedInternal(lts);
        for (LongConsumer consumer : onStarted)
            consumer.accept(lts);
    }

    public void finished(long lts)
    {
        finishedInternal(lts);
        for (LongConsumer consumer : onFinished)
            consumer.accept(lts);
    }

    abstract void startedInternal(long lts);
    abstract void finishedInternal(long lts);

    public abstract long maxStarted();
    public abstract long maxConsecutiveFinished();

    public abstract Configuration.DataTrackerConfiguration toConfig();

    public static interface DataTrackerFactory
    {
        DataTracker make();
    }

    public static final DataTracker NO_OP = new NoOpDataTracker();

    public static class NoOpDataTracker extends DataTracker
    {
        private NoOpDataTracker() {}

        protected void startedInternal(long lts) {}
        protected void finishedInternal(long lts) {}
        public long maxStarted() { return 0; }
        public long maxConsecutiveFinished() { return 0; }

        public Configuration.DataTrackerConfiguration toConfig()
        {
            return null;
        }
    }
}
