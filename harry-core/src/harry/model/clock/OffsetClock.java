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

package harry.model.clock;

import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Configuration;
import harry.model.OpSelectors;

public class OffsetClock implements OpSelectors.MonotonicClock
{
    final AtomicLong lts = new AtomicLong(0);

    private final long base;

    public OffsetClock(long base)
    {
        this.base = base;
    }

    public long rts(long lts)
    {
        return base + lts;
    }

    public long lts(long rts)
    {
        return rts - base;
    }

    public long currentLts()
    {
        return lts.get();
    }

    public long nextLts()
    {
        return lts.getAndIncrement();
    }

    public long maxLts()
    {
        return lts.get();
    }

    public Configuration.ClockConfiguration toConfig()
    {
        throw new RuntimeException("not implemented");
    }

    @JsonTypeName("offset")
    public static class OffsetClockConfiguration implements Configuration.ClockConfiguration
    {
        public final long offset;

        @JsonCreator
        public OffsetClockConfiguration(@JsonProperty("offset") int offset)
        {
            this.offset = offset;
        }

        public OpSelectors.MonotonicClock make()
        {
            return new OffsetClock(offset);
        }
    }
}
