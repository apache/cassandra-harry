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

import java.util.function.LongPredicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Configuration;
import harry.core.Run;
import harry.model.sut.injvm.InJvmSut;
import harry.visitors.AllPartitionsValidator;
import harry.visitors.Visitor;

/**
 * Validator similar to {@link AllPartitionsValidator}, but performs
 * repair before checking node states.
 *
 * Can be useful for testing repair, bootstrap, and streaming code.
 */
public class RepairingLocalStateValidator extends AllPartitionsValidator
{
    private final InJvmSut inJvmSut;
    private final OpSelectors.MonotonicClock clock;

    public static Configuration.VisitorConfiguration factory(int concurrency, LongPredicate condition, Model.ModelFactory modelFactory)
    {
        return (r) -> new RepairingLocalStateValidator(r, concurrency, condition, modelFactory);
    }

    public RepairingLocalStateValidator(Run run, int concurrency, long triggerAfter, Model.ModelFactory modelFactory)
    {
        this(run, concurrency, (lts) -> lts > 0 && lts % triggerAfter == 0, modelFactory);
    }

    public RepairingLocalStateValidator(Run run, int concurrency, LongPredicate condition, Model.ModelFactory modelFactory)
    {
        super(run, concurrency, condition, modelFactory);
        this.inJvmSut = (InJvmSut) run.sut;
        this.clock = run.clock;
    }

    public void visit()
    {
        long lts = clock.peek();
        if (condition.test(lts))
        {
            System.out.println("Starting repair...");
            inJvmSut.cluster().stream().forEach((instance) -> instance.nodetool("repair", "--full"));
            System.out.println("Validating partitions...");
            super.visit();
        }
    }

    @JsonTypeName("repair_and_validate_local_states")
    public static class RepairingLocalStateValidatorConfiguration implements Configuration.VisitorConfiguration
    {
        private final int concurrency;
        private final int trigger_after;
        private final Configuration.ModelConfiguration modelConfiguration;

        @JsonCreator
        public RepairingLocalStateValidatorConfiguration(@JsonProperty("concurrency") int concurrency,
                                                         @JsonProperty("trigger_after") int trigger_after,
                                                         @JsonProperty("model") Configuration.ModelConfiguration model)
        {
            this.concurrency = concurrency;
            this.trigger_after = trigger_after;
            this.modelConfiguration = model;
        }

        public Visitor make(Run run)
        {
            return new RepairingLocalStateValidator(run, concurrency, trigger_after, modelConfiguration);
        }
    }
}
