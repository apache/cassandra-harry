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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Configuration;
import harry.core.Run;
import harry.data.ResultSetRow;
import harry.model.Model;
import harry.model.QuiescentChecker;
import harry.model.sut.InJvmSut;
import harry.model.sut.SystemUnderTest;
import harry.operations.CompiledStatement;

import static harry.model.SelectHelper.resultSetToRow;

public class RepairingLocalStateValidator extends AllPartitionsValidator
{
    public static void init()
    {
        Configuration.registerSubtypes(RepairingLocalStateValidatorConfiguration.class,
                                       QuiescentCheckerConfig.class);
    }

    public final InJvmSut inJvmSut;

    public RepairingLocalStateValidator(int concurrency, int triggerAfter, Run run, Model.ModelFactory modelFactory)
    {
        super(concurrency, triggerAfter, run, modelFactory);

        this.inJvmSut = (InJvmSut) run.sut;
    }

    @Override
    public void visitPartition(long lts)
    {
        if (lts > 0 && lts % triggerAfter == 0)
        {
            System.out.println("Starting repair...");
            inJvmSut.cluster().get(1).nodetool("repair", "--full");
            System.out.println("Validating partitions...");
            validateAllPartitions(executor, concurrency);
        }
    }

    @JsonTypeName("repair_and_validate_local_states")
    public static class RepairingLocalStateValidatorConfiguration implements Configuration.PartitionVisitorConfiguration
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

        public PartitionVisitor make(Run run)
        {
            return new RepairingLocalStateValidator(concurrency, trigger_after, run, modelConfiguration);
        }
    }

    public static class QuiescentLocalStateChecker extends QuiescentChecker
    {
        public final InJvmSut inJvmSut;

        public QuiescentLocalStateChecker(Run run)
        {
            super(run);
            assert run.sut instanceof InJvmSut;

            this.inJvmSut = (InJvmSut) run.sut;
        }

        @Override
        public void validate(Query query)
        {
            CompiledStatement compiled = query.toSelectStatement();
            for (int i = 1; i <= inJvmSut.cluster.size(); i++)
            {
                int node = i;
                validate(() -> {
                    Object[][] objects = inJvmSut.execute(compiled.cql(),
                                                          SystemUnderTest.ConsistencyLevel.NODE_LOCAL,
                                                          node,
                                                          compiled.bindings());
                    List<ResultSetRow> result = new ArrayList<>();
                    for (Object[] obj : objects)
                        result.add(resultSetToRow(query.schemaSpec, clock, obj));

                    return result;
                }, query);
            }
        }
    }

    @JsonTypeName("quiescent_local_state_checker")
    public static class QuiescentCheckerConfig implements Configuration.ModelConfiguration
    {
        @JsonCreator
        public QuiescentCheckerConfig()
        {
        }

        public Model make(Run run)
        {
            return new QuiescentLocalStateChecker(run);
        }
    }
}
