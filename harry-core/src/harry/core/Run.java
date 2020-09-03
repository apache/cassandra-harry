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

package harry.core;

import java.util.function.Supplier;

import harry.ddl.SchemaSpec;
import harry.model.Model;
import harry.model.OpSelectors;
import harry.model.sut.SystemUnderTest;
import harry.runner.PartitionVisitor;
import harry.runner.RowVisitor;
import harry.runner.Validator;

public class Run
{
    public final OpSelectors.Rng rng;
    public final OpSelectors.MonotonicClock clock;
    public final OpSelectors.PdSelector pdSelector;

    public final OpSelectors.DescriptorSelector descriptorSelector;

    public final SchemaSpec schemaSpec;
    public final Model model;
    public final SystemUnderTest sut;
    public final Validator validator;
    public final RowVisitor rowVisitor;
    public final Supplier<PartitionVisitor> visitorFactory;

    public final Configuration snapshot;

    Run(OpSelectors.Rng rng,
        OpSelectors.MonotonicClock clock,
        OpSelectors.PdSelector pdSelector,
        OpSelectors.DescriptorSelector descriptorSelector,

        SchemaSpec schemaSpec,
        Model model,
        SystemUnderTest sut,
        Validator validator,
        RowVisitor rowVisitor,
        Supplier<PartitionVisitor> visitorFactory,
        Configuration snapshot)
    {
        this.rng = rng;
        this.clock = clock;
        this.pdSelector = pdSelector;
        this.descriptorSelector = descriptorSelector;
        this.schemaSpec = schemaSpec;
        this.model = model;
        this.sut = sut;
        this.validator = validator;
        this.rowVisitor = rowVisitor;
        this.visitorFactory = visitorFactory;
        this.snapshot = snapshot;
    }
}
