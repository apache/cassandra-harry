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

import java.util.Objects;
import java.util.function.Function;

import harry.core.Configuration;
import harry.ddl.SchemaSpec;
import harry.generators.Surjections;
import harry.generators.distribution.Distribution;
import harry.util.BitSet;

import static harry.model.OpSelectors.DefaultDescriptorSelector.DEFAULT_OP_TYPE_SELECTOR;

public class DescriptorSelectorBuilder implements Configuration.CDSelectorConfiguration
{
    private Function<SchemaSpec, Function<OpSelectors.OperationKind, Surjections.Surjection<BitSet>>> columnMaskSelector;
    private Surjections.Surjection<OpSelectors.OperationKind> operationTypeSelector = DEFAULT_OP_TYPE_SELECTOR;
    private Distribution numberOfRowsDistribution = new Distribution.ScaledDistribution(2, 30);
    private Distribution numberOfModificationsDistribution = new Distribution.ScaledDistribution(1, 3);
    private int maxPartitionSize = Integer.MAX_VALUE;
    private Function<SchemaSpec, int[]> fractionsSupplier = null;

    public DescriptorSelectorBuilder setFractions(int[] fractions)
    {
        this.fractionsSupplier = (schema) -> fractions;
        return this;
    }

    public DescriptorSelectorBuilder setFractions(Function<SchemaSpec, int[]> fractions)
    {
        this.fractionsSupplier = fractions;
        return this;
    }

    public DescriptorSelectorBuilder setColumnMaskSelector(Surjections.Surjection<BitSet> selector)
    {
        this.columnMaskSelector = (schemaSpec) -> new OpSelectors.ColumnSelectorBuilder().forAll(selector).build();
        return this;
    }

    public DescriptorSelectorBuilder setColumnMaskSelector(Function<SchemaSpec, Function<OpSelectors.OperationKind, Surjections.Surjection<BitSet>>> columnMaskSelector)
    {
        this.columnMaskSelector = Objects.requireNonNull(columnMaskSelector, "mask");
        return this;
    }

    public DescriptorSelectorBuilder setOperationTypeSelector(Surjections.Surjection<OpSelectors.OperationKind> operationTypeSelector)
    {
        this.operationTypeSelector = Objects.requireNonNull(operationTypeSelector, "type");
        return this;
    }

    /**
     * In a given modification, we are only able to visit as many rows as there are rows in the partition, so
     * we'll always be limited by this.
     **/
    public DescriptorSelectorBuilder setRowsPerModificationDistribution(Distribution numberOfRowsDistribution)
    {
        this.numberOfRowsDistribution = Objects.requireNonNull(numberOfRowsDistribution, "distribution");
        return this;
    }

    public DescriptorSelectorBuilder setNumberOfModificationsDistribution(Distribution numberOfModificationsDistribution)
    {
        this.numberOfModificationsDistribution = Objects.requireNonNull(numberOfModificationsDistribution, "distribution");
        return this;
    }

    public DescriptorSelectorBuilder setMaxPartitionSize(int maxPartitionSize)
    {
        if (maxPartitionSize <= 0)
            throw new IllegalArgumentException("Max partition size should be positive");
        this.maxPartitionSize = maxPartitionSize;
        return this;
    }

    public OpSelectors.DescriptorSelector make(OpSelectors.Rng rng, SchemaSpec schemaSpec)
    {
        return new OpSelectors.DefaultDescriptorSelector(rng,
                                                         columnMaskSelector.apply(schemaSpec),
                                                         operationTypeSelector,
                                                         numberOfModificationsDistribution,
                                                         numberOfRowsDistribution,
                                                         maxPartitionSize);
    }
}
