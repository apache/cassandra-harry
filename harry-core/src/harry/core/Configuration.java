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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.generators.Surjections;
import harry.generators.distribution.Distribution;
import harry.model.ExhaustiveChecker;
import harry.model.Model;
import harry.model.OpSelectors;
import harry.model.QuiescentChecker;
import harry.model.clock.ApproximateMonotonicClock;
import harry.model.sut.SystemUnderTest;
import harry.runner.DefaultPartitionVisitorFactory;
import harry.runner.DefaultRowVisitor;
import harry.runner.PartitionVisitor;
import harry.runner.Query;
import harry.runner.QuerySelector;
import harry.runner.RowVisitor;
import harry.runner.Runner;
import harry.runner.Validator;
import harry.util.BitSet;

public class Configuration
{
    private static final ObjectMapper mapper;

    static
    {
        mapper = new ObjectMapper(new YAMLFactory()
                                  .disable(YAMLGenerator.Feature.USE_NATIVE_TYPE_ID)
                                  .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                                  .disable(YAMLGenerator.Feature.CANONICAL_OUTPUT)
                                  .enable(YAMLGenerator.Feature.INDENT_ARRAYS));
        mapper.registerSubtypes(Configuration.DebugApproximateMonotonicClockConfiguration.class);
        mapper.registerSubtypes(Configuration.ConcurrentRunnerConfig.class);

        mapper.registerSubtypes(Configuration.ExhaustiveCheckerConfig.class);
        mapper.registerSubtypes(Configuration.DefaultCDSelectorConfiguration.class);
        mapper.registerSubtypes(Configuration.DefaultPDSelectorConfiguration.class);
        mapper.registerSubtypes(Configuration.ConstantDistributionConfig.class);
        mapper.registerSubtypes(DefaultSchemaProviderConfiguration.class);
        mapper.registerSubtypes(DefaultRowVisitorConfiguration.class);
    }

    public final long seed;
    public final SchemaProviderConfiguration schema_provider;

    public final boolean drop_schema;
    public final boolean create_schema;
    public final boolean truncate_table;

    public final ClockConfiguration clock;
    public final RunnerConfiguration runner;
    public final SutConfiguration system_under_test;
    public final ModelConfiguration model;
    public final RowVisitorConfiguration row_visitor;

    public final PDSelectorConfiguration partition_descriptor_selector;
    public final CDSelectorConfiguration clustering_descriptor_selector;

    public final long run_time;
    public final TimeUnit run_time_unit;

    @JsonCreator
    public Configuration(@JsonProperty("seed") long seed,
                         @JsonProperty("schema_provider") SchemaProviderConfiguration schema_provider,
                         @JsonProperty("drop_schema") boolean drop_schema,
                         @JsonProperty("create_schema") boolean create_schema,
                         @JsonProperty("truncate_schema") boolean truncate_table,
                         @JsonProperty("clock") ClockConfiguration clock,
                         @JsonProperty("runner") RunnerConfiguration runner,
                         @JsonProperty("system_under_test") SutConfiguration system_under_test,
                         @JsonProperty("model") ModelConfiguration model,
                         @JsonProperty("row_visitor") RowVisitorConfiguration row_visitor,
                         @JsonProperty("partition_descriptor_selector") PDSelectorConfiguration partition_descriptor_selector,
                         @JsonProperty("clustering_descriptor_selector") CDSelectorConfiguration clustering_descriptor_selector,
                         @JsonProperty(value = "run_time", defaultValue = "2") long run_time,
                         @JsonProperty(value = "run_time_unit", defaultValue = "HOURS") TimeUnit run_time_unit)
    {
        this.seed = seed;
        this.schema_provider = schema_provider;
        this.drop_schema = drop_schema;
        this.create_schema = create_schema;
        this.truncate_table = truncate_table;
        this.clock = clock;
        this.runner = runner;
        this.system_under_test = system_under_test;
        this.model = model;
        this.row_visitor = row_visitor;
        this.partition_descriptor_selector = partition_descriptor_selector;
        this.clustering_descriptor_selector = clustering_descriptor_selector;
        this.run_time = run_time;
        this.run_time_unit = run_time_unit;
    }

    public static void registerSubtypes(Class<?>... classes)
    {
        mapper.registerSubtypes(classes);
    }

    public static String toYamlString(Configuration config)
    {
        try
        {
            return mapper.writeValueAsString(config);
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    public static Configuration fromYamlString(String config)
    {
        try
        {
            return mapper.readValue(config, Configuration.class);
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    public static Configuration fromFile(String path)
    {
        try
        {
            return mapper.readValue(new File(path), Configuration.class);
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    public static void validate(Configuration config)
    {
        // TODO: validation
        //assert historySize * clockEpochTimeUnit.toMillis(clockEpoch) > runTimePeriod.toMillis(runTime) : "History size is too small for this run";
    }

    public Runner createRunner()
    {
        return createRunner(this);
    }

    public Run createRun()
    {
        return createRun(this);
    }

    public static Run createRun(Configuration snapshot)
    {
        long seed = snapshot.seed;

        OpSelectors.Rng rng = new OpSelectors.PCGFast(seed);

        OpSelectors.MonotonicClock clock = snapshot.clock.make();

        // TODO: parsing schema
        SchemaSpec schemaSpec = snapshot.schema_provider.make(seed);

        OpSelectors.PdSelector pdSelector = snapshot.partition_descriptor_selector.make(rng);
        OpSelectors.DescriptorSelector descriptorSelector = snapshot.clustering_descriptor_selector.make(rng, schemaSpec);

        SystemUnderTest sut = snapshot.system_under_test.make();
        QuerySelector querySelector = new QuerySelector(schemaSpec,
                                                        pdSelector,
                                                        descriptorSelector,
                                                        Surjections.pick(Query.QueryKind.CLUSTERING_SLICE,
                                                                         Query.QueryKind.CLUSTERING_RANGE),
                                                        rng);
        Model model = snapshot.model.create(schemaSpec, pdSelector, descriptorSelector, clock, querySelector, sut);
        Validator validator = new Validator(model, schemaSpec, clock, pdSelector, descriptorSelector, rng);

        RowVisitor rowVisitor;
        if (snapshot.row_visitor != null)
            rowVisitor = snapshot.row_visitor.make(schemaSpec, clock, descriptorSelector, querySelector);
        else
            rowVisitor = new DefaultRowVisitor(schemaSpec, clock, descriptorSelector, querySelector);

        // TODO: make this one configurable, too?
        Supplier<PartitionVisitor> visitorFactory = new DefaultPartitionVisitorFactory(model, sut, pdSelector, descriptorSelector, schemaSpec, rowVisitor);
        return new Run(rng,
                       clock,
                       pdSelector,
                       descriptorSelector,
                       schemaSpec,
                       model,
                       sut,
                       validator,
                       rowVisitor,
                       visitorFactory,
                       snapshot);
    }

    public static Runner createRunner(Configuration snapshot)
    {
        Run run = createRun(snapshot);
        return snapshot.runner.make(run);
    }

    public static class ConfigurationBuilder
    {
        long seed;
        SchemaProviderConfiguration schema_provider = new DefaultSchemaProviderConfiguration();

        boolean drop_schema;
        boolean create_schema;
        boolean truncate_table;

        ClockConfiguration clock;
        RunnerConfiguration runner;
        SutConfiguration system_under_test;
        ModelConfiguration model;
        RowVisitorConfiguration row_visitor = new DefaultRowVisitorConfiguration();

        PDSelectorConfiguration partition_descriptor_selector = new Configuration.DefaultPDSelectorConfiguration(10, 100);
        CDSelectorConfiguration clustering_descriptor_selector; // TODO: sensible default value

        long run_time = 2;
        TimeUnit run_time_unit = TimeUnit.HOURS;

        public ConfigurationBuilder setSeed(long seed)
        {
            this.seed = seed;
            return this;
        }

        public ConfigurationBuilder setSchemaProvider(SchemaProviderConfiguration schema_provider)
        {
            this.schema_provider = schema_provider;
            return this;
        }

        public ConfigurationBuilder setRunTime(long runTime, TimeUnit runTimeUnit)
        {
            this.run_time_unit = Objects.requireNonNull(runTimeUnit, "unit");
            this.run_time = runTime;
            return this;
        }

        public ConfigurationBuilder setClock(ClockConfiguration clock)
        {
            this.clock = clock;
            return this;
        }


        public ConfigurationBuilder setSUT(SutConfiguration system_under_test)
        {
            this.system_under_test = system_under_test;
            return this;
        }

        public ConfigurationBuilder setDropSchema(boolean drop_schema)
        {
            this.drop_schema = drop_schema;
            return this;
        }

        public ConfigurationBuilder setCreateSchema(boolean create_schema)
        {
            this.create_schema = create_schema;
            return this;
        }

        public ConfigurationBuilder setTruncateTable(boolean truncate_table)
        {
            this.truncate_table = truncate_table;
            return this;
        }

        public ConfigurationBuilder setModel(ModelConfiguration model)
        {
            this.model = model;
            return this;
        }

        public ConfigurationBuilder setRunner(RunnerConfiguration runner)
        {
            this.runner = runner;
            return this;
        }

        public ConfigurationBuilder setPartitionDescriptorSelector(PDSelectorConfiguration partition_descriptor_selector)
        {
            this.partition_descriptor_selector = partition_descriptor_selector;
            return this;
        }

        public ConfigurationBuilder setClusteringDescriptorSelector(CDSelectorConfiguration builder)
        {
            this.clustering_descriptor_selector = builder;
            return this;
        }

        public ConfigurationBuilder setClusteringDescriptorSelector(Consumer<CDSelectorConfigurationBuilder> build)
        {
            CDSelectorConfigurationBuilder builder = new CDSelectorConfigurationBuilder();
            build.accept(builder);
            return setClusteringDescriptorSelector(builder.build());
        }

        public ConfigurationBuilder setRowVisitor(RowVisitorConfiguration row_visitor)
        {
            this.row_visitor = row_visitor;
            return this;
        }

        public Configuration build()
        {
            return new Configuration(seed,
                                     Objects.requireNonNull(schema_provider, "Schema provider should not be null"),
                                     drop_schema,
                                     create_schema,
                                     truncate_table,

                                     Objects.requireNonNull(clock, "Clock should not be null"),
                                     runner,
                                     Objects.requireNonNull(system_under_test, "System under test should not be null"),
                                     Objects.requireNonNull(model, "Model should not be null"),
                                     Objects.requireNonNull(row_visitor, "Row visitor should not be null"),

                                     Objects.requireNonNull(partition_descriptor_selector, "Partition descriptor selector should not be null"),
                                     Objects.requireNonNull(clustering_descriptor_selector, "Clustering descriptor selector should not be null"),

                                     run_time,
                                     run_time_unit);
        }
    }

    public ConfigurationBuilder unbuild()
    {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.seed = seed;
        builder.schema_provider = schema_provider;
        builder.drop_schema = drop_schema;
        builder.create_schema = create_schema;
        builder.truncate_table = truncate_table;

        builder.clock = clock;
        builder.runner = runner;
        builder.system_under_test = system_under_test;
        builder.model = model;
        builder.row_visitor = row_visitor;

        builder.partition_descriptor_selector = partition_descriptor_selector;
        builder.clustering_descriptor_selector = clustering_descriptor_selector;

        builder.run_time = run_time;
        builder.run_time_unit = run_time_unit;
        return builder;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface ClockConfiguration extends OpSelectors.MonotonicClockFactory
    {
    }

    @JsonTypeName("approximate_monotonic")
    public static class ApproximateMonotonicClockConfiguration implements ClockConfiguration
    {
        public final int history_size;
        public final int epoch_length;
        public final TimeUnit epoch_time_unit;

        @JsonCreator
        public ApproximateMonotonicClockConfiguration(@JsonProperty("history_size") int history_size,
                                                      @JsonProperty("epoch_length") int epoch_length,
                                                      @JsonProperty("epoch_time_unit") TimeUnit epoch_time_unit)
        {
            this.history_size = history_size;
            this.epoch_length = epoch_length;
            this.epoch_time_unit = epoch_time_unit;
        }

        public OpSelectors.MonotonicClock make()
        {
            return new ApproximateMonotonicClock(history_size,
                                                 epoch_length,
                                                 epoch_time_unit);
        }
    }

    @JsonTypeName("debug_approximate_monotonic")
    public static class DebugApproximateMonotonicClockConfiguration implements ClockConfiguration
    {
        public final long startTimeMicros;
        public final int historySize;
        public final long[] history;
        public final long lts;
        public final int idx;
        public final long epochPeriod;
        public final TimeUnit epochTimeUnit;

        @JsonCreator
        public DebugApproximateMonotonicClockConfiguration(@JsonProperty("start_time_micros") long startTimeMicros,
                                                           @JsonProperty("history_size") int historySize,
                                                           @JsonProperty("history") long[] history,
                                                           @JsonProperty("lts") long lts,
                                                           @JsonProperty("idx") int idx,
                                                           @JsonProperty("epoch_period") long epochPeriod,
                                                           @JsonProperty("epoch_time_unit") TimeUnit epochTimeUnit)
        {
            this.startTimeMicros = startTimeMicros;
            this.historySize = historySize;
            this.history = history;
            this.lts = lts;
            this.idx = idx;
            this.epochPeriod = epochPeriod;
            this.epochTimeUnit = epochTimeUnit;
        }

        public OpSelectors.MonotonicClock make()
        {
            return ApproximateMonotonicClock.forDebug(startTimeMicros,
                                                      historySize,
                                                      lts,
                                                      idx,
                                                      epochPeriod,
                                                      epochTimeUnit,
                                                      history);
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface RunnerConfiguration extends Runner.RunnerFactory
    {
    }

    @JsonTypeName("concurrent")
    public static class ConcurrentRunnerConfig implements RunnerConfiguration
    {
        public final int writer_threads;
        public final int round_robin_validator_threads;
        public final int recent_partition_validator_threads;

        @JsonCreator
        public ConcurrentRunnerConfig(@JsonProperty(value = "writer_threads", defaultValue = "2") int writer_threads,
                                      @JsonProperty(value = "round_robin_validator_threads", defaultValue = "2") int round_robin_validator_threads,
                                      @JsonProperty(value = "recent_partition_validator_threads", defaultValue = "2") int recent_partition_validator_threads)
        {
            this.writer_threads = writer_threads;
            this.round_robin_validator_threads = round_robin_validator_threads;
            this.recent_partition_validator_threads = recent_partition_validator_threads;
        }

        public Runner make(Run run)
        {
            return new Runner.ConcurrentRunner(run, writer_threads, round_robin_validator_threads, recent_partition_validator_threads);
        }
    }

    @JsonTypeName("sequential")
    public static class SequentialRunnerConfig implements RunnerConfiguration
    {
        private final int round_robin_validator_threads;
        private final int check_recent_after;
        private final int check_all_after;

        @JsonCreator
        public SequentialRunnerConfig(@JsonProperty(value = "round_robin_validator_threads", defaultValue = "2") int round_robin_validator_threads,
                                      @JsonProperty(value = "check_recent_after", defaultValue = "100") int check_recent_after,
                                      @JsonProperty(value = "check_all_after", defaultValue = "5000") int check_all_after)
        {
            this.round_robin_validator_threads = round_robin_validator_threads;
            this.check_recent_after = check_recent_after;
            this.check_all_after = check_all_after;
        }

        public Runner make(Run run)
        {
            return new Runner.SequentialRunner(run, round_robin_validator_threads, check_recent_after, check_all_after);
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface SutConfiguration extends SystemUnderTest.SUTFactory
    {
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface ModelConfiguration extends Model.ModelFactory
    {
    }

    @JsonTypeName("exhaustive_checker")
    public static class ExhaustiveCheckerConfig implements ModelConfiguration
    {
        public final long max_seen_lts;
        public final long max_complete_lts;

        public ExhaustiveCheckerConfig()
        {
            this(-1, -1);
        }

        @JsonCreator
        public ExhaustiveCheckerConfig(@JsonProperty(value = "max_seen_lts", defaultValue = "-1") long max_seen_lts,
                                       @JsonProperty(value = "max_complete_lts", defaultValue = "-1") long max_complete_lts)
        {
            this.max_seen_lts = max_seen_lts;
            this.max_complete_lts = max_complete_lts;
        }

        public Model create(SchemaSpec schema, OpSelectors.PdSelector pdSelector, OpSelectors.DescriptorSelector descriptorSelector, OpSelectors.MonotonicClock clock, QuerySelector querySelector, SystemUnderTest sut)
        {
            ExhaustiveChecker exhaustiveChecker = new ExhaustiveChecker(schema,
                                                                        pdSelector,
                                                                        descriptorSelector,
                                                                        clock,
                                                                        querySelector,
                                                                        sut);
            exhaustiveChecker.forceLts(max_seen_lts, max_complete_lts);
            return exhaustiveChecker;
        }
    }

    @JsonTypeName("quiescent_checker")
    public static class QuiescentCheckerConfig implements ModelConfiguration
    {
        public final long max_seen_lts;
        public final long max_complete_lts;

        public QuiescentCheckerConfig()
        {
            this(-1, -1);
        }

        @JsonCreator
        public QuiescentCheckerConfig(@JsonProperty(value = "max_seen_lts", defaultValue = "-1") long max_seen_lts,
                                      @JsonProperty(value = "max_complete_lts", defaultValue = "-1") long max_complete_lts)
        {
            this.max_seen_lts = max_seen_lts;
            this.max_complete_lts = max_complete_lts;
        }

        public Model create(SchemaSpec schema, OpSelectors.PdSelector pdSelector, OpSelectors.DescriptorSelector descriptorSelector, OpSelectors.MonotonicClock clock, QuerySelector querySelector, SystemUnderTest sut)
        {
            QuiescentChecker exhaustiveChecker = new QuiescentChecker(schema,
                                                                      pdSelector,
                                                                      descriptorSelector,
                                                                      clock,
                                                                      querySelector,
                                                                      sut);
            exhaustiveChecker.forceLts(max_seen_lts, max_complete_lts);
            return exhaustiveChecker;
        }
    }


    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface PDSelectorConfiguration extends OpSelectors.PdSelectorFactory
    {
    }

    @JsonTypeName("default")
    public static class DefaultPDSelectorConfiguration implements PDSelectorConfiguration
    {
        public final int window_size;
        public final int slide_after_repeats;

        @JsonCreator
        public DefaultPDSelectorConfiguration(@JsonProperty(value = "window_size", defaultValue = "10") int window_size,
                                              @JsonProperty(value = "slide_after_repeats", defaultValue = "100") int slide_after_repeats)
        {
            this.window_size = window_size;
            this.slide_after_repeats = slide_after_repeats;
        }

        public OpSelectors.PdSelector make(OpSelectors.Rng rng)
        {
            return new OpSelectors.DefaultPdSelector(rng, window_size, slide_after_repeats);
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface CDSelectorConfiguration extends OpSelectors.DescriptorSelectorFactory
    {
    }

    public static class WeightedSelectorBuilder<T>
    {
        private final Map<T, Integer> operation_kind_weights;

        public WeightedSelectorBuilder()
        {
            operation_kind_weights = new HashMap<>();
        }

        public WeightedSelectorBuilder addWeight(T v, int weight)
        {
            operation_kind_weights.put(v, weight);
            return this;
        }

        public Map<T, Integer> build()
        {
            return operation_kind_weights;
        }
    }

    public static class OperationKindSelectorBuilder extends WeightedSelectorBuilder<OpSelectors.OperationKind>
    {
    }

    // TODO: configure fractions/fractional builder
    public static class CDSelectorConfigurationBuilder
    {
        private DistributionConfig modifications_per_lts = new ConstantDistributionConfig(10);
        private DistributionConfig rows_per_modification = new ConstantDistributionConfig(10);
        private int max_partition_size = 100;
        private Map<OpSelectors.OperationKind, Integer> operation_kind_weights = new OperationKindSelectorBuilder()
                                                                                 .addWeight(OpSelectors.OperationKind.DELETE_ROW, 1)
                                                                                 .addWeight(OpSelectors.OperationKind.DELETE_COLUMN, 1)
                                                                                 .addWeight(OpSelectors.OperationKind.WRITE, 98)
                                                                                 .build();
        private Map<OpSelectors.OperationKind, long[]> column_mask_bitsets;
        private int[] fractions;

        public CDSelectorConfigurationBuilder setNumberOfModificationsDistribution(DistributionConfig modifications_per_lts)
        {
            this.modifications_per_lts = modifications_per_lts;
            return this;
        }

        public CDSelectorConfigurationBuilder setRowsPerModificationDistribution(DistributionConfig rows_per_modification)
        {
            this.rows_per_modification = rows_per_modification;
            return this;
        }

        public CDSelectorConfigurationBuilder setMaxPartitionSize(int max_partition_size)
        {
            if (max_partition_size <= 0)
                throw new IllegalArgumentException("Max partition size should be positive");
            this.max_partition_size = max_partition_size;
            return this;
        }

        public CDSelectorConfigurationBuilder setOperationKindWeights(Map<OpSelectors.OperationKind, Integer> operation_kind_weights)
        {
            this.operation_kind_weights = operation_kind_weights;
            return this;
        }

        public CDSelectorConfigurationBuilder setColumnMasks(Map<OpSelectors.OperationKind, long[]> column_mask_bitsets)
        {
            this.column_mask_bitsets = column_mask_bitsets;
            return this;
        }

        public void setFractions(int[] fractions)
        {
            this.fractions = fractions;
        }

        public DefaultCDSelectorConfiguration build()
        {
            if (fractions == null)
            {
                return new DefaultCDSelectorConfiguration(modifications_per_lts,
                                                          rows_per_modification,
                                                          max_partition_size,
                                                          operation_kind_weights,
                                                          column_mask_bitsets);
            }
            else
            {
                return new HierarchicalCDSelectorConfiguration(modifications_per_lts,
                                                               rows_per_modification,
                                                               max_partition_size,
                                                               operation_kind_weights,
                                                               column_mask_bitsets,
                                                               fractions);
            }
        }
    }

    @JsonTypeName("default")
    public static class DefaultCDSelectorConfiguration implements CDSelectorConfiguration
    {
        public final DistributionConfig modifications_per_lts;
        public final DistributionConfig rows_per_modification;
        public final int max_partition_size;
        public final Map<OpSelectors.OperationKind, Integer> operation_kind_weights;
        public final Map<OpSelectors.OperationKind, long[]> column_mask_bitsets;

        @JsonCreator
        public DefaultCDSelectorConfiguration(@JsonProperty("modifications_per_lts") DistributionConfig modifications_per_lts,
                                              @JsonProperty("rows_per_modification") DistributionConfig rows_per_modification,
                                              @JsonProperty(value = "window_size", defaultValue = "100") int max_partition_size,
                                              @JsonProperty("operation_kind_weights") Map<OpSelectors.OperationKind, Integer> operation_kind_weights,
                                              @JsonProperty("column_mask_bitsets") Map<OpSelectors.OperationKind, long[]> column_mask_bitsets)
        {
            this.modifications_per_lts = modifications_per_lts;
            this.rows_per_modification = rows_per_modification;
            this.max_partition_size = max_partition_size;
            this.operation_kind_weights = operation_kind_weights;
            this.column_mask_bitsets = column_mask_bitsets;
        }

        protected Function<OpSelectors.OperationKind, Surjections.Surjection<BitSet>> columnSelector(SchemaSpec schemaSpec)
        {
            Function<OpSelectors.OperationKind, Surjections.Surjection<BitSet>> columnSelector;
            if (column_mask_bitsets == null)
            {
                columnSelector = OpSelectors.columnSelectorBuilder().forAll(schemaSpec.regularColumns.size()).build();
            }
            else
            {
                Map<OpSelectors.OperationKind, Surjections.Surjection<BitSet>> m = new HashMap<>();
                for (Map.Entry<OpSelectors.OperationKind, long[]> entry : column_mask_bitsets.entrySet())
                {
                    List<BitSet> bitSets = new ArrayList<>(entry.getValue().length);
                    for (long raw_bitset : entry.getValue())
                        bitSets.add(BitSet.create(raw_bitset, schemaSpec.regularColumns.size()));
                    Surjections.Surjection<BitSet> selector = Surjections.pick(bitSets);
                    m.put(entry.getKey(), selector);
                }
                columnSelector = m::get;
            }

            return columnSelector;
        }

        public OpSelectors.DescriptorSelector make(OpSelectors.Rng rng, SchemaSpec schemaSpec)
        {
            return new OpSelectors.DefaultDescriptorSelector(rng,
                                                             columnSelector(schemaSpec),
                                                             Surjections.weighted(operation_kind_weights),
                                                             modifications_per_lts.make(),
                                                             rows_per_modification.make(),
                                                             max_partition_size);
        }
    }

    public static class HierarchicalCDSelectorConfiguration extends DefaultCDSelectorConfiguration
    {
        private final int[] fractions;

        public HierarchicalCDSelectorConfiguration(DistributionConfig modifications_per_lts,
                                                   DistributionConfig rows_per_modification,
                                                   int max_partition_size,
                                                   Map<OpSelectors.OperationKind, Integer> operation_kind_weights,
                                                   Map<OpSelectors.OperationKind, long[]> column_mask_bitsets,
                                                   int[] fractions)
        {
            super(modifications_per_lts, rows_per_modification, max_partition_size, operation_kind_weights, column_mask_bitsets);
            this.fractions = fractions;
        }

        public OpSelectors.DescriptorSelector make(OpSelectors.Rng rng, SchemaSpec schemaSpec)
        {
            return new OpSelectors.HierarchicalDescriptorSelector(rng,
                                                                  fractions,
                                                                  columnSelector(schemaSpec),
                                                                  Surjections.weighted(operation_kind_weights),
                                                                  modifications_per_lts.make(),
                                                                  rows_per_modification.make(),
                                                                  max_partition_size);
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    public interface DistributionConfig extends Distribution.DistributionFactory
    {
    }

    @JsonTypeName("identity")
    public static class IdentityDistributionConfig implements DistributionConfig
    {
        @JsonCreator
        public IdentityDistributionConfig()
        {
        }

        public Distribution make()
        {
            return new Distribution.IdentityDistribution();
        }
    }

    @JsonTypeName("normal")
    public static class NormalDistributionConfig implements DistributionConfig
    {
        @JsonCreator
        public NormalDistributionConfig()
        {
        }

        public Distribution make()
        {
            return new Distribution.NormalDistribution();
        }
    }

    @JsonTypeName("constant")
    public static class ConstantDistributionConfig implements DistributionConfig
    {
        public final long constant;

        @JsonCreator
        public ConstantDistributionConfig(@JsonProperty("constant") long constant)
        {
            this.constant = constant;
        }

        public Distribution make()
        {
            return new Distribution.ConstantDistribution(constant);
        }
    }

    @JsonTypeName("scaled")
    public static class ScaledDistributionConfig implements DistributionConfig
    {
        private final long min;
        private final long max;

        @JsonCreator
        public ScaledDistributionConfig(long min, long max)
        {
            this.min = min;
            this.max = max;
        }

        public Distribution make()
        {
            return new Distribution.ScaledDistribution(min, max);
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface RowVisitorConfiguration extends RowVisitor.RowVisitorFactory
    {
    }

    @JsonTypeName("default")
    public static class DefaultRowVisitorConfiguration implements RowVisitorConfiguration
    {
        public RowVisitor make(SchemaSpec schema,
                               OpSelectors.MonotonicClock clock,
                               OpSelectors.DescriptorSelector descriptorSelector,
                               QuerySelector querySelector)
        {
            return new DefaultRowVisitor(schema,
                                         clock,
                                         descriptorSelector,
                                         querySelector);
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface SchemaProviderConfiguration extends SchemaSpec.SchemaSpecFactory
    {
    }

    @JsonTypeName("default")
    public static class DefaultSchemaProviderConfiguration implements SchemaProviderConfiguration
    {
        public SchemaSpec make(long seed)
        {
            return SchemaGenerators.defaultSchemaSpecGen("harry", "table0")
                                   .inflate(seed);
        }
    }

    // TODO: schema provider by DDL
}
