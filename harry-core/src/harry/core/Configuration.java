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
import harry.model.AlwaysSamePartitionSelector;
import harry.model.Model;
import harry.model.OpSelectors;
import harry.model.QuiescentChecker;
import harry.model.clock.ApproximateMonotonicClock;
import harry.model.clock.OffsetClock;
import harry.model.sut.PrintlnSut;
import harry.model.sut.SystemUnderTest;
import harry.runner.AllPartitionsValidator;
import harry.runner.CorruptingPartitionVisitor;
import harry.runner.DataTracker;
import harry.runner.DefaultDataTracker;
import harry.runner.LoggingPartitionVisitor;
import harry.runner.MutatingPartitionVisitor;
import harry.runner.MutatingRowVisitor;
import harry.runner.Operation;
import harry.runner.ParallelRecentPartitionValidator;
import harry.runner.PartitionVisitor;
import harry.runner.RecentPartitionValidator;
import harry.runner.Runner;
import harry.runner.Sampler;
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
        mapper.registerSubtypes(Configuration.ApproximateMonotonicClockConfiguration.class);
        mapper.registerSubtypes(Configuration.ConcurrentRunnerConfig.class);
        mapper.registerSubtypes(Configuration.SequentialRunnerConfig.class);
        mapper.registerSubtypes(Configuration.DefaultDataTrackerConfiguration.class);
        mapper.registerSubtypes(Configuration.NoOpDataTrackerConfiguration.class);

        mapper.registerSubtypes(Configuration.QuiescentCheckerConfig.class);
        mapper.registerSubtypes(NoOpCheckerConfig.class);
        mapper.registerSubtypes(Configuration.DefaultCDSelectorConfiguration.class);
        mapper.registerSubtypes(Configuration.DefaultPDSelectorConfiguration.class);
        mapper.registerSubtypes(Configuration.ConstantDistributionConfig.class);
        mapper.registerSubtypes(DefaultSchemaProviderConfiguration.class);
        mapper.registerSubtypes(MutatingRowVisitorConfiguration.class);

        mapper.registerSubtypes(MutatingPartitionVisitorConfiguation.class);
        mapper.registerSubtypes(LoggingPartitionVisitorConfiguration.class);
        mapper.registerSubtypes(AllPartitionsValidatorConfiguration.class);
        mapper.registerSubtypes(ParallelRecentPartitionValidator.ParallelRecentPartitionValidatorConfig.class);
        mapper.registerSubtypes(Sampler.SamplerConfiguration.class);
        mapper.registerSubtypes(CorruptingPartitionVisitorConfiguration.class);
        mapper.registerSubtypes(RecentPartitionsValidatorConfiguration.class);
        mapper.registerSubtypes(FixedSchemaProviderConfiguration.class);
        mapper.registerSubtypes(AlwaysSamePartitionSelector.AlwaysSamePartitionSelectorConfiguration.class);
        mapper.registerSubtypes(OffsetClock.OffsetClockConfiguration.class);
        mapper.registerSubtypes(PrintlnSut.PrintlnSutConfiguration.class);
        mapper.registerSubtypes(NoOpDataTrackerConfiguration.class);
        mapper.registerSubtypes(NoOpMetricReporterConfiguration.class);
    }

    public final long seed;
    public final SchemaProviderConfiguration schema_provider;

    public final boolean drop_schema;
    public final boolean create_schema;
    public final boolean truncate_table;

    public final MetricReporterConfiguration metric_reporter;
    public final ClockConfiguration clock;
    public final SutConfiguration system_under_test;
    public final DataTrackerConfiguration data_tracker;
    public final RunnerConfiguration runner;
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
                         @JsonProperty("metric_reporter") MetricReporterConfiguration metric_reporter,
                         @JsonProperty("clock") ClockConfiguration clock,
                         @JsonProperty("runner") RunnerConfiguration runner,
                         @JsonProperty("system_under_test") SutConfiguration system_under_test,
                         @JsonProperty("data_tracker") DataTrackerConfiguration data_tracker,
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
        this.metric_reporter = metric_reporter;
        this.clock = clock;
        this.system_under_test = system_under_test;
        this.data_tracker = data_tracker;
        this.partition_descriptor_selector = partition_descriptor_selector;
        this.clustering_descriptor_selector = clustering_descriptor_selector;
        this.run_time = run_time;
        this.run_time_unit = run_time_unit;
        this.runner = runner;
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
        return fromFile(new File(path));
    }

    public static Configuration fromFile(File file)
    {
        try
        {
            return mapper.readValue(file, Configuration.class);
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    public static void validate(Configuration config)
    {
        Objects.requireNonNull(config.schema_provider, "Schema provider should not be null");
        Objects.requireNonNull(config.metric_reporter, "Metric reporter should not be null");
        Objects.requireNonNull(config.clock, "Clock should not be null");
        Objects.requireNonNull(config.system_under_test, "System under test should not be null");
        Objects.requireNonNull(config.partition_descriptor_selector, "Partition descriptor selector should not be null");
        Objects.requireNonNull(config.clustering_descriptor_selector, "Clustering descriptor selector should not be null");

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
        validate(snapshot);

        long seed = snapshot.seed;

        DataTracker tracker = snapshot.data_tracker == null ? new DefaultDataTrackerConfiguration().make() : snapshot.data_tracker.make();
        OpSelectors.Rng rng = new OpSelectors.PCGFast(seed);

        OpSelectors.MonotonicClock clock = snapshot.clock.make();

        MetricReporter metricReporter = snapshot.metric_reporter.make();
        // TODO: parse schema
        SchemaSpec schemaSpec = snapshot.schema_provider.make(seed);
        schemaSpec.validate();

        OpSelectors.PdSelector pdSelector = snapshot.partition_descriptor_selector.make(rng);
        OpSelectors.DescriptorSelector descriptorSelector = snapshot.clustering_descriptor_selector.make(rng, schemaSpec);
        // TODO: validate that operation kind is compactible with schema, due to statics etc
        SystemUnderTest sut = snapshot.system_under_test.make();

        return new Run(rng,
                       clock,
                       pdSelector,
                       descriptorSelector,
                       schemaSpec,
                       tracker,
                       sut,
                       metricReporter);
    }

    public static Runner createRunner(Configuration config)
    {
        Run run = createRun(config);
        return config.runner.make(run, config);
    }

    public static class ConfigurationBuilder
    {
        long seed;
        SchemaProviderConfiguration schema_provider = new DefaultSchemaProviderConfiguration();

        boolean drop_schema;
        boolean create_schema;
        boolean truncate_table;

        ClockConfiguration clock;
        MetricReporterConfiguration metric_reporter = new NoOpMetricReporterConfiguration();
        DataTrackerConfiguration data_tracker = new DefaultDataTrackerConfiguration();
        RunnerConfiguration runner;
        SutConfiguration system_under_test;
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


        public ConfigurationBuilder setDataTracker(DataTrackerConfiguration tracker)
        {
            this.data_tracker = tracker;
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

        public ConfigurationBuilder setMetricReporter(MetricReporterConfiguration metric_reporter)
        {
            this.metric_reporter = metric_reporter;
            return this;
        }

        public Configuration build()
        {
            return new Configuration(seed,
                                     schema_provider,
                                     drop_schema,
                                     create_schema,
                                     truncate_table,
                                     metric_reporter,
                                     clock,

                                     runner,
                                     system_under_test,
                                     data_tracker,

                                     partition_descriptor_selector,
                                     clustering_descriptor_selector,

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

        builder.partition_descriptor_selector = partition_descriptor_selector;
        builder.clustering_descriptor_selector = clustering_descriptor_selector;

        builder.run_time = run_time;
        builder.run_time_unit = run_time_unit;
        return builder;
    }


    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface DataTrackerConfiguration extends DataTracker.DataTrackerFactory
    {

    }

    @JsonTypeName("no_op")
    public static class NoOpDataTrackerConfiguration implements DataTrackerConfiguration
    {
        @JsonCreator
        public NoOpDataTrackerConfiguration()
        {
        }

        public DataTracker make()
        {
            return new DataTracker()
            {
                public void started(long lts)
                {
                }

                public void finished(long lts)
                {

                }

                public long maxStarted()
                {
                    return 0;
                }

                public long maxConsecutiveFinished()
                {
                    return 0;
                }

                public DataTrackerConfiguration toConfig()
                {
                    return null;
                }
            };
        }
    }


    @JsonTypeName("default")
    public static class DefaultDataTrackerConfiguration implements DataTrackerConfiguration
    {
        public final long max_seen_lts;
        public final long max_complete_lts;

        public DefaultDataTrackerConfiguration()
        {
            this(-1, -1);
        }

        @JsonCreator
        public DefaultDataTrackerConfiguration(@JsonProperty(value = "max_seen_lts", defaultValue = "-1") long max_seen_lts,
                                               @JsonProperty(value = "max_complete_lts", defaultValue = "-1") long max_complete_lts)
        {
            this.max_seen_lts = max_seen_lts;
            this.max_complete_lts = max_complete_lts;
        }

        public DataTracker make()
        {
            DefaultDataTracker defaultDataTracker = new DefaultDataTracker();
            defaultDataTracker.forceLts(max_seen_lts, max_complete_lts);
            return defaultDataTracker;
        }
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
        public final long start_time_micros;
        public final int history_size;
        public final long[] history;
        public final long lts;
        public final int idx;
        public final long epoch_period;
        public final TimeUnit epoch_time_unit;

        @JsonCreator
        public DebugApproximateMonotonicClockConfiguration(@JsonProperty("start_time_micros") long start_time_micros,
                                                           @JsonProperty("history_size") int history_size,
                                                           @JsonProperty("history") long[] history,
                                                           @JsonProperty("lts") long lts,
                                                           @JsonProperty("idx") int idx,
                                                           @JsonProperty("epoch_period") long epoch_period,
                                                           @JsonProperty("epoch_time_unit") TimeUnit epoch_time_unit)
        {
            this.start_time_micros = start_time_micros;
            this.history_size = history_size;
            this.history = history;
            this.lts = lts;
            this.idx = idx;
            this.epoch_period = epoch_period;
            this.epoch_time_unit = epoch_time_unit;
        }

        public OpSelectors.MonotonicClock make()
        {
            return ApproximateMonotonicClock.forDebug(start_time_micros,
                                                      history_size,
                                                      lts,
                                                      idx,
                                                      epoch_period,
                                                      epoch_time_unit,
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
        public final int concurrency;
        public final List<PartitionVisitorConfiguration> partition_visitor_factories;

        @JsonCreator
        public ConcurrentRunnerConfig(@JsonProperty(value = "concurrency", defaultValue = "2") int concurrency,
                                      @JsonProperty(value = "partition_visitors") List<PartitionVisitorConfiguration> partitionVisitors)
        {
            this.concurrency = concurrency;
            this.partition_visitor_factories = partitionVisitors;
        }

        @Override
        public Runner make(Run run, Configuration config)
        {
            return new Runner.ConcurrentRunner(run, config, concurrency, partition_visitor_factories);
        }
    }

    @JsonTypeName("sequential")
    public static class SequentialRunnerConfig implements RunnerConfiguration
    {
        public final List<PartitionVisitorConfiguration> partition_visitor_factories;

        @JsonCreator
        public SequentialRunnerConfig(@JsonProperty(value = "partition_visitors") List<PartitionVisitorConfiguration> partitionVisitors)
        {
            this.partition_visitor_factories = partitionVisitors;
        }

        @Override
        public Runner make(Run run, Configuration config)
        {
            return new Runner.SequentialRunner(run, config, partition_visitor_factories);
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

    @JsonTypeName("quiescent_checker")
    public static class QuiescentCheckerConfig implements ModelConfiguration
    {
        @JsonCreator
        public QuiescentCheckerConfig()
        {
        }

        public Model make(Run run)
        {
            return new QuiescentChecker(run);
        }
    }

    @JsonTypeName("no_op")
    public static class NoOpCheckerConfig implements ModelConfiguration
    {
        @JsonCreator
        public NoOpCheckerConfig()
        {
        }

        public Model make(Run run)
        {
            return new harry.model.NoOpChecker(run);
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
                                                                                 .addWeight(OpSelectors.OperationKind.INSERT, 98)
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

        public CDSelectorConfigurationBuilder setFractions(int[] fractions)
        {
            this.fractions = fractions;
            return this;
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

        protected OpSelectors.ColumnSelector columnSelector(SchemaSpec schemaSpec)
        {
            OpSelectors.ColumnSelector columnSelector;
            if (column_mask_bitsets == null)
            {
                columnSelector = OpSelectors.columnSelectorBuilder().forAll(schemaSpec).build();
            }
            else
            {
                Map<OpSelectors.OperationKind, Surjections.Surjection<BitSet>> m = new HashMap<>();
                for (Map.Entry<OpSelectors.OperationKind, long[]> entry : column_mask_bitsets.entrySet())
                {
                    List<BitSet> bitSets = new ArrayList<>(entry.getValue().length);
                    for (long raw_bitset : entry.getValue())
                    {
                        bitSets.add(BitSet.create(raw_bitset, schemaSpec.allColumns.size()));
                    }
                    Surjections.Surjection<BitSet> selector = Surjections.pick(bitSets);
                    m.put(entry.getKey(), selector);
                }
                columnSelector = (opKind, descr) -> m.get(opKind).inflate(descr);
            }

            return columnSelector;
        }

        public OpSelectors.DescriptorSelector make(OpSelectors.Rng rng, SchemaSpec schemaSpec)
        {
            return new OpSelectors.DefaultDescriptorSelector(rng,
                                                             columnSelector(schemaSpec),
                                                             OpSelectors.OperationSelector.weighted(operation_kind_weights),
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
                                                                  OpSelectors.OperationSelector.weighted(operation_kind_weights),
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
    public interface PartitionVisitorConfiguration extends PartitionVisitor.PartitionVisitorFactory
    {
    }


    @JsonTypeName("mutating")
    public static class MutatingPartitionVisitorConfiguation implements PartitionVisitorConfiguration
    {
        public final RowVisitorConfiguration row_visitor;

        @JsonCreator
        public MutatingPartitionVisitorConfiguation(@JsonProperty("row_visitor") RowVisitorConfiguration row_visitor)
        {
            this.row_visitor = row_visitor;
        }

        @Override
        public PartitionVisitor make(Run run)
        {
            return new MutatingPartitionVisitor(run, row_visitor);
        }
    }

    @JsonTypeName("logging")
    public static class LoggingPartitionVisitorConfiguration implements PartitionVisitorConfiguration
    {
        protected final RowVisitorConfiguration row_visitor;

        @JsonCreator
        public LoggingPartitionVisitorConfiguration(@JsonProperty("row_visitor") RowVisitorConfiguration row_visitor)
        {
            this.row_visitor = row_visitor;
        }

        @Override
        public PartitionVisitor make(Run run)
        {
            return new LoggingPartitionVisitor(run, row_visitor);
        }
    }

    @JsonTypeName("validate_all_partitions")
    public static class AllPartitionsValidatorConfiguration implements Configuration.PartitionVisitorConfiguration
    {
        public final int concurrency;
        public final int trigger_after;
        public final Configuration.ModelConfiguration modelConfiguration;

        @JsonCreator
        public AllPartitionsValidatorConfiguration(@JsonProperty("concurrency") int concurrency,
                                                   @JsonProperty("trigger_after") int trigger_after,
                                                   @JsonProperty("model") Configuration.ModelConfiguration model)
        {
            this.concurrency = concurrency;
            this.trigger_after = trigger_after;
            this.modelConfiguration = model;
        }

        public PartitionVisitor make(Run run)
        {
            return new AllPartitionsValidator(concurrency, trigger_after, run, modelConfiguration);
        }
    }

    @JsonTypeName("corrupt")
    public static class CorruptingPartitionVisitorConfiguration implements Configuration.PartitionVisitorConfiguration
    {
        public final int trigger_after;

        @JsonCreator
        public CorruptingPartitionVisitorConfiguration(@JsonProperty("trigger_after") int trigger_after)
        {
            this.trigger_after = trigger_after;
        }

        public PartitionVisitor make(Run run)
        {
            return new CorruptingPartitionVisitor(trigger_after, run);
        }
    }

    @JsonTypeName("validate_recent_partitions")
    public static class RecentPartitionsValidatorConfiguration implements Configuration.PartitionVisitorConfiguration
    {
        public final int partition_count;
        public final int trigger_after;
        public final int queries;
        public final Configuration.ModelConfiguration modelConfiguration;

        // TODO: make query selector configurable
        @JsonCreator
        public RecentPartitionsValidatorConfiguration(@JsonProperty("partition_count") int partition_count,
                                                      @JsonProperty("trigger_after") int trigger_after,
                                                      @JsonProperty("queries_per_partition") int queries,
                                                      @JsonProperty("model") Configuration.ModelConfiguration model)
        {
            this.partition_count = partition_count;
            this.queries = queries;
            this.trigger_after = trigger_after;
            this.modelConfiguration = model;
        }

        @Override
        public PartitionVisitor make(Run run)
        {
            return new RecentPartitionValidator(partition_count, queries, trigger_after, run, modelConfiguration);
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface RowVisitorConfiguration extends Operation.RowVisitorFactory
    {
    }

    @JsonTypeName("mutating")
    public static class MutatingRowVisitorConfiguration implements RowVisitorConfiguration
    {
        @Override
        public Operation make(Run run)
        {
            return new MutatingRowVisitor(run);
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

    @JsonTypeName("fixed")
    public static class FixedSchemaProviderConfiguration implements SchemaProviderConfiguration
    {
        public final String keyspace;
        public final String table;
        public final Map<String, String> partition_keys;
        public final Map<String, String> clustering_keys;
        public final Map<String, String> regular_columns;
        public final Map<String, String> static_keys;
        private final SchemaSpec schemaSpec;

        @JsonCreator
        public FixedSchemaProviderConfiguration(@JsonProperty("keyspace") String keyspace,
                                                @JsonProperty("table") String table,
                                                @JsonProperty("partition_keys") Map<String, String> pks,
                                                @JsonProperty("clustering_keys") Map<String, String> cks,
                                                @JsonProperty("regular_columns") Map<String, String> regulars,
                                                @JsonProperty("static_columns") Map<String, String> statics)
        {
            this(SchemaGenerators.parse(keyspace, table,
                                        pks, cks, regulars, statics),
                 pks,
                 cks,
                 regulars,
                 statics);
        }

        public FixedSchemaProviderConfiguration(SchemaSpec schemaSpec,
                                                Map<String, String> pks,
                                                Map<String, String> cks,
                                                Map<String, String> regulars,
                                                Map<String, String> statics)
        {
            this.schemaSpec = schemaSpec;
            this.keyspace = schemaSpec.keyspace;
            this.table = schemaSpec.table;
            this.partition_keys = pks;
            this.clustering_keys = cks;
            this.regular_columns = regulars;
            this.static_keys = statics;
        }
        public SchemaSpec make(long seed)
        {
            return schemaSpec;
        }
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
    public interface MetricReporterConfiguration extends MetricReporter.MetricReporterFactory
    {
    }

    @JsonTypeName("no_op")
    public static class NoOpMetricReporterConfiguration implements MetricReporterConfiguration
    {
        public MetricReporter make()
        {
            return MetricReporter.NO_OP;
        }
    }

    // TODO: schema provider by DDL
}
