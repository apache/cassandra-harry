package org.apache.cassandra.harry.dml.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.harry.DataType;
import org.apache.cassandra.harry.gen.Bijections.IndexedBijection;
import org.apache.cassandra.harry.gen.Generators;
import org.apache.cassandra.harry.gen.InvertibleGenerator;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;

public class TableSpec
{
    private final String schema;
    private final String table;
    private final List<Column> columns;
    private final int nextColumnOrdinal;
    private final long seed;
    private final int defaultPopulation;

    private TableSpec(String schema, String table, List<Column> columns,
                      int nextColumnOrdinal, long seed, int defaultPopulation)
    {
        this.schema = schema;
        this.table = table;
        this.columns = Collections.unmodifiableList(columns);
        this.nextColumnOrdinal = nextColumnOrdinal;
        this.seed = seed;
        this.defaultPopulation = defaultPopulation;
    }

    public String schema()
    {
        return schema;
    }

    public String table()
    {
        return table;
    }

    public List<Column> columns()
    {
        return columns;
    }

    public Column column(String name)
    {
        for (Column c : columns)
        {
            if (c.name.equals(name))
                return c;
        }
        throw new IllegalArgumentException("No column named: " + name);
    }

    public List<Column> primaryKey()
    {
        return columns.stream().filter(c -> c.isPk).collect(Collectors.toList());
    }

    public List<Column> regularColumns()
    {
        return columns.stream().filter(c -> !c.isPk).collect(Collectors.toList());
    }

    public String compileCreateTable()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ");
        if (schema != null)
            sb.append(schema).append('.');
        sb.append(table).append(" (\n");

        for (int i = 0; i < columns.size(); i++)
        {
            Column c = columns.get(i);
            sb.append("    ").append(c.name).append(' ').append(c.type.sqlName());
            if (c.isPk)
                sb.append(" NOT NULL");
            sb.append(",\n");
        }

        List<Column> pks = primaryKey();
        sb.append("    PRIMARY KEY (");
        for (int i = 0; i < pks.size(); i++)
        {
            if (i > 0) sb.append(", ");
            sb.append(pks.get(i).name);
        }
        sb.append(")\n)");

        return sb.toString();
    }

    public String compileDropTable()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP TABLE IF EXISTS ");
        if (schema != null)
            sb.append(schema).append('.');
        sb.append(table);
        return sb.toString();
    }

    public static Builder builder(String schema, String table)
    {
        return new Builder(schema, table, 0);
    }

    public static ColumnOption pk()
    {
        return ColumnOption.pk();
    }

    public static ColumnOption population(int n)
    {
        return ColumnOption.population(n);
    }

    public EvolutionBuilder evolve()
    {
        return new EvolutionBuilder(this);
    }

    // -- Column --

    public static class Column
    {
        public final int ordinal;
        public final String name;
        public final DataType<?> type;
        public final boolean isPk;
        public final IndexedBijection<Object> bijection;

        @SuppressWarnings({"unchecked", "rawtypes"})
        Column(int ordinal, String name, DataType<?> type, boolean isPk,
               int population, long seed)
        {
            this.ordinal = ordinal;
            this.name = name;
            this.type = type;
            this.isPk = isPk;
            this.bijection = makeBijection((DataType) type, population, seed);
        }

        @SuppressWarnings("unchecked")
        private static <T> IndexedBijection<Object> makeBijection(DataType<T> type, int population, long seed)
        {
            return (IndexedBijection<Object>) (IndexedBijection) new InvertibleGenerator<>(
                new JdkRandomEntropySource(seed), type.typeEntropy(), population,
                Generators.defaultFor(type), type.comparator());
        }

        // Constructor for evolution: reuse existing column as-is
        Column(int ordinal, String name, DataType<?> type, boolean isPk,
               IndexedBijection<Object> bijection)
        {
            this.ordinal = ordinal;
            this.name = name;
            this.type = type;
            this.isPk = isPk;
            this.bijection = bijection;
        }

        public Object inflate(ValueIndex idx)
        {
            return bijection.inflate(bijection.descriptorAt(idx.getValue()));
        }

        public ValueIndex deflate(Object value)
        {
            long descriptor = bijection.deflate(value);
            long idx = bijection.idxFor(descriptor);
            return ValueIndex.value(idx);
        }

        public long population()
        {
            return bijection.population();
        }
    }

    // -- Column options --

    public interface ColumnOption
    {
        ColumnOption PK = new ColumnOption() {};

        static ColumnOption pk()
        {
            return PK;
        }

        static ColumnOption population(int n)
        {
            return new PopulationOption(n);
        }
    }

    static class PopulationOption implements ColumnOption
    {
        final int population;

        PopulationOption(int population)
        {
            this.population = population;
        }
    }

    // -- Builder --

    public static class Builder
    {
        private final String schema;
        private final String table;
        private final Map<String, ColumnDef> columns = new LinkedHashMap<>();
        private int nextOrdinal;
        private long seed = 0L;
        private int defaultPopulation = 128;

        Builder(String schema, String table, int startOrdinal)
        {
            this.schema = schema;
            this.table = table;
            this.nextOrdinal = startOrdinal;
        }

        public Builder seed(long seed)
        {
            this.seed = seed;
            return this;
        }

        public Builder defaultPopulation(int defaultPopulation)
        {
            this.defaultPopulation = defaultPopulation;
            return this;
        }

        public Builder column(String name, DataType<?> type, ColumnOption... options)
        {
            if (columns.containsKey(name))
                throw new IllegalArgumentException("Duplicate column name: " + name);

            boolean isPk = false;
            int population = 0;
            for (ColumnOption opt : options)
            {
                if (opt == ColumnOption.PK)
                    isPk = true;
                else if (opt instanceof PopulationOption)
                    population = ((PopulationOption) opt).population;
            }

            columns.put(name, new ColumnDef(nextOrdinal++, name, type, isPk, population));
            return this;
        }

        public TableSpec build()
        {
            List<Column> cols = new ArrayList<>();
            boolean hasPk = false;
            for (ColumnDef def : columns.values())
            {
                int effectivePopulation = def.population > 0 ? def.population : defaultPopulation;
                cols.add(new Column(def.ordinal, def.name, def.type, def.isPk,
                                    effectivePopulation, seed));
                if (def.isPk) hasPk = true;
            }
            if (!hasPk)
                throw new IllegalStateException("TableSpec requires at least one primary key column");

            return new TableSpec(schema, table, cols, nextOrdinal, seed, defaultPopulation);
        }

        private record ColumnDef(int ordinal, String name, DataType<?> type, boolean isPk, int population) {}
    }

    // -- Evolution builder --

    public static class EvolutionBuilder
    {
        private final String schema;
        private final String table;
        private final Map<String, Column> columns = new LinkedHashMap<>();
        private int nextOrdinal;
        private final long seed;
        private final int defaultPopulation;

        EvolutionBuilder(TableSpec source)
        {
            this.schema = source.schema;
            this.table = source.table;
            this.nextOrdinal = source.nextColumnOrdinal;
            this.seed = source.seed;
            this.defaultPopulation = source.defaultPopulation;
            for (Column c : source.columns)
                columns.put(c.name, c);
        }

        public EvolutionBuilder addColumn(String name, DataType<?> type, ColumnOption... options)
        {
            if (columns.containsKey(name))
                throw new IllegalArgumentException("Duplicate column name: " + name);

            boolean isPk = false;
            int population = 0;
            for (ColumnOption opt : options)
            {
                if (opt == ColumnOption.PK)
                    isPk = true;
                else if (opt instanceof PopulationOption)
                    population = ((PopulationOption) opt).population;
            }

            int effectivePopulation = population > 0 ? population : defaultPopulation;
            columns.put(name, new Column(nextOrdinal++, name, type, isPk,
                                         effectivePopulation, seed));
            return this;
        }

        public EvolutionBuilder dropColumn(String name)
        {
            if (!columns.containsKey(name))
                throw new IllegalArgumentException("No column named: " + name);
            columns.remove(name);
            return this;
        }

        public TableSpec build()
        {
            List<Column> cols = new ArrayList<>(columns.values());
            boolean hasPk = cols.stream().anyMatch(c -> c.isPk);
            if (!hasPk)
                throw new IllegalStateException("TableSpec requires at least one primary key column");

            return new TableSpec(schema, table, cols, nextOrdinal, seed, defaultPopulation);
        }
    }
}
