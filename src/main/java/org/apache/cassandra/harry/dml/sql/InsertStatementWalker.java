package org.apache.cassandra.harry.dml.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;

/**
 * Deterministic random generator for INSERT statements. Given a schema and a
 * seed, produces a syntactically complete InsertStatement by deciding which
 * columns to include and what expression type to use for each value.
 * All branching weights are configurable via {@link Weights}.
 */
public class InsertStatementWalker
{
    private final TableSpec spec;
    private final Weights weights;

    public InsertStatementWalker(TableSpec spec, Weights weights)
    {
        this.spec = spec;
        this.weights = weights;
    }

    public InsertStatementWalker(TableSpec spec, double forkProbability)
    {
        this(spec, Weights.withForkProbability(forkProbability));
    }

    public InsertStatement generate(long seed)
    {
        EntropySource rng = new JdkRandomEntropySource(seed);
        List<InsertValue> values = new ArrayList<>();

        // Always include PK columns
        if (weights.alwaysIncludePk)
        {
            for (TableSpec.Column col : spec.primaryKey())
                values.add(walkInsertValue(rng, col, true));
        }

        // Fork to add regular columns
        List<TableSpec.Column> available = new ArrayList<>(spec.regularColumns());
        if (!available.isEmpty())
        {
            // Always at least one regular column
            values.add(walkInsertValue(rng, pickAndRemove(rng, available), false));
            while (!available.isEmpty() && fork(rng))
                values.add(walkInsertValue(rng, pickAndRemove(rng, available), false));
        }

        int nPk = spec.primaryKey().size();
        int[] pkIndexes = new int[nPk];
        for (int i = 0; i < nPk; i++)
        {
            pkIndexes[i] = switch (values.get(i).value()) {
                case Expression.Param p -> (int) p.valueIndex().getValue();
                case Expression.InlineParam ip -> (int) ip.valueIndex().getValue();
                default -> throw new IllegalStateException("PK column must not be null");
            };
        }

        return new InsertStatement(spec, values.toArray(new InsertValue[0]), new PkTuple(pkIndexes));
    }

    private InsertValue walkInsertValue(EntropySource rng, TableSpec.Column col, boolean isPk)
    {
        return InsertValue.val(col, walkValExpression(rng, col, isPk));
    }

    private Expression walkValExpression(EntropySource rng, TableSpec.Column col, boolean isPk)
    {
        int nullWeight = isPk ? 0 : weights.valNull;
        int total = weights.valParam + weights.valInlineParam + nullWeight;

        // If all non-null weights are zero for a PK column, fall back to Param
        if (total == 0)
            return Expression.param(col, randomValueIndex(rng, col));

        int choice = rng.nextInt(total);

        if (choice < weights.valParam)
            return Expression.param(col, randomValueIndex(rng, col));
        else if (choice < weights.valParam + weights.valInlineParam)
            return Expression.inlineParam(col, randomValueIndex(rng, col));
        else
            return Expression.nullLiteral();
    }

    private boolean fork(EntropySource rng)
    {
        return rng.nextDouble() < weights.forkProbability;
    }

    private TableSpec.Column pickAndRemove(EntropySource rng, List<TableSpec.Column> columns)
    {
        int idx = rng.nextInt(columns.size());
        return columns.remove(idx);
    }

    private ValueIndex randomValueIndex(EntropySource rng, TableSpec.Column col)
    {
        long pop = col.population();
        return ValueIndex.value(rng.nextInt(0, (int) Math.min(pop, Integer.MAX_VALUE)));
    }

    public static class Weights
    {
        public final double forkProbability;
        public final boolean alwaysIncludePk;
        public final int valParam;
        public final int valInlineParam;
        public final int valNull;

        public Weights(double forkProbability, boolean alwaysIncludePk,
                       int valParam, int valInlineParam, int valNull)
        {
            if (forkProbability < 0.0 || forkProbability > 1.0)
                throw new IllegalArgumentException("forkProbability must be in [0.0, 1.0]");
            this.forkProbability = forkProbability;
            this.alwaysIncludePk = alwaysIncludePk;
            this.valParam = valParam;
            this.valInlineParam = valInlineParam;
            this.valNull = valNull;
        }

        public static Weights defaults()
        {
            return new Weights(0.3, true, 70, 10, 20);
        }

        public static Weights withForkProbability(double forkProbability)
        {
            return new Weights(forkProbability, true, 70, 10, 20);
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static class Builder
        {
            private double forkProbability = 0.3;
            private boolean alwaysIncludePk = true;
            private int valParam = 70;
            private int valInlineParam = 10;
            private int valNull = 20;

            public Builder forkProbability(double v) { forkProbability = v; return this; }
            public Builder alwaysIncludePk(boolean v) { alwaysIncludePk = v; return this; }
            public Builder valParam(int v) { valParam = v; return this; }
            public Builder valInlineParam(int v) { valInlineParam = v; return this; }
            public Builder valNull(int v) { valNull = v; return this; }

            public Weights build()
            {
                return new Weights(forkProbability, alwaysIncludePk, valParam, valInlineParam, valNull);
            }
        }
    }
}
