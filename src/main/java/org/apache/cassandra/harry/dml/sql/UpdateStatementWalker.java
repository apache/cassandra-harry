package org.apache.cassandra.harry.dml.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;

/**
 * Deterministic random generator for UPDATE statements. Given a schema and a
 * seed, produces a syntactically complete UpdateStatement by walking the full
 * UPDATE grammar. All branching weights are configurable via {@link Weights}.
 *
 * Predicate generation is delegated to {@link PredicateWalker}.
 */
public class UpdateStatementWalker
{
    private final TableSpec spec;
    private final Weights weights;
    private final PredicateWalker predicateWalker;

    public UpdateStatementWalker(TableSpec spec, Weights weights)
    {
        this.spec = spec;
        this.weights = weights;
        this.predicateWalker = new PredicateWalker(spec, weights.toPredicateWeights());
    }

    public UpdateStatementWalker(TableSpec spec, double forkProbability)
    {
        this(spec, Weights.withForkProbability(forkProbability));
    }

    public UpdateStatement generate(long seed)
    {
        EntropySource rng = new JdkRandomEntropySource(seed);
        Assignment[] assignments = walkAssignments(rng);
        Predicate where = predicateWalker.walkPredicate(rng, 0);
        return new UpdateStatement(spec, assignments, where);
    }

    // -- Assignments --

    private Assignment[] walkAssignments(EntropySource rng)
    {
        List<TableSpec.Column> regular = spec.regularColumns();
        if (regular.isEmpty())
        {
            TableSpec.Column col = pickColumn(rng, spec.columns());
            return new Assignment[]{ walkAssignment(rng, col) };
        }

        List<Assignment> result = new ArrayList<>();
        List<TableSpec.Column> available = new ArrayList<>(regular);
        result.add(walkAssignment(rng, pickAndRemove(rng, available)));

        while (!available.isEmpty() && fork(rng))
            result.add(walkAssignment(rng, pickAndRemove(rng, available)));

        return result.toArray(new Assignment[0]);
    }

    private Assignment walkAssignment(EntropySource rng, TableSpec.Column col)
    {
        return Assignment.set(col, walkSetExpression(rng, col));
    }

    // -- Expressions (SET context) --

    private Expression walkSetExpression(EntropySource rng, TableSpec.Column col)
    {
        int total = weights.setParam + weights.setInlineParam + weights.setNull;
        int choice = rng.nextInt(total);
        if (choice < weights.setParam)
            return Expression.param(col, randomValueIndex(rng, col));
        else if (choice < weights.setParam + weights.setInlineParam)
            return Expression.inlineParam(col, randomValueIndex(rng, col));
        else
            return Expression.nullLiteral();
    }

    // -- Utilities --

    private boolean fork(EntropySource rng)
    {
        return rng.nextDouble() < weights.forkProbability;
    }

    private TableSpec.Column pickColumn(EntropySource rng, List<TableSpec.Column> columns)
    {
        return columns.get(rng.nextInt(columns.size()));
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

    // -- Weights --

    public static class Weights
    {
        public final double forkProbability;

        // SET expression weights
        public final int setParam;
        public final int setInlineParam;
        public final int setNull;

        // Predicate weights (delegated to PredicateWalker)
        public final int whereParam;
        public final int whereInlineParam;
        public final int allPredicatePercent;
        public final int maxPredicateDepth;
        public final int leafComparison;
        public final int leafIsNull;
        public final int leafIsNotNull;
        public final int leafFuture;
        public final int compoundAnd;
        public final int compoundOr;
        public final int compoundNot;

        public Weights(double forkProbability,
                       int setParam, int setInlineParam, int setNull,
                       int whereParam, int whereInlineParam,
                       int allPredicatePercent, int maxPredicateDepth,
                       int leafComparison, int leafIsNull, int leafIsNotNull, int leafFuture,
                       int compoundAnd, int compoundOr, int compoundNot)
        {
            if (forkProbability < 0.0 || forkProbability > 1.0)
                throw new IllegalArgumentException("forkProbability must be in [0.0, 1.0]");
            if (allPredicatePercent < 0 || allPredicatePercent > 100)
                throw new IllegalArgumentException("allPredicatePercent must be in [0, 100]");
            if (maxPredicateDepth < 1)
                throw new IllegalArgumentException("maxPredicateDepth must be >= 1");

            this.forkProbability = forkProbability;
            this.setParam = setParam;
            this.setInlineParam = setInlineParam;
            this.setNull = setNull;
            this.whereParam = whereParam;
            this.whereInlineParam = whereInlineParam;
            this.allPredicatePercent = allPredicatePercent;
            this.maxPredicateDepth = maxPredicateDepth;
            this.leafComparison = leafComparison;
            this.leafIsNull = leafIsNull;
            this.leafIsNotNull = leafIsNotNull;
            this.leafFuture = leafFuture;
            this.compoundAnd = compoundAnd;
            this.compoundOr = compoundOr;
            this.compoundNot = compoundNot;
        }

        public PredicateWalker.Weights toPredicateWeights()
        {
            return new PredicateWalker.Weights(
                forkProbability, whereParam, whereInlineParam,
                allPredicatePercent, maxPredicateDepth,
                leafComparison, leafIsNull, leafIsNotNull, leafFuture,
                compoundAnd, compoundOr, compoundNot
            );
        }

        public static Weights defaults()
        {
            return new Weights(0.3, 70, 10, 20, 80, 20, 5, 4, 60, 15, 15, 10, 45, 35, 20);
        }

        public static Weights withForkProbability(double forkProbability)
        {
            Weights d = defaults();
            return new Weights(
                forkProbability,
                d.setParam, d.setInlineParam, d.setNull,
                d.whereParam, d.whereInlineParam,
                d.allPredicatePercent, d.maxPredicateDepth,
                d.leafComparison, d.leafIsNull, d.leafIsNotNull, d.leafFuture,
                d.compoundAnd, d.compoundOr, d.compoundNot
            );
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static class Builder
        {
            private double forkProbability = 0.3;
            private int setParam = 70;
            private int setInlineParam = 10;
            private int setNull = 20;
            private int whereParam = 80;
            private int whereInlineParam = 20;
            private int allPredicatePercent = 5;
            private int maxPredicateDepth = 4;
            private int leafComparison = 60;
            private int leafIsNull = 15;
            private int leafIsNotNull = 15;
            private int leafFuture = 10;
            private int compoundAnd = 45;
            private int compoundOr = 35;
            private int compoundNot = 20;

            public Builder forkProbability(double v) { forkProbability = v; return this; }
            public Builder setParam(int v) { setParam = v; return this; }
            public Builder setInlineParam(int v) { setInlineParam = v; return this; }
            public Builder setNull(int v) { setNull = v; return this; }
            public Builder whereParam(int v) { whereParam = v; return this; }
            public Builder whereInlineParam(int v) { whereInlineParam = v; return this; }
            public Builder allPredicatePercent(int v) { allPredicatePercent = v; return this; }
            public Builder maxPredicateDepth(int v) { maxPredicateDepth = v; return this; }
            public Builder leafComparison(int v) { leafComparison = v; return this; }
            public Builder leafIsNull(int v) { leafIsNull = v; return this; }
            public Builder leafIsNotNull(int v) { leafIsNotNull = v; return this; }
            public Builder leafFuture(int v) { leafFuture = v; return this; }
            public Builder compoundAnd(int v) { compoundAnd = v; return this; }
            public Builder compoundOr(int v) { compoundOr = v; return this; }
            public Builder compoundNot(int v) { compoundNot = v; return this; }

            public Weights build()
            {
                return new Weights(forkProbability, setParam, setInlineParam, setNull,
                    whereParam, whereInlineParam, allPredicatePercent, maxPredicateDepth,
                    leafComparison, leafIsNull, leafIsNotNull, leafFuture,
                    compoundAnd, compoundOr, compoundNot);
            }
        }
    }
}
