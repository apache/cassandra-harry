package org.apache.cassandra.harry.dml.sql;

import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;

/**
 * Deterministic random generator for DELETE statements. Given a schema and a
 * seed, produces a syntactically complete DeleteStatement by walking the WHERE
 * predicate grammar. All branching weights are configurable via {@link Weights}.
 *
 * DELETE is the simplest DML statement -- it only has a WHERE clause.
 */
public class DeleteStatementWalker
{
    private final TableSpec spec;
    private final PredicateWalker predicateWalker;

    public DeleteStatementWalker(TableSpec spec, Weights weights)
    {
        this.spec = spec;
        this.predicateWalker = new PredicateWalker(spec, weights.toPredicateWeights());
    }

    public DeleteStatementWalker(TableSpec spec, double forkProbability)
    {
        this(spec, Weights.withForkProbability(forkProbability));
    }

    public DeleteStatement generate(long seed)
    {
        EntropySource rng = new JdkRandomEntropySource(seed);
        Predicate where = predicateWalker.walkPredicate(rng, 0);
        return new DeleteStatement(spec, where);
    }

    public static class Weights
    {
        public final double forkProbability;
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

        public static Weights defaults()
        {
            return new Weights(
                0.3,
                80, 20,
                5, 4,
                60, 15, 15, 10,
                45, 35, 20
            );
        }

        public static Weights withForkProbability(double forkProbability)
        {
            Weights d = defaults();
            return new Weights(
                forkProbability,
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

        PredicateWalker.Weights toPredicateWeights()
        {
            return new PredicateWalker.Weights(
                forkProbability,
                whereParam, whereInlineParam,
                allPredicatePercent, maxPredicateDepth,
                leafComparison, leafIsNull, leafIsNotNull, leafFuture,
                compoundAnd, compoundOr, compoundNot
            );
        }

        public static class Builder
        {
            private double forkProbability = 0.3;
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
                return new Weights(
                    forkProbability,
                    whereParam, whereInlineParam,
                    allPredicatePercent, maxPredicateDepth,
                    leafComparison, leafIsNull, leafIsNotNull, leafFuture,
                    compoundAnd, compoundOr, compoundNot
                );
            }
        }
    }
}
