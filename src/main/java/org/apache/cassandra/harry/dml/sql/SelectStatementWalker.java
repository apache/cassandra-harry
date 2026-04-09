package org.apache.cassandra.harry.dml.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.rng.JdkRandomEntropySource;

/**
 * Deterministic random generator for SELECT statements. Given a schema and a
 * seed, produces a syntactically complete SelectStatement by walking the full
 * SELECT grammar. All branching weights are configurable via {@link Weights}.
 *
 * Predicate generation is delegated to {@link PredicateWalker}.
 */
public class SelectStatementWalker
{
    private final TableSpec spec;
    private final Weights weights;
    private final PredicateWalker predicateWalker;

    public SelectStatementWalker(TableSpec spec, Weights weights)
    {
        this.spec = spec;
        this.weights = weights;
        this.predicateWalker = new PredicateWalker(spec, weights.toPredicateWeights());
    }

    public SelectStatementWalker(TableSpec spec, double forkProbability)
    {
        this(spec, Weights.withForkProbability(forkProbability));
    }

    public SelectStatement generate(long seed)
    {
        EntropySource rng = new JdkRandomEntropySource(seed);
        Projection projection = walkProjection(rng);
        Predicate where = predicateWalker.walkPredicate(rng, 0);
        OrderBy[] orderBy = walkOrderBy(rng);
        int limit = walkLimit(rng);
        return new SelectStatement(spec, projection, where, orderBy, limit);
    }

    // -- Projection --

    private Projection walkProjection(EntropySource rng)
    {
        int total = weights.projectionWildcard + weights.projectionSubset;
        int choice = rng.nextInt(total);

        if (choice < weights.projectionWildcard)
            return Projection.all();

        // Pick a random subset of columns
        List<TableSpec.Column> available = new ArrayList<>(spec.columns());
        List<SelectItem> items = new ArrayList<>();
        items.add(SelectItem.col(pickAndRemove(rng, available)));

        while (!available.isEmpty() && fork(rng))
            items.add(SelectItem.col(pickAndRemove(rng, available)));

        return Projection.of(items.toArray(new SelectItem[0]));
    }

    // -- ORDER BY --

    private OrderBy[] walkOrderBy(EntropySource rng)
    {
        int total = weights.orderByNone + weights.orderByPresent;
        int choice = rng.nextInt(total);

        if (choice < weights.orderByNone)
            return new OrderBy[0];

        List<TableSpec.Column> available = new ArrayList<>(spec.columns());
        List<OrderBy> result = new ArrayList<>();
        result.add(makeOrderBy(rng, pickAndRemove(rng, available)));

        while (!available.isEmpty() && fork(rng))
            result.add(makeOrderBy(rng, pickAndRemove(rng, available)));

        return result.toArray(new OrderBy[0]);
    }

    private OrderBy makeOrderBy(EntropySource rng, TableSpec.Column col)
    {
        boolean desc = rng.nextInt(100) < weights.orderByDescPercent;
        return new OrderBy(col, desc);
    }

    // -- LIMIT --

    private int walkLimit(EntropySource rng)
    {
        int total = weights.limitNone + weights.limitPresent;
        int choice = rng.nextInt(total);

        if (choice < weights.limitNone)
            return -1;

        return rng.nextInt(1, 101);
    }

    // -- Utilities --

    private boolean fork(EntropySource rng)
    {
        return rng.nextDouble() < weights.forkProbability;
    }

    private TableSpec.Column pickAndRemove(EntropySource rng, List<TableSpec.Column> columns)
    {
        int idx = rng.nextInt(columns.size());
        return columns.remove(idx);
    }

    // -- Weights --

    public static class Weights
    {
        public final double forkProbability;

        // Projection weights
        public final int projectionWildcard;
        public final int projectionSubset;

        // ORDER BY weights
        public final int orderByNone;
        public final int orderByPresent;
        public final int orderByDescPercent;

        // LIMIT weights
        public final int limitNone;
        public final int limitPresent;

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
                       int projectionWildcard, int projectionSubset,
                       int orderByNone, int orderByPresent, int orderByDescPercent,
                       int limitNone, int limitPresent,
                       int whereParam, int whereInlineParam,
                       int allPredicatePercent, int maxPredicateDepth,
                       int leafComparison, int leafIsNull, int leafIsNotNull, int leafFuture,
                       int compoundAnd, int compoundOr, int compoundNot)
        {
            if (forkProbability < 0.0 || forkProbability > 1.0)
                throw new IllegalArgumentException("forkProbability must be in [0.0, 1.0]");

            this.forkProbability = forkProbability;
            this.projectionWildcard = projectionWildcard;
            this.projectionSubset = projectionSubset;
            this.orderByNone = orderByNone;
            this.orderByPresent = orderByPresent;
            this.orderByDescPercent = orderByDescPercent;
            this.limitNone = limitNone;
            this.limitPresent = limitPresent;
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
            return new Weights(
                0.3,
                40, 60,         // projection: wildcard, subset
                40, 60, 30,     // orderBy: none, present, descPercent
                70, 30,         // limit: none, present
                80, 20,         // where: param, inlineParam
                5, 4,           // allPredicatePercent, maxPredicateDepth
                60, 15, 15, 10, // leaf: comparison, isNull, isNotNull, future
                45, 35, 20      // compound: and, or, not
            );
        }

        public static Weights withForkProbability(double forkProbability)
        {
            Weights d = defaults();
            return new Weights(
                forkProbability,
                d.projectionWildcard, d.projectionSubset,
                d.orderByNone, d.orderByPresent, d.orderByDescPercent,
                d.limitNone, d.limitPresent,
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
            private int projectionWildcard = 40;
            private int projectionSubset = 60;
            private int orderByNone = 40;
            private int orderByPresent = 60;
            private int orderByDescPercent = 30;
            private int limitNone = 70;
            private int limitPresent = 30;
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
            public Builder projectionWildcard(int v) { projectionWildcard = v; return this; }
            public Builder projectionSubset(int v) { projectionSubset = v; return this; }
            public Builder orderByNone(int v) { orderByNone = v; return this; }
            public Builder orderByPresent(int v) { orderByPresent = v; return this; }
            public Builder orderByDescPercent(int v) { orderByDescPercent = v; return this; }
            public Builder limitNone(int v) { limitNone = v; return this; }
            public Builder limitPresent(int v) { limitPresent = v; return this; }
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
                return new Weights(forkProbability,
                    projectionWildcard, projectionSubset,
                    orderByNone, orderByPresent, orderByDescPercent,
                    limitNone, limitPresent,
                    whereParam, whereInlineParam,
                    allPredicatePercent, maxPredicateDepth,
                    leafComparison, leafIsNull, leafIsNotNull, leafFuture,
                    compoundAnd, compoundOr, compoundNot);
            }
        }
    }
}
