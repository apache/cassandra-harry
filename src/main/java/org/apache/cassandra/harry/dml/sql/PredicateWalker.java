package org.apache.cassandra.harry.dml.sql;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.harry.gen.EntropySource;

/**
 * Shared predicate grammar walker used by all statement walkers. Given a
 * schema and weights, generates random predicates deterministically from
 * an entropy source. Values are resolved through Column.inflate() directly.
 */
public class PredicateWalker
{
    private final TableSpec spec;
    private final Weights weights;

    public PredicateWalker(TableSpec spec, Weights weights)
    {
        this.spec = spec;
        this.weights = weights;
    }

    public Predicate walkPredicate(EntropySource rng, int depth)
    {
        if (depth == 0 && rng.nextInt(100) < weights.allPredicatePercent)
            return Predicate.all();

        if (depth >= weights.maxPredicateDepth || !fork(rng))
            return walkLeafPredicate(rng);

        return walkCompoundPredicate(rng, depth);
    }

    private Predicate walkLeafPredicate(EntropySource rng)
    {
        TableSpec.Column col = pickColumn(rng, spec.columns());
        int total = weights.leafComparison + weights.leafIsNull
                    + weights.leafIsNotNull + weights.leafFuture;
        int choice = rng.nextInt(total);

        if (choice < weights.leafComparison)
            return walkComparison(rng, col);
        else if (choice < weights.leafComparison + weights.leafIsNull)
            return Predicate.isNull(col);
        else if (choice < weights.leafComparison + weights.leafIsNull + weights.leafIsNotNull)
            return Predicate.isNotNull(col);
        else
            return walkComparison(rng, col);
    }

    private Predicate walkComparison(EntropySource rng, TableSpec.Column col)
    {
        ComparisonOp op = pickOp(rng);
        Expression value = walkWhereExpression(rng, col);
        return new Predicate.Comparison(Expression.col(col), op, value);
    }

    public Expression walkWhereExpression(EntropySource rng, TableSpec.Column col)
    {
        int total = weights.whereParam + weights.whereInlineParam;
        int choice = rng.nextInt(total);
        if (choice < weights.whereParam)
            return Expression.param(col, randomValueIndex(rng, col));
        else
            return Expression.inlineParam(col, randomValueIndex(rng, col));
    }

    private Predicate walkCompoundPredicate(EntropySource rng, int depth)
    {
        int total = weights.compoundAnd + weights.compoundOr + weights.compoundNot;
        int choice = rng.nextInt(total);

        if (choice < weights.compoundAnd)
            return walkNary(rng, depth, true);
        else if (choice < weights.compoundAnd + weights.compoundOr)
            return walkNary(rng, depth, false);
        else
            return Predicate.not(walkPredicate(rng, depth + 1));
    }

    private Predicate walkNary(EntropySource rng, int depth, boolean isAnd)
    {
        List<Predicate> children = new ArrayList<>();
        children.add(walkPredicate(rng, depth + 1));
        children.add(walkPredicate(rng, depth + 1));

        while (fork(rng))
            children.add(walkPredicate(rng, depth + 1));

        return isAnd
            ? new Predicate.And(List.copyOf(children))
            : new Predicate.Or(List.copyOf(children));
    }

    private boolean fork(EntropySource rng)
    {
        return rng.nextDouble() < weights.forkProbability;
    }

    private TableSpec.Column pickColumn(EntropySource rng, List<TableSpec.Column> columns)
    {
        return columns.get(rng.nextInt(columns.size()));
    }

    private ComparisonOp pickOp(EntropySource rng)
    {
        ComparisonOp[] ops = ComparisonOp.values();
        return ops[rng.nextInt(ops.length)];
    }

    private ValueIndex randomValueIndex(EntropySource rng, TableSpec.Column col)
    {
        long pop = col.population();
        return ValueIndex.value(rng.nextInt(0, (int) Math.min(pop, Integer.MAX_VALUE)));
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

        public static Weights defaults(double forkProbability)
        {
            return new Weights(forkProbability, 80, 20, 5, 4, 60, 15, 15, 10, 45, 35, 20);
        }
    }
}
