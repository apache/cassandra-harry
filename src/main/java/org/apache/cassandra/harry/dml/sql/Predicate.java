package org.apache.cassandra.harry.dml.sql;

import java.util.List;

public sealed interface Predicate
{
    // Leaf comparisons
    record Comparison(Expression.ColumnRef column, ComparisonOp op, Expression value) implements Predicate {}

    record IsNull(Expression.ColumnRef column) implements Predicate {}

    record IsNotNull(Expression.ColumnRef column) implements Predicate {}

    // Composite
    record And(List<Predicate> children) implements Predicate {}

    record Or(List<Predicate> children) implements Predicate {}

    record Not(Predicate child) implements Predicate {}

    // Matches all rows (no WHERE clause emitted)
    record All() implements Predicate {}

    // -- Factories (accept Column directly, wrap ColumnRef internally) --

    static Predicate eq(TableSpec.Column column, Expression value)
    {
        return new Comparison(Expression.col(column), ComparisonOp.EQ, value);
    }

    static Predicate neq(TableSpec.Column column, Expression value)
    {
        return new Comparison(Expression.col(column), ComparisonOp.NEQ, value);
    }

    static Predicate gt(TableSpec.Column column, Expression value)
    {
        return new Comparison(Expression.col(column), ComparisonOp.GT, value);
    }

    static Predicate gte(TableSpec.Column column, Expression value)
    {
        return new Comparison(Expression.col(column), ComparisonOp.GTE, value);
    }

    static Predicate lt(TableSpec.Column column, Expression value)
    {
        return new Comparison(Expression.col(column), ComparisonOp.LT, value);
    }

    static Predicate lte(TableSpec.Column column, Expression value)
    {
        return new Comparison(Expression.col(column), ComparisonOp.LTE, value);
    }

    static Predicate and(Predicate... children)
    {
        return new And(List.of(children));
    }

    static Predicate or(Predicate... children)
    {
        return new Or(List.of(children));
    }

    static Predicate not(Predicate child)
    {
        return new Not(child);
    }

    static Predicate isNull(TableSpec.Column column)
    {
        return new IsNull(Expression.col(column));
    }

    static Predicate isNotNull(TableSpec.Column column)
    {
        return new IsNotNull(Expression.col(column));
    }

    static Predicate all()
    {
        return new All();
    }
}
