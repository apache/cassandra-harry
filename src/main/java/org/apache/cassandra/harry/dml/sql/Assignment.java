package org.apache.cassandra.harry.dml.sql;

public record Assignment(Expression.ColumnRef column, Expression value)
{
    public static Assignment set(TableSpec.Column column, Expression value)
    {
        return new Assignment(Expression.col(column), value);
    }

    public static Assignment[] assign(Assignment... assignments)
    {
        return assignments;
    }
}
