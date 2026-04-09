package org.apache.cassandra.harry.dml.sql;

public record InsertValue(Expression.ColumnRef column, Expression value)
{
    public static InsertValue val(TableSpec.Column column, Expression value)
    {
        return new InsertValue(Expression.col(column), value);
    }

    public static InsertValue[] values(InsertValue... values)
    {
        return values;
    }
}
