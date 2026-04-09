package org.apache.cassandra.harry.dml.sql;

public record SelectItem(Expression expression, String alias)
{
    public static SelectItem of(Expression expression)
    {
        return new SelectItem(expression, null);
    }

    public static SelectItem as(Expression expression, String alias)
    {
        return new SelectItem(expression, alias);
    }

    public static SelectItem col(TableSpec.Column column)
    {
        return new SelectItem(Expression.col(column), null);
    }
}
