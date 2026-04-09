package org.apache.cassandra.harry.dml.sql;

public record DeleteStatement(
    TableSpec table,
    Predicate where
) implements Statement
{
    public static DeleteStatement delete(TableSpec table, Predicate where)
    {
        return new DeleteStatement(table, where);
    }
}
