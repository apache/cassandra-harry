package org.apache.cassandra.harry.dml.sql;

public record UpdateStatement(
    TableSpec table,
    Assignment[] assignments,
    Predicate where
) implements Statement
{
    public static UpdateStatement update(TableSpec table, Assignment[] assignments, Predicate where)
    {
        return new UpdateStatement(table, assignments, where);
    }
}
