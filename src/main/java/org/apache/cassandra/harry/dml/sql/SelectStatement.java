package org.apache.cassandra.harry.dml.sql;

public record SelectStatement(
    TableSpec table,
    Projection projection,
    Predicate where,
    OrderBy[] orderBy,
    int limit
) implements Statement
{
    public static SelectStatement select(TableSpec table, Projection projection,
                                         Predicate where, OrderBy[] orderBy, int limit)
    {
        return new SelectStatement(table, projection, where, orderBy, limit);
    }
}
