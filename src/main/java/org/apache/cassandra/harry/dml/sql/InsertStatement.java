package org.apache.cassandra.harry.dml.sql;

public record InsertStatement(
    TableSpec table,
    InsertValue[] values,
    PkTuple pkTuple
) implements Statement
{
    public static InsertStatement insert(TableSpec table, InsertValue[] values, PkTuple pkTuple)
    {
        return new InsertStatement(table, values, pkTuple);
    }
}
