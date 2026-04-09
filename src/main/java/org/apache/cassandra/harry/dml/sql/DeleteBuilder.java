package org.apache.cassandra.harry.dml.sql;

public class DeleteBuilder
{
    private final TableSpec table;
    private Predicate where = Predicate.all();

    public DeleteBuilder(TableSpec table)
    {
        this.table = table;
    }

    public DeleteBuilder where(Predicate predicate)
    {
        this.where = predicate;
        return this;
    }

    public DeleteStatement build()
    {
        return new DeleteStatement(table, where);
    }
}
