package org.apache.cassandra.harry.dml.sql;

public sealed interface Projection
{
    record Wildcard() implements Projection {}

    record Items(SelectItem[] items) implements Projection {}

    static Projection all()
    {
        return new Wildcard();
    }

    static Projection of(SelectItem... items)
    {
        return new Items(items);
    }

    static Projection columns(TableSpec.Column... columns)
    {
        SelectItem[] items = new SelectItem[columns.length];
        for (int i = 0; i < columns.length; i++)
            items[i] = SelectItem.col(columns[i]);
        return new Items(items);
    }
}
