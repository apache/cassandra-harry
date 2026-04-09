package org.apache.cassandra.harry.dml.sql;

public record OrderBy(TableSpec.Column column, boolean desc)
{
    public static OrderBy asc(TableSpec.Column column)
    {
        return new OrderBy(column, false);
    }

    public static OrderBy desc(TableSpec.Column column)
    {
        return new OrderBy(column, true);
    }
}
