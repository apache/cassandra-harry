package org.apache.cassandra.harry.dml.sql;

import java.util.Arrays;

public class SelectBuilder
{
    private final TableSpec table;
    private Projection projection = Projection.all();
    private Predicate where = Predicate.all();
    private OrderBy[] orderBy = new OrderBy[0];
    private int limit = -1;

    public SelectBuilder(TableSpec table)
    {
        this.table = table;
    }

    // -- Projection --

    public SelectBuilder all()
    {
        this.projection = Projection.all();
        return this;
    }

    public SelectBuilder columns(TableSpec.Column... cols)
    {
        this.projection = Projection.columns(cols);
        return this;
    }

    public SelectBuilder items(SelectItem... items)
    {
        this.projection = Projection.of(items);
        return this;
    }

    // -- WHERE --

    public SelectBuilder where(Predicate predicate)
    {
        this.where = predicate;
        return this;
    }

    // -- ORDER BY --

    public SelectBuilder orderBy(OrderBy... specs)
    {
        this.orderBy = specs;
        return this;
    }

    public SelectBuilder asc(TableSpec.Column column)
    {
        this.orderBy = Arrays.copyOf(this.orderBy, this.orderBy.length + 1);
        this.orderBy[this.orderBy.length - 1] = OrderBy.asc(column);
        return this;
    }

    public SelectBuilder desc(TableSpec.Column column)
    {
        this.orderBy = Arrays.copyOf(this.orderBy, this.orderBy.length + 1);
        this.orderBy[this.orderBy.length - 1] = OrderBy.desc(column);
        return this;
    }

    // -- LIMIT --

    public SelectBuilder limit(int n)
    {
        this.limit = n;
        return this;
    }

    public SelectStatement build()
    {
        return new SelectStatement(table, projection, where, orderBy, limit);
    }
}
