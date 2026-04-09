package org.apache.cassandra.harry.dml.sql;

import java.util.ArrayList;
import java.util.List;

public class InsertBuilder
{
    private final TableSpec table;
    private final List<InsertValue> values = new ArrayList<>();

    public InsertBuilder(TableSpec table)
    {
        this.table = table;
    }

    public InsertBuilder set(TableSpec.Column column, Expression value)
    {
        values.add(InsertValue.val(column, value));
        return this;
    }

    public InsertBuilder set(TableSpec.Column column, ValueIndex valueIndex)
    {
        values.add(InsertValue.val(column, Expression.param(column, valueIndex)));
        return this;
    }

    public InsertBuilder setNull(TableSpec.Column column)
    {
        values.add(InsertValue.val(column, Expression.nullLiteral()));
        return this;
    }

    public InsertStatement build()
    {
        if (values.isEmpty())
            throw new IllegalStateException("INSERT must have at least one value");
        return new InsertStatement(table, values.toArray(new InsertValue[0]), buildPkTuple());
    }

    private PkTuple buildPkTuple()
    {
        java.util.List<TableSpec.Column> pks = table.primaryKey();
        int[] pkIndexes = new int[pks.size()];
        for (int i = 0; i < pks.size(); i++)
        {
            TableSpec.Column pk = pks.get(i);
            boolean found = false;
            for (InsertValue iv : values)
            {
                if (iv.column().column() == pk)
                {
                    pkIndexes[i] = switch (iv.value()) {
                        case Expression.Param p -> (int) p.valueIndex().getValue();
                        case Expression.InlineParam ip -> (int) ip.valueIndex().getValue();
                        default -> throw new IllegalStateException("PK column must not be null");
                    };
                    found = true;
                    break;
                }
            }
            if (!found)
                throw new IllegalStateException("INSERT missing PK column: " + pk.name);
        }
        return new PkTuple(pkIndexes);
    }
}
