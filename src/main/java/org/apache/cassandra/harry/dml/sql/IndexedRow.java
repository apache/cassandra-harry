package org.apache.cassandra.harry.dml.sql;

import java.util.Arrays;
import java.util.List;

public class IndexedRow
{
    private final TableSpec.Column[] columns;
    private final ValueIndex[] values;

    public IndexedRow(TableSpec.Column[] columns, ValueIndex[] values)
    {
        assert columns.length == values.length;
        this.columns = columns;
        this.values = values;
    }

    public ValueIndex get(int i)
    {
        return values[i];
    }

    public int width()
    {
        return values.length;
    }

    // -- Projection helper --

    public static TableSpec.Column[] projectionColumns(TableSpec spec, Projection projection)
    {
        return switch (projection)
        {
            case Projection.Wildcard w -> spec.columns().toArray(new TableSpec.Column[0]);
            case Projection.Items items ->
            {
                TableSpec.Column[] cols = new TableSpec.Column[items.items().length];
                for (int i = 0; i < items.items().length; i++)
                {
                    Expression expr = items.items()[i].expression();
                    if (expr instanceof Expression.ColumnRef ref)
                        cols[i] = ref.column();
                    else
                        throw new IllegalArgumentException(
                            "Cannot deflate non-column expression: " + expr);
                }
                yield cols;
            }
        };
    }

    // -- Deflation --

    public static IndexedRow deflate(List<Object> row, TableSpec.Column[] columns)
    {
        ValueIndex[] indices = new ValueIndex[row.size()];
        for (int i = 0; i < row.size(); i++)
        {
            Object val = row.get(i);
            indices[i] = val == null ? ValueIndex.NULL_INDEX : columns[i].deflate(val);
        }
        return new IndexedRow(columns, indices);
    }

    // -- equals / hashCode --

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof IndexedRow that)) return false;
        return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("row(");
        for (int i = 0; i < values.length; i++)
        {
            if (i > 0) sb.append(", ");
            sb.append(columns[i].name).append('=');
            if (ValueIndex.NULL_INDEX.equals(values[i]))
                sb.append("NULL");
            else
                sb.append(values[i].getValue())
                  .append('/').append(columns[i].inflate(values[i]));
        }
        return sb.append(')').toString();
    }
}
