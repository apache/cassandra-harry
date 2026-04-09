package org.apache.cassandra.harry.dml.sql;

import java.util.List;

public record RenderedStatement(String sql, List<Object> bindings, List<ValueRef> valueRefs)
{
    public record ValueRef(TableSpec.Column column, ValueIndex valueIndex) {}

    public RenderedStatement(String sql, List<Object> bindings)
    {
        this(sql, bindings, List.of());
    }

    /**
     * For debugging: interpolate bindings into the SQL, replacing each '?'
     * with a string representation of the corresponding binding value.
     */
    public String interpolated()
    {
        StringBuilder sb = new StringBuilder();
        int bindIdx = 0;
        for (int i = 0; i < sql.length(); i++)
        {
            char ch = sql.charAt(i);
            if (ch == '?' && bindIdx < bindings.size())
            {
                Object val = bindings.get(bindIdx++);
                if (val == null)
                    sb.append("NULL");
                else if (val instanceof String)
                    sb.append('\'').append(val).append('\'');
                else
                    sb.append(val);
            }
            else
            {
                sb.append(ch);
            }
        }
        return sb.toString();
    }
}
