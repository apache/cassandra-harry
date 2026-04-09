package org.apache.cassandra.harry.dml.sql;

import java.util.Arrays;

/**
 * Holds the value indexes for each PK column in schema order.
 * indexes[i] corresponds to spec.primaryKey().get(i).
 */
public record PkTuple(int[] indexes)
{
    @Override
    public boolean equals(Object o)
    {
        return o instanceof PkTuple that && Arrays.equals(indexes, that.indexes);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(indexes);
    }

    @Override
    public String toString()
    {
        return "PkTuple" + Arrays.toString(indexes);
    }
}
