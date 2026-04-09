package org.apache.cassandra.harry.dml.sql;

public class ValueIndex
{
    public static final ValueIndex NULL_INDEX = new ValueIndex(-1);

    final long val;

    public ValueIndex(long val)
    {
        this.val = val;
    }

    public static ValueIndex value(long val)
    {
        return new ValueIndex(val);
    }

    public long getValue()
    {
        return this.val;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (!(o instanceof ValueIndex that)) return false;
        return val == that.val;
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(val);
    }

    @Override
    public String toString()
    {
        return "ValueIndex(" + val + ")";
    }
}