package org.apache.cassandra.harry.dml.sql;

import java.util.Objects;

public class ColumnIndex {
    final int val;

    public ColumnIndex(int val) {
        this.val = val;
    }

    public int getValue() {
        return this.val;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnIndex)) return false;
        ColumnIndex other = (ColumnIndex) o;
        return this.val == other.val;
    }

    @Override
    public int hashCode() {
        return Objects.hash(val);
    }
}