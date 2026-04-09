package org.apache.cassandra.harry.dml.sql;

public enum ComparisonOp
{
    EQ("="),
    NEQ("!="),
    GT(">"),
    GTE(">="),
    LT("<"),
    LTE("<=");

    private final String symbol;

    ComparisonOp(String symbol)
    {
        this.symbol = symbol;
    }

    public String symbol()
    {
        return symbol;
    }
}
