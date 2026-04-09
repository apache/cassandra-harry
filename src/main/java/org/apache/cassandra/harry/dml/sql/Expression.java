package org.apache.cassandra.harry.dml.sql;

public sealed interface Expression
{
    record ColumnRef(TableSpec.Column column) implements Expression {}

    record Param(TableSpec.Column column, ValueIndex valueIndex) implements Expression {}

    record InlineParam(TableSpec.Column column, ValueIndex valueIndex) implements Expression {}

    record NullLiteral() implements Expression {}

    // -- Factories --

    static ColumnRef col(TableSpec.Column column)
    {
        return new ColumnRef(column);
    }

    static Param param(TableSpec.Column column, ValueIndex valueIndex)
    {
        return new Param(column, valueIndex);
    }

    static InlineParam inlineParam(TableSpec.Column column, ValueIndex valueIndex)
    {
        return new InlineParam(column, valueIndex);
    }

    static NullLiteral nullLiteral()
    {
        return new NullLiteral();
    }
}
