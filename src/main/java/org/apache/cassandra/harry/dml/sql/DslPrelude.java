package org.apache.cassandra.harry.dml.sql;

import org.apache.cassandra.harry.DataType;

/**
 * Single entry point for all SQL IR DSL factory methods and type constants.
 * Test classes can import everything with:
 * <pre>
 *     import static org.apache.cassandra.harry.dml.sql.DslPrelude.*;
 * </pre>
 */
public final class DslPrelude
{
    private DslPrelude() {}

    // ---- Types (from DataType) ----

    public static final DataType<Byte> int8Type = DataType.int8Type;
    public static final DataType<Short> int16Type = DataType.int16Type;
    public static final DataType<Integer> int32Type = DataType.int32Type;
    public static final DataType<Long> int64Type = DataType.int64Type;
    public static final DataType<Boolean> booleanType = DataType.booleanType;
    public static final DataType<Float> floatType = DataType.floatType;
    public static final DataType<Double> doubleType = DataType.doubleType;
    public static final DataType<java.nio.ByteBuffer> blobType = DataType.blobType;
    public static final DataType<java.nio.ByteBuffer> pgBlobType = DataType.pgBlobType;
    public static final DataType<String> asciiType = DataType.asciiType;
    public static final DataType<String> textType = DataType.textType;
    public static final DataType<java.util.UUID> uuidType = DataType.uuidType;
    public static final DataType<java.util.Date> timestampType = DataType.timestampType;
    public static final DataType<java.util.Date> pgTimestampType = DataType.pgTimestampType;
    public static final DataType<java.math.BigInteger> varintType = DataType.varintType;
    public static final DataType<Long> timeType = DataType.timeType;
    public static final DataType<java.math.BigDecimal> decimalType = DataType.decimalType;
    public static final DataType<java.net.InetAddress> inetType = DataType.inetType;

    // ---- TableSpec (builder, column options) ----

    public static TableSpec.Builder builder(String schema, String table)
    {
        return TableSpec.builder(schema, table);
    }

    public static TableSpec.ColumnOption pk()
    {
        return TableSpec.pk();
    }

    public static TableSpec.ColumnOption population(int n)
    {
        return TableSpec.population(n);
    }

    // ---- Expression ----

    public static Expression.ColumnRef col(TableSpec.Column column)
    {
        return Expression.col(column);
    }

    public static Expression.Param param(TableSpec.Column column, ValueIndex valueIndex)
    {
        return Expression.param(column, valueIndex);
    }

    public static Expression.InlineParam inlineParam(TableSpec.Column column, ValueIndex valueIndex)
    {
        return Expression.inlineParam(column, valueIndex);
    }

    public static Expression.NullLiteral nullLiteral()
    {
        return Expression.nullLiteral();
    }

    // ---- ValueIndex ----

    public static ValueIndex value(long val)
    {
        return ValueIndex.value(val);
    }

    // ---- Assignment ----

    public static Assignment set(TableSpec.Column column, Expression value)
    {
        return Assignment.set(column, value);
    }

    public static Assignment[] assign(Assignment... assignments)
    {
        return Assignment.assign(assignments);
    }

    // ---- Predicate ----

    public static Predicate eq(TableSpec.Column column, Expression value)
    {
        return Predicate.eq(column, value);
    }

    public static Predicate neq(TableSpec.Column column, Expression value)
    {
        return Predicate.neq(column, value);
    }

    public static Predicate gt(TableSpec.Column column, Expression value)
    {
        return Predicate.gt(column, value);
    }

    public static Predicate gte(TableSpec.Column column, Expression value)
    {
        return Predicate.gte(column, value);
    }

    public static Predicate lt(TableSpec.Column column, Expression value)
    {
        return Predicate.lt(column, value);
    }

    public static Predicate lte(TableSpec.Column column, Expression value)
    {
        return Predicate.lte(column, value);
    }

    public static Predicate and(Predicate... children)
    {
        return Predicate.and(children);
    }

    public static Predicate or(Predicate... children)
    {
        return Predicate.or(children);
    }

    public static Predicate not(Predicate child)
    {
        return Predicate.not(child);
    }

    public static Predicate isNull(TableSpec.Column column)
    {
        return Predicate.isNull(column);
    }

    public static Predicate isNotNull(TableSpec.Column column)
    {
        return Predicate.isNotNull(column);
    }

    public static Predicate all()
    {
        return Predicate.all();
    }

    // ---- SelectItem ----

    public static SelectItem selectCol(TableSpec.Column column)
    {
        return SelectItem.col(column);
    }

    public static SelectItem selectAs(Expression expression, String alias)
    {
        return SelectItem.as(expression, alias);
    }

    public static SelectItem selectItem(Expression expression)
    {
        return SelectItem.of(expression);
    }

    // ---- Statement ----

    public static UpdateStatement update(TableSpec table, Assignment[] assignments, Predicate where)
    {
        return UpdateStatement.update(table, assignments, where);
    }

    public static SelectStatement select(TableSpec table, Projection projection,
                                         Predicate where, OrderBy[] orderBy, int limit)
    {
        return SelectStatement.select(table, projection, where, orderBy, limit);
    }

    public static SelectBuilder from(TableSpec table)
    {
        return new SelectBuilder(table);
    }

    // ---- InsertValue ----

    public static InsertValue val(TableSpec.Column column, Expression value)
    {
        return InsertValue.val(column, value);
    }

    public static InsertValue[] values(InsertValue... values)
    {
        return InsertValue.values(values);
    }

    // ---- DeleteStatement ----

    public static DeleteStatement delete(TableSpec table, Predicate where)
    {
        return DeleteStatement.delete(table, where);
    }

    public static DeleteBuilder deleteFrom(TableSpec table)
    {
        return new DeleteBuilder(table);
    }

    // ---- InsertStatement ----

    public static InsertStatement insert(TableSpec table, InsertValue[] values)
    {
        InsertBuilder b = new InsertBuilder(table);
        for (InsertValue v : values)
            b.set(v.column().column(), v.value());
        return b.build();
    }

    public static InsertBuilder insertInto(TableSpec table)
    {
        return new InsertBuilder(table);
    }
}
