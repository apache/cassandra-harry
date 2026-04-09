package org.apache.cassandra.harry.dml.sql;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.harry.DataType;

public class JdbcStatementExecutor implements StatementExecutor
{
    private static final Logger logger = LoggerFactory.getLogger(JdbcStatementExecutor.class);

    private final Connection connection;

    public JdbcStatementExecutor(Connection connection)
    {
        this.connection = connection;
    }

    @Override
    public void execute(RenderedStatement stmt) throws SQLException
    {
        try (PreparedStatement ps = connection.prepareStatement(stmt.sql()))
        {
            bindAll(ps, stmt.bindings());
            ps.executeUpdate();
        }
        catch (SQLException e)
        {
            // SQLState 23505 = unique_violation in PostgreSQL
            if ("23505".equals(e.getSQLState()))
            {
                logger.debug("Ignoring duplicate key: {}", stmt.interpolated());
                return;
            }
            logger.error("Failed to execute statement: {}", stmt.interpolated(), e);
            throw e;
        }
    }

    @Override
    public List<IndexedRow> query(RenderedStatement stmt, TableSpec.Column[] columns) throws SQLException
    {
        try (PreparedStatement ps = connection.prepareStatement(stmt.sql()))
        {
            bindAll(ps, stmt.bindings());
            try (ResultSet rs = ps.executeQuery())
            {
                List<IndexedRow> rows = new ArrayList<>();
                while (rs.next())
                {
                    ValueIndex[] indices = new ValueIndex[columns.length];
                    for (int i = 0; i < columns.length; i++)
                    {
                        Object val = rs.getObject(i + 1);
                        if (val == null)
                        {
                            indices[i] = ValueIndex.NULL_INDEX;
                        }
                        else
                        {
                            val = normalizeJdbcValue(val, columns[i].type);
                            indices[i] = columns[i].deflate(val);
                        }
                    }
                    rows.add(new IndexedRow(columns, indices));
                }
                return rows;
            }
        }
        catch (SQLException e)
        {
            logger.error("Failed to execute query: {}", stmt.interpolated(), e);
            throw e;
        }
    }

    private static void bindAll(PreparedStatement ps, List<Object> bindings) throws SQLException
    {
        for (int i = 0; i < bindings.size(); i++)
            ps.setObject(i + 1, toJdbcValue(bindings.get(i)));
    }

    /**
     * Convert Harry value types to JDBC-compatible types for binding.
     */
    static Object toJdbcValue(Object val)
    {
        if (val instanceof java.util.Date d && !(val instanceof java.sql.Timestamp))
            return new java.sql.Timestamp(d.getTime());
        if (val instanceof java.nio.ByteBuffer bb)
        {
            byte[] bytes = new byte[bb.remaining()];
            bb.duplicate().get(bytes);
            return bytes;
        }
        return val;
    }

    /**
     * JDBC drivers return driver-specific Java types that may not match the
     * exact type the bijection expects. Dispatch on the SQL type name to
     * handle both standard and pg-variant DataType instances.
     */
    static Object normalizeJdbcValue(Object val, DataType<?> type)
    {
        String sqlName = type.sqlName();
        return switch (sqlName)
        {
            case "smallint" ->
            {
                // int8Type maps to PG "smallint"; JDBC returns Short, bijection expects Byte
                if (type == DataType.int8Type)
                    yield ((Number) val).byteValue();
                yield ((Number) val).shortValue();
            }
            case "integer" -> ((Number) val).intValue();
            case "bigint" -> ((Number) val).longValue();
            case "real" -> ((Number) val).floatValue();
            case "double precision" -> ((Number) val).doubleValue();
            case "bytea" ->
            {
                if (val instanceof byte[] bytes)
                    yield ByteBuffer.wrap(bytes);
                yield val;
            }
            case "timestamp" ->
            {
                if (val instanceof java.sql.Timestamp ts)
                    yield new Date(ts.getTime());
                yield val;
            }
            case "uuid" ->
            {
                if (val instanceof String s)
                    yield UUID.fromString(s);
                yield val;
            }
            case "numeric" ->
            {
                // varint expects BigInteger; decimal expects BigDecimal
                if (type == DataType.varintType)
                {
                    if (val instanceof BigDecimal bd)
                        yield bd.toBigIntegerExact();
                    if (val instanceof Number n)
                        yield BigInteger.valueOf(n.longValue());
                }
                yield val;
            }
            default -> val;
        };
    }
}
