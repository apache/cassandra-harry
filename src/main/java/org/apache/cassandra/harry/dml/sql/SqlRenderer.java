package org.apache.cassandra.harry.dml.sql;

import java.util.ArrayList;
import java.util.List;

public class SqlRenderer
{
    private final List<Object> bindings = new ArrayList<>();
    private final List<RenderedStatement.ValueRef> valueRefs = new ArrayList<>();
    private final boolean bindAll;

    private SqlRenderer(boolean bindAll)
    {
        this.bindAll = bindAll;
    }

    public static RenderedStatement render(Statement statement)
    {
        return render(statement, false);
    }

    /**
     * Render a statement to SQL. When {@code bindAll} is true, InlineParam
     * expressions are treated as regular Param bindings (producing '?'
     * placeholders). Use this mode for JDBC targets where all values should
     * go through PreparedStatement parameter binding.
     */
    public static RenderedStatement render(Statement statement, boolean bindAll)
    {
        SqlRenderer r = new SqlRenderer(bindAll);
        StringBuilder sb = new StringBuilder();
        r.renderStatement(sb, statement);
        return new RenderedStatement(sb.toString(), List.copyOf(r.bindings), List.copyOf(r.valueRefs));
    }

    private void renderStatement(StringBuilder sb, Statement stmt)
    {
        switch (stmt)
        {
            case UpdateStatement u -> renderUpdate(sb, u);
            case SelectStatement s -> renderSelect(sb, s);
            case DeleteStatement d -> renderDelete(sb, d);
            case InsertStatement i -> renderInsert(sb, i);
        }
    }

    private void renderUpdate(StringBuilder sb, UpdateStatement update)
    {
        sb.append("UPDATE ");
        renderTableName(sb, update.table());

        sb.append(" SET ");
        Assignment[] assignments = update.assignments();
        for (int i = 0; i < assignments.length; i++)
        {
            if (i > 0) sb.append(", ");
            renderAssignment(sb, assignments[i]);
        }

        if (!(update.where() instanceof Predicate.All))
        {
            sb.append(" WHERE ");
            renderPredicate(sb, update.where());
        }
    }

    private void renderPredicate(StringBuilder sb, Predicate pred)
    {
        switch (pred)
        {
            case Predicate.Comparison c ->
            {
                renderColumnRef(sb, c.column());
                sb.append(' ').append(opSymbol(c.op())).append(' ');
                renderExpression(sb, c.value());
            }
            case Predicate.And a ->
            {
                for (int i = 0; i < a.children().size(); i++)
                {
                    if (i > 0) sb.append(" AND ");
                    sb.append('(');
                    renderPredicate(sb, a.children().get(i));
                    sb.append(')');
                }
            }
            case Predicate.Or o ->
            {
                for (int i = 0; i < o.children().size(); i++)
                {
                    if (i > 0) sb.append(" OR ");
                    sb.append('(');
                    renderPredicate(sb, o.children().get(i));
                    sb.append(')');
                }
            }
            case Predicate.Not n ->
            {
                sb.append("NOT (");
                renderPredicate(sb, n.child());
                sb.append(')');
            }
            case Predicate.IsNull isn ->
            {
                renderColumnRef(sb, isn.column());
                sb.append(" IS NULL");
            }
            case Predicate.IsNotNull isnn ->
            {
                renderColumnRef(sb, isnn.column());
                sb.append(" IS NOT NULL");
            }
            case Predicate.All ignored -> sb.append("1 = 1");
        }
    }

    private void renderExpression(StringBuilder sb, Expression expr)
    {
        switch (expr)
        {
            case Expression.Param p ->
            {
                sb.append('?');
                bindings.add(p.column().inflate(p.valueIndex()));
                valueRefs.add(new RenderedStatement.ValueRef(p.column(), p.valueIndex()));
            }
            case Expression.InlineParam ip ->
            {
                if (bindAll)
                {
                    sb.append('?');
                    bindings.add(ip.column().inflate(ip.valueIndex()));
                    valueRefs.add(new RenderedStatement.ValueRef(ip.column(), ip.valueIndex()));
                }
                else
                {
                    Object val = ip.column().inflate(ip.valueIndex());
                    if (val == null)
                        sb.append("NULL");
                    else if (val instanceof String s)
                        sb.append("'").append(s.replace("'", "''")).append("'");
                    else if (val instanceof java.util.UUID)
                        sb.append("'").append(val).append("'");
                    else if (val instanceof java.util.Date d)
                        sb.append("'").append(new java.sql.Timestamp(d.getTime())).append("'");
                    else if (val instanceof java.nio.ByteBuffer bb)
                    {
                        sb.append("'\\x");
                        java.nio.ByteBuffer dup = bb.duplicate();
                        while (dup.hasRemaining())
                            sb.append(String.format("%02x", dup.get()));
                        sb.append("'");
                    }
                    else
                        sb.append(val);
                }
            }
            case Expression.NullLiteral ignored -> sb.append("NULL");
            case Expression.ColumnRef ref -> renderColumnRef(sb, ref);
        }
    }

    private void renderSelect(StringBuilder sb, SelectStatement select)
    {
        sb.append("SELECT ");
        renderProjection(sb, select.projection());

        sb.append(" FROM ");
        renderTableName(sb, select.table());

        if (!(select.where() instanceof Predicate.All))
        {
            sb.append(" WHERE ");
            renderPredicate(sb, select.where());
        }

        OrderBy[] orderBy = select.orderBy();
        if (orderBy.length > 0)
        {
            sb.append(" ORDER BY ");
            for (int i = 0; i < orderBy.length; i++)
            {
                if (i > 0) sb.append(", ");
                sb.append(orderBy[i].column().name);
                sb.append(orderBy[i].desc() ? " DESC" : " ASC");
            }
        }

        if (select.limit() >= 0)
        {
            sb.append(" LIMIT ").append(select.limit());
        }
    }

    private void renderProjection(StringBuilder sb, Projection projection)
    {
        switch (projection)
        {
            case Projection.Wildcard ignored -> sb.append('*');
            case Projection.Items items ->
            {
                SelectItem[] entries = items.items();
                for (int i = 0; i < entries.length; i++)
                {
                    if (i > 0) sb.append(", ");
                    renderSelectItem(sb, entries[i]);
                }
            }
        }
    }

    private void renderSelectItem(StringBuilder sb, SelectItem item)
    {
        renderExpression(sb, item.expression());
        if (item.alias() != null)
            sb.append(" AS ").append(item.alias());
    }

    private void renderDelete(StringBuilder sb, DeleteStatement delete)
    {
        sb.append("DELETE FROM ");
        renderTableName(sb, delete.table());

        if (!(delete.where() instanceof Predicate.All))
        {
            sb.append(" WHERE ");
            renderPredicate(sb, delete.where());
        }
    }

    private void renderInsert(StringBuilder sb, InsertStatement insert)
    {
        sb.append("INSERT INTO ");
        renderTableName(sb, insert.table());

        InsertValue[] values = insert.values();
        sb.append(" (");
        for (int i = 0; i < values.length; i++)
        {
            if (i > 0) sb.append(", ");
            renderColumnRef(sb, values[i].column());
        }
        sb.append(") VALUES (");
        for (int i = 0; i < values.length; i++)
        {
            if (i > 0) sb.append(", ");
            renderExpression(sb, values[i].value());
        }
        sb.append(')');
    }

    private void renderTableName(StringBuilder sb, TableSpec table)
    {
        if (table.schema() != null)
            sb.append(table.schema()).append('.');
        sb.append(table.table());
    }

    private void renderColumnRef(StringBuilder sb, Expression.ColumnRef ref)
    {
        sb.append(ref.column().name);
    }

    private void renderAssignment(StringBuilder sb, Assignment a)
    {
        renderColumnRef(sb, a.column());
        sb.append(" = ");
        renderExpression(sb, a.value());
    }

    private String opSymbol(ComparisonOp op)
    {
        return op.symbol();
    }
}
