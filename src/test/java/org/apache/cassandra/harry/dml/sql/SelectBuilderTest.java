package org.apache.cassandra.harry.dml.sql;

import org.junit.Before;
import org.junit.Test;

import static org.apache.cassandra.harry.dml.sql.DslPrelude.*;
import static org.junit.Assert.*;

public class SelectBuilderTest
{
    private TableSpec spec;
    private TableSpec.Column pk;
    private TableSpec.Column v0;
    private TableSpec.Column v1;

    @Before
    public void setUp()
    {
        spec = builder("public", "t1")
            .column("pk", int64Type, pk())
            .column("v0", int32Type, population(100))
            .column("v1", asciiType, population(50))
            .build();
        pk = spec.column("pk");
        v0 = spec.column("v0");
        v1 = spec.column("v1");
    }

    @Test
    public void testDefaultsProduceWildcardNoFilterNoOrderNoLimit()
    {
        SelectStatement stmt = from(spec).build();
        assertTrue(stmt.projection() instanceof Projection.Wildcard);
        assertTrue(stmt.where() instanceof Predicate.All);
        assertEquals(0, stmt.orderBy().length);
        assertEquals(-1, stmt.limit());
    }

    @Test
    public void testColumns()
    {
        SelectStatement stmt = from(spec).columns(v0, v1).build();
        assertTrue(stmt.projection() instanceof Projection.Items);
        Projection.Items items = (Projection.Items) stmt.projection();
        assertEquals(2, items.items().length);
        assertTrue(items.items()[0].expression() instanceof Expression.ColumnRef);
        assertEquals("v0", ((Expression.ColumnRef) items.items()[0].expression()).column().name);
        assertEquals("v1", ((Expression.ColumnRef) items.items()[1].expression()).column().name);
    }

    @Test
    public void testItems()
    {
        SelectStatement stmt = from(spec)
            .items(SelectItem.col(v0), SelectItem.as(col(v1), "alias"))
            .build();
        Projection.Items items = (Projection.Items) stmt.projection();
        assertEquals(2, items.items().length);
        assertNull(items.items()[0].alias());
        assertEquals("alias", items.items()[1].alias());
    }

    @Test
    public void testWhere()
    {
        Predicate pred = eq(pk, param(pk, value(0)));
        SelectStatement stmt = from(spec).where(pred).build();
        assertSame(pred, stmt.where());
    }

    @Test
    public void testAscDesc()
    {
        SelectStatement stmt = from(spec).asc(v0).desc(pk).build();
        assertEquals(2, stmt.orderBy().length);
        assertEquals("v0", stmt.orderBy()[0].column().name);
        assertFalse(stmt.orderBy()[0].desc());
        assertEquals("pk", stmt.orderBy()[1].column().name);
        assertTrue(stmt.orderBy()[1].desc());
    }

    @Test
    public void testOrderByVarargs()
    {
        SelectStatement stmt = from(spec)
            .orderBy(OrderBy.asc(v0), OrderBy.desc(v1))
            .build();
        assertEquals(2, stmt.orderBy().length);
    }

    @Test
    public void testLimit()
    {
        SelectStatement stmt = from(spec).limit(10).build();
        assertEquals(10, stmt.limit());
    }

    @Test
    public void testWhereTwiceReplaces()
    {
        Predicate p1 = eq(pk, param(pk, value(0)));
        Predicate p2 = gt(v0, param(v0, value(1)));
        SelectStatement stmt = from(spec).where(p1).where(p2).build();
        assertSame(p2, stmt.where());
    }

    @Test
    public void testColumnsAfterItemsReplaces()
    {
        SelectStatement stmt = from(spec)
            .items(SelectItem.as(col(v0), "alias"))
            .columns(v0, v1)
            .build();
        Projection.Items items = (Projection.Items) stmt.projection();
        assertEquals(2, items.items().length);
        // Should be bare columns (no alias), since columns() replaced items()
        assertNull(items.items()[0].alias());
        assertNull(items.items()[1].alias());
    }

    @Test
    public void testAllClauses()
    {
        SelectStatement stmt = from(spec)
            .columns(v0)
            .where(eq(pk, param(pk, value(0))))
            .asc(v0)
            .limit(5)
            .build();
        assertTrue(stmt.projection() instanceof Projection.Items);
        assertTrue(stmt.where() instanceof Predicate.Comparison);
        assertEquals(1, stmt.orderBy().length);
        assertEquals(5, stmt.limit());
    }
}
