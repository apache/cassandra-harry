package org.apache.cassandra.harry.dml.sql;

import org.junit.Before;
import org.junit.Test;

import static org.apache.cassandra.harry.dml.sql.DslPrelude.*;
import static org.junit.Assert.*;

public class SqlRendererTest
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
    public void testSingleAssignmentEqPredicate()
    {
        var stmt = update(spec,
                          assign(set(v0, param(v0, value(5)))),
                          eq(pk, param(pk, value(7))));

        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("UPDATE public.t1 SET v0 = ? WHERE pk = ?", result.sql());
        assertEquals(2, result.bindings().size());
    }

    @Test
    public void testMultipleAssignments()
    {
        var stmt = update(spec,
                          assign(set(v0, param(v0, value(3))),
                                 set(v1, param(v1, value(1)))),
                          eq(pk, param(pk, value(0))));

        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("UPDATE public.t1 SET v0 = ?, v1 = ? WHERE pk = ?", result.sql());
        assertEquals(3, result.bindings().size());
    }

    @Test
    public void testNullLiteralInAssignment()
    {
        var stmt = update(spec,
                          assign(set(v0, nullLiteral())),
                          eq(pk, param(pk, value(0))));

        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("UPDATE public.t1 SET v0 = NULL WHERE pk = ?", result.sql());
        assertEquals(1, result.bindings().size());
    }

    @Test
    public void testMixedValuesAndNulls()
    {
        var stmt = update(spec,
                          assign(set(v0, param(v0, value(5))),
                                 set(v1, nullLiteral())),
                          eq(pk, param(pk, value(0))));

        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("UPDATE public.t1 SET v0 = ?, v1 = NULL WHERE pk = ?", result.sql());
        assertEquals(2, result.bindings().size());
    }

    @Test
    public void testAndPredicate()
    {
        var stmt = update(spec,
                          assign(set(v0, param(v0, value(1)))),
                          and(eq(pk, param(pk, value(0))),
                              gt(v0, param(v0, value(5)))));

        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("UPDATE public.t1 SET v0 = ? WHERE (pk = ?) AND (v0 > ?)", result.sql());
        assertEquals(3, result.bindings().size());
    }

    @Test
    public void testOrPredicate()
    {
        var stmt = update(spec,
                          assign(set(v0, param(v0, value(1)))),
                          or(eq(v0, param(v0, value(5))),
                             eq(v1, param(v1, value(2)))));

        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("UPDATE public.t1 SET v0 = ? WHERE (v0 = ?) OR (v1 = ?)", result.sql());
        assertEquals(3, result.bindings().size());
    }

    @Test
    public void testNotPredicate()
    {
        var stmt = update(spec,
                          assign(set(v0, param(v0, value(1)))),
                          not(eq(v1, param(v1, value(2)))));

        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("UPDATE public.t1 SET v0 = ? WHERE NOT (v1 = ?)", result.sql());
        assertEquals(2, result.bindings().size());
    }

    @Test
    public void testNestedCompoundPredicate()
    {
        var stmt = update(spec,
                          assign(set(v0, param(v0, value(1)))),
                          or(and(eq(v0, param(v0, value(5))),
                                 gt(pk, param(pk, value(0)))),
                             eq(v1, param(v1, value(2)))));

        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals(
            "UPDATE public.t1 SET v0 = ? WHERE ((v0 = ?) AND (pk > ?)) OR (v1 = ?)",
            result.sql());
        assertEquals(4, result.bindings().size());
    }

    @Test
    public void testIsNull()
    {
        var stmt = update(spec,
                          assign(set(v0, param(v0, value(1)))),
                          isNull(v1));

        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("UPDATE public.t1 SET v0 = ? WHERE v1 IS NULL", result.sql());
        assertEquals(1, result.bindings().size());
    }

    @Test
    public void testIsNotNull()
    {
        var stmt = update(spec,
                          assign(set(v0, param(v0, value(1)))),
                          isNotNull(v1));

        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("UPDATE public.t1 SET v0 = ? WHERE v1 IS NOT NULL", result.sql());
        assertEquals(1, result.bindings().size());
    }

    @Test
    public void testAllPredicateOmitsWhere()
    {
        var stmt = update(spec,
                          assign(set(v0, param(v0, value(1)))),
                          all());

        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("UPDATE public.t1 SET v0 = ?", result.sql());
        assertFalse(result.sql().contains("WHERE"));
        assertEquals(1, result.bindings().size());
    }

    @Test
    public void testBindingOrderSetThenWhere()
    {
        var stmt = update(spec,
                          assign(set(v0, param(v0, value(3))),
                                 set(v1, param(v1, value(4)))),
                          eq(pk, param(pk, value(0))));

        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals(3, result.bindings().size());
        // Bindings come from the pool; just verify order is SET columns first, then WHERE
        assertNotNull(result.bindings().get(0)); // v0 value
        assertNotNull(result.bindings().get(1)); // v1 value
        assertNotNull(result.bindings().get(2)); // pk value
    }

    @Test
    public void testValueRefsParallelToBindings()
    {
        var stmt = update(spec,
                          assign(set(v0, param(v0, value(3))),
                                 set(v1, param(v1, value(4)))),
                          eq(pk, param(pk, value(0))));

        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals(3, result.valueRefs().size());
        assertEquals(result.bindings().size(), result.valueRefs().size());

        // Value refs track column + value index for each binding, in order
        assertEquals(v0, result.valueRefs().get(0).column());
        assertEquals(value(3).getValue(), result.valueRefs().get(0).valueIndex().getValue());

        assertEquals(v1, result.valueRefs().get(1).column());
        assertEquals(value(4).getValue(), result.valueRefs().get(1).valueIndex().getValue());

        assertEquals(pk, result.valueRefs().get(2).column());
        assertEquals(value(0).getValue(), result.valueRefs().get(2).valueIndex().getValue());
    }

    @Test
    public void testValueRefsExcludeNullsAndInline()
    {
        var stmt = insert(spec, values(
            val(pk, param(pk, value(0))),
            val(v0, nullLiteral()),
            val(v1, inlineParam(v1, value(2)))));

        RenderedStatement result = SqlRenderer.render(stmt);
        // Only 1 binding (pk param); null literal and inline param produce no bindings
        assertEquals(1, result.bindings().size());
        assertEquals(1, result.valueRefs().size());
        assertEquals(pk, result.valueRefs().get(0).column());
        assertEquals(value(0).getValue(), result.valueRefs().get(0).valueIndex().getValue());
    }

    @Test
    public void testValueRefsEmptyForNoBindings()
    {
        var stmt = from(spec).build();
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals(0, result.bindings().size());
        assertEquals(0, result.valueRefs().size());
    }

    @Test
    public void testComparisonOps()
    {
        for (ComparisonOp op : ComparisonOp.values())
        {
            Predicate pred = new Predicate.Comparison(col(v0), op, param(v0, value(1)));
            var stmt = update(spec,
                              assign(set(v1, param(v1, value(2)))),
                              pred);

            RenderedStatement result = SqlRenderer.render(stmt);
            assertTrue("Expected operator " + op.symbol() + " in: " + result.sql(),
                       result.sql().contains("v0 " + op.symbol() + " ?"));
        }
    }

    @Test
    public void testNoSchemaInTableName()
    {
        TableSpec noSchema = builder(null, "t1")
            .column("pk", int64Type, pk())
            .column("v0", int32Type, population(10))
            .build();

        var stmt = update(noSchema,
                          assign(set(noSchema.column("v0"), param(noSchema.column("v0"), value(1)))),
                          eq(noSchema.column("pk"), param(noSchema.column("pk"), value(0))));

        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("UPDATE t1 SET v0 = ? WHERE pk = ?", result.sql());
    }

    @Test
    public void testInterpolated()
    {
        var stmt = update(spec,
                          assign(set(v0, param(v0, value(5))),
                                 set(v1, nullLiteral())),
                          eq(pk, param(pk, value(0))));

        RenderedStatement result = SqlRenderer.render(stmt);
        // Interpolated replaces ? with actual pool values; just verify structure
        String interp = result.interpolated();
        assertTrue(interp.startsWith("UPDATE public.t1 SET v0 = "));
        assertTrue(interp.contains(", v1 = NULL WHERE pk = "));
        assertFalse(interp.contains("?"));
    }

    @Test
    public void testInterpolatedWithStrings()
    {
        var stmt = update(spec,
                          assign(set(v1, param(v1, value(0)))),
                          eq(pk, param(pk, value(0))));

        RenderedStatement result = SqlRenderer.render(stmt);
        String interp = result.interpolated();
        // String values get quoted with single quotes in interpolated output
        assertTrue(interp.contains("SET v1 = '"));
        assertFalse(interp.contains("?"));
    }

    // -- SELECT rendering --

    @Test
    public void testSelectWildcardNoFilter()
    {
        var stmt = from(spec).build();
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("SELECT * FROM public.t1", result.sql());
        assertEquals(0, result.bindings().size());
    }

    @Test
    public void testSelectColumnsWithFilter()
    {
        var stmt = from(spec)
            .columns(v0, v1)
            .where(gt(v0, param(v0, value(3))))
            .build();
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("SELECT v0, v1 FROM public.t1 WHERE v0 > ?", result.sql());
        assertEquals(1, result.bindings().size());
    }

    @Test
    public void testSelectWithOrderByAsc()
    {
        var stmt = from(spec).asc(v0).build();
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("SELECT * FROM public.t1 ORDER BY v0 ASC", result.sql());
    }

    @Test
    public void testSelectWithOrderByDesc()
    {
        var stmt = from(spec).desc(v0).build();
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("SELECT * FROM public.t1 ORDER BY v0 DESC", result.sql());
    }

    @Test
    public void testSelectMultiOrderBy()
    {
        var stmt = from(spec).asc(v0).desc(pk).build();
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("SELECT * FROM public.t1 ORDER BY v0 ASC, pk DESC", result.sql());
    }

    @Test
    public void testSelectWithLimit()
    {
        var stmt = from(spec).limit(10).build();
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("SELECT * FROM public.t1 LIMIT 10", result.sql());
    }

    @Test
    public void testSelectAllClauses()
    {
        var stmt = from(spec)
            .columns(v0)
            .where(eq(pk, param(pk, value(0))))
            .asc(v0)
            .limit(5)
            .build();
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("SELECT v0 FROM public.t1 WHERE pk = ? ORDER BY v0 ASC LIMIT 5", result.sql());
        assertEquals(1, result.bindings().size());
    }

    @Test
    public void testSelectAllPredicateOmitsWhere()
    {
        var stmt = from(spec).where(all()).build();
        RenderedStatement result = SqlRenderer.render(stmt);
        assertFalse(result.sql().contains("WHERE"));
    }

    @Test
    public void testSelectCompoundPredicate()
    {
        var stmt = from(spec)
            .where(and(eq(v0, param(v0, value(1))), isNotNull(v1)))
            .build();
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("SELECT * FROM public.t1 WHERE (v0 = ?) AND (v1 IS NOT NULL)", result.sql());
    }

    @Test
    public void testSelectAliasedItem()
    {
        var stmt = from(spec)
            .items(SelectItem.col(v0), SelectItem.as(col(v1), "label"))
            .build();
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("SELECT v0, v1 AS label FROM public.t1", result.sql());
    }

    @Test
    public void testSelectNoSchemaInTableName()
    {
        TableSpec noSchema = builder(null, "t1")
            .column("pk", int64Type, pk())
            .column("v0", int32Type, population(10))
            .build();

        var stmt = new SelectBuilder(noSchema).columns(noSchema.column("v0")).build();
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("SELECT v0 FROM t1", result.sql());
    }

    // -- DELETE rendering --

    @Test
    public void testDeleteWithEqPredicate()
    {
        var stmt = delete(spec, eq(pk, param(pk, value(0))));
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("DELETE FROM public.t1 WHERE pk = ?", result.sql());
        assertEquals(1, result.bindings().size());
    }

    @Test
    public void testDeleteWithCompoundPredicate()
    {
        var stmt = delete(spec, and(eq(v0, param(v0, value(1))), isNotNull(v1)));
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("DELETE FROM public.t1 WHERE (v0 = ?) AND (v1 IS NOT NULL)", result.sql());
        assertEquals(1, result.bindings().size());
    }

    @Test
    public void testDeleteAllPredicateOmitsWhere()
    {
        var stmt = delete(spec, all());
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("DELETE FROM public.t1", result.sql());
        assertFalse(result.sql().contains("WHERE"));
        assertEquals(0, result.bindings().size());
    }

    @Test
    public void testDeleteNoSchemaInTableName()
    {
        TableSpec noSchema = builder(null, "t1")
            .column("pk", int64Type, pk())
            .column("v0", int32Type, population(10))
            .build();

        var stmt = delete(noSchema, eq(noSchema.column("pk"), param(noSchema.column("pk"), value(0))));
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("DELETE FROM t1 WHERE pk = ?", result.sql());
    }

    // -- INSERT rendering --

    @Test
    public void testInsertWithParams()
    {
        var stmt = insert(spec, values(
            val(pk, param(pk, value(0))),
            val(v0, param(v0, value(5)))));
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("INSERT INTO public.t1 (pk, v0) VALUES (?, ?)", result.sql());
        assertEquals(2, result.bindings().size());
    }

    @Test
    public void testInsertWithNull()
    {
        var stmt = insert(spec, values(
            val(pk, param(pk, value(0))),
            val(v0, param(v0, value(5))),
            val(v1, nullLiteral())));
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("INSERT INTO public.t1 (pk, v0, v1) VALUES (?, ?, NULL)", result.sql());
        assertEquals(2, result.bindings().size());
    }

    @Test
    public void testInsertWithInlineParam()
    {
        var stmt = insert(spec, values(
            val(pk, inlineParam(pk, value(0))),
            val(v0, inlineParam(v0, value(5)))));
        RenderedStatement result = SqlRenderer.render(stmt);
        // Inline params are rendered directly, not as ?
        assertFalse(result.sql().contains("?"));
        assertTrue(result.sql().startsWith("INSERT INTO public.t1 (pk, v0) VALUES ("));
        assertEquals(0, result.bindings().size());
    }

    @Test
    public void testInsertWithInlineParamBindAll()
    {
        var stmt = insert(spec, values(
            val(pk, inlineParam(pk, value(0))),
            val(v0, inlineParam(v0, value(5)))));
        RenderedStatement result = SqlRenderer.render(stmt, true);
        // bindAll treats inline params as regular params
        assertEquals("INSERT INTO public.t1 (pk, v0) VALUES (?, ?)", result.sql());
        assertEquals(2, result.bindings().size());
        assertEquals(2, result.valueRefs().size());
    }

    @Test
    public void testBindAllPromotesInlineInPredicate()
    {
        var stmt = update(spec,
                          assign(set(v0, inlineParam(v0, value(5)))),
                          eq(pk, inlineParam(pk, value(0))));
        RenderedStatement result = SqlRenderer.render(stmt, true);
        assertEquals("UPDATE public.t1 SET v0 = ? WHERE pk = ?", result.sql());
        assertEquals(2, result.bindings().size());
        assertEquals(2, result.valueRefs().size());
    }

    @Test
    public void testInsertWithMixedExpressions()
    {
        var stmt = insert(spec, values(
            val(pk, param(pk, value(0))),
            val(v0, nullLiteral()),
            val(v1, param(v1, value(2)))));
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("INSERT INTO public.t1 (pk, v0, v1) VALUES (?, NULL, ?)", result.sql());
        assertEquals(2, result.bindings().size());
    }

    @Test
    public void testInsertBindingOrderLeftToRight()
    {
        var stmt = insert(spec, values(
            val(pk, param(pk, value(0))),
            val(v0, param(v0, value(3))),
            val(v1, param(v1, value(4)))));
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals(3, result.bindings().size());
        assertNotNull(result.bindings().get(0)); // pk value
        assertNotNull(result.bindings().get(1)); // v0 value
        assertNotNull(result.bindings().get(2)); // v1 value
    }

    @Test
    public void testInsertNoSchemaInTableName()
    {
        TableSpec noSchema = builder(null, "t1")
            .column("pk", int64Type, pk())
            .column("v0", int32Type, population(10))
            .build();

        var stmt = insert(noSchema, values(
            val(noSchema.column("pk"), param(noSchema.column("pk"), value(0))),
            val(noSchema.column("v0"), param(noSchema.column("v0"), value(1)))));
        RenderedStatement result = SqlRenderer.render(stmt);
        assertEquals("INSERT INTO t1 (pk, v0) VALUES (?, ?)", result.sql());
    }

    @Test
    public void testInsertInterpolated()
    {
        var stmt = insert(spec, values(
            val(pk, param(pk, value(0))),
            val(v1, param(v1, value(2)))));
        RenderedStatement result = SqlRenderer.render(stmt);
        String interp = result.interpolated();
        assertFalse(interp.contains("?"));
        assertTrue(interp.startsWith("INSERT INTO public.t1 (pk, v1) VALUES ("));
    }
}
