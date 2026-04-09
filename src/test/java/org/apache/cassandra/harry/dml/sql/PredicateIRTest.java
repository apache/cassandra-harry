package org.apache.cassandra.harry.dml.sql;

import org.junit.Test;

import static org.apache.cassandra.harry.dml.sql.DslPrelude.*;
import static org.junit.Assert.*;

public class PredicateIRTest
{
    private final TableSpec spec = builder("public", "t")
        .column("pk", int64Type, pk())
        .column("v0", int32Type)
        .column("v1", asciiType)
        .build();

    private final TableSpec.Column pk = spec.column("pk");
    private final TableSpec.Column v0 = spec.column("v0");
    private final TableSpec.Column v1 = spec.column("v1");

    @Test
    public void testComparison()
    {
        Predicate p = eq(pk, param(pk, value(0)));
        assertTrue(p instanceof Predicate.Comparison);

        Predicate.Comparison c = (Predicate.Comparison) p;
        assertEquals(pk, c.column().column());
        assertEquals(ComparisonOp.EQ, c.op());
        assertTrue(c.value() instanceof Expression.Param);
        assertEquals(0, ((Expression.Param) c.value()).valueIndex().getValue());
    }

    @Test
    public void testAllComparisonOps()
    {
        assertOp(eq(pk, param(pk, value(0))), ComparisonOp.EQ);
        assertOp(neq(pk, param(pk, value(0))), ComparisonOp.NEQ);
        assertOp(gt(pk, param(pk, value(0))), ComparisonOp.GT);
        assertOp(gte(pk, param(pk, value(0))), ComparisonOp.GTE);
        assertOp(lt(pk, param(pk, value(0))), ComparisonOp.LT);
        assertOp(lte(pk, param(pk, value(0))), ComparisonOp.LTE);
    }

    private void assertOp(Predicate p, ComparisonOp expected)
    {
        assertTrue(p instanceof Predicate.Comparison);
        assertEquals(expected, ((Predicate.Comparison) p).op());
    }

    @Test
    public void testAnd()
    {
        Predicate p1 = eq(pk, param(pk, value(0)));
        Predicate p2 = gt(v0, param(v0, value(1)));
        Predicate combined = and(p1, p2);

        assertTrue(combined instanceof Predicate.And);
        Predicate.And a = (Predicate.And) combined;
        assertEquals(2, a.children().size());
        assertSame(p1, a.children().get(0));
        assertSame(p2, a.children().get(1));
    }

    @Test
    public void testAndSingleChild()
    {
        Predicate p1 = eq(pk, param(pk, value(0)));
        Predicate combined = and(p1);

        assertTrue(combined instanceof Predicate.And);
        assertEquals(1, ((Predicate.And) combined).children().size());
    }

    @Test
    public void testOr()
    {
        Predicate p1 = eq(v0, param(v0, value(0)));
        Predicate p2 = eq(v1, param(v1, value(1)));
        Predicate combined = or(p1, p2);

        assertTrue(combined instanceof Predicate.Or);
        assertEquals(2, ((Predicate.Or) combined).children().size());
    }

    @Test
    public void testNot()
    {
        Predicate inner = eq(pk, param(pk, value(0)));
        Predicate negated = not(inner);

        assertTrue(negated instanceof Predicate.Not);
        assertSame(inner, ((Predicate.Not) negated).child());
    }

    @Test
    public void testIsNull()
    {
        Predicate p = isNull(v0);
        assertTrue(p instanceof Predicate.IsNull);
        assertEquals(v0, ((Predicate.IsNull) p).column().column());
    }

    @Test
    public void testIsNotNull()
    {
        Predicate p = isNotNull(v0);
        assertTrue(p instanceof Predicate.IsNotNull);
        assertEquals(v0, ((Predicate.IsNotNull) p).column().column());
    }

    @Test
    public void testAll()
    {
        Predicate p = all();
        assertTrue(p instanceof Predicate.All);
    }

    @Test
    public void testNestedComposition()
    {
        // (v0 = ? AND v1 > ?) OR (NOT (pk IS NULL))
        Predicate eqP = eq(v0, param(v0, value(0)));
        Predicate gtP = gt(v1, param(v1, value(1)));
        Predicate isNullP = isNull(pk);

        Predicate nested = or(
            and(eqP, gtP),
            not(isNullP)
        );

        assertTrue(nested instanceof Predicate.Or);
        Predicate.Or orP = (Predicate.Or) nested;
        assertEquals(2, orP.children().size());

        assertTrue(orP.children().get(0) instanceof Predicate.And);
        Predicate.And andP = (Predicate.And) orP.children().get(0);
        assertEquals(2, andP.children().size());

        assertTrue(orP.children().get(1) instanceof Predicate.Not);
        Predicate.Not notP = (Predicate.Not) orP.children().get(1);
        assertTrue(notP.child() instanceof Predicate.IsNull);
    }

    @Test
    public void testExpressionFactories()
    {
        Expression.ColumnRef colRef = col(pk);
        assertEquals("pk", colRef.column().name);

        Expression.Param p = param(pk, value(5));
        assertEquals("pk", p.column().name);
        assertEquals(5, p.valueIndex().getValue());

        Expression.Param p2 = param(pk, value(3));
        assertEquals(pk, p2.column());

        Expression.NullLiteral nl = nullLiteral();
        assertNotNull(nl);
    }
}
