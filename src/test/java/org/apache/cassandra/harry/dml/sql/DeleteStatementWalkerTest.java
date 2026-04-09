package org.apache.cassandra.harry.dml.sql;

import org.junit.Before;
import org.junit.Test;

import static org.apache.cassandra.harry.dml.sql.DslPrelude.*;
import static org.junit.Assert.*;

public class DeleteStatementWalkerTest
{
    private TableSpec spec;

    @Before
    public void setUp()
    {
        spec = builder("public", "del_walker_test")
            .column("pk", int64Type, pk())
            .column("v0", int32Type, population(20))
            .column("v1", asciiType, population(15))
            .column("v2", floatType, population(10))
            .build();
    }

    @Test
    public void testDeterministic()
    {
        DeleteStatementWalker walker = new DeleteStatementWalker(spec, 0.3);

        RenderedStatement r1 = SqlRenderer.render(walker.generate(123L));
        RenderedStatement r2 = SqlRenderer.render(walker.generate(123L));

        assertEquals(r1.sql(), r2.sql());
        assertEquals(r1.bindings(), r2.bindings());
    }

    @Test
    public void testDifferentSeedsDifferentOutput()
    {
        DeleteStatementWalker walker = new DeleteStatementWalker(spec, 0.3);

        int distinct = 0;
        String first = SqlRenderer.render(walker.generate(0L)).sql();
        for (long seed = 1; seed < 50; seed++)
        {
            String sql = SqlRenderer.render(walker.generate(seed)).sql();
            if (!sql.equals(first))
                distinct++;
        }
        assertTrue("Expected variation across seeds, got none", distinct > 0);
    }

    @Test
    public void testLowForkProbabilityProducesSimpleStatements()
    {
        DeleteStatementWalker walker = new DeleteStatementWalker(spec, 0.0);
        for (long seed = 0; seed < 100; seed++)
        {
            DeleteStatement stmt = walker.generate(seed);
            Predicate where = stmt.where();
            assertTrue("Expected leaf or All predicate at fork=0, got: " + where.getClass().getSimpleName(),
                       where instanceof Predicate.Comparison ||
                       where instanceof Predicate.IsNull ||
                       where instanceof Predicate.IsNotNull ||
                       where instanceof Predicate.All);
        }
    }

    @Test
    public void testHighForkProbabilityProducesCompoundPredicates()
    {
        DeleteStatementWalker walker = new DeleteStatementWalker(spec, 0.8);
        int compoundCount = 0;
        for (long seed = 0; seed < 100; seed++)
        {
            Predicate where = walker.generate(seed).where();
            if (where instanceof Predicate.And || where instanceof Predicate.Or ||
                where instanceof Predicate.Not)
                compoundCount++;
        }
        assertTrue("Expected some compound predicates at fork=0.8, got " + compoundCount,
                   compoundCount > 20);
    }

    @Test
    public void testRendersValidSql()
    {
        DeleteStatementWalker walker = new DeleteStatementWalker(spec, 0.5);
        for (long seed = 0; seed < 200; seed++)
        {
            DeleteStatement stmt = walker.generate(seed);
            RenderedStatement rendered = SqlRenderer.render(stmt);
            String sql = rendered.sql();
            assertTrue("Must start with DELETE FROM", sql.startsWith("DELETE FROM public.del_walker_test"));
        }
    }

    @Test
    public void testAllPredicateTypesAppear()
    {
        DeleteStatementWalker walker = new DeleteStatementWalker(spec, 0.5);
        boolean[] seen = new boolean[7]; // comparison, isNull, isNotNull, and, or, not, all
        for (long seed = 0; seed < 1000; seed++)
        {
            markPredicateTypes(walker.generate(seed).where(), seen);
        }
        assertTrue("Expected Comparison predicates", seen[0]);
        assertTrue("Expected IsNull predicates", seen[1]);
        assertTrue("Expected IsNotNull predicates", seen[2]);
        assertTrue("Expected And predicates", seen[3]);
        assertTrue("Expected Or predicates", seen[4]);
        assertTrue("Expected Not predicates", seen[5]);
        assertTrue("Expected All predicates", seen[6]);
    }

    private void markPredicateTypes(Predicate p, boolean[] seen)
    {
        if (p instanceof Predicate.Comparison) seen[0] = true;
        else if (p instanceof Predicate.IsNull) seen[1] = true;
        else if (p instanceof Predicate.IsNotNull) seen[2] = true;
        else if (p instanceof Predicate.And a)
        {
            seen[3] = true;
            for (Predicate child : a.children()) markPredicateTypes(child, seen);
        }
        else if (p instanceof Predicate.Or o)
        {
            seen[4] = true;
            for (Predicate child : o.children()) markPredicateTypes(child, seen);
        }
        else if (p instanceof Predicate.Not n)
        {
            seen[5] = true;
            markPredicateTypes(n.child(), seen);
        }
        else if (p instanceof Predicate.All) seen[6] = true;
    }

    @Test
    public void testAllComparisonOpsAppear()
    {
        DeleteStatementWalker walker = new DeleteStatementWalker(spec, 0.5);
        boolean[] seenOps = new boolean[ComparisonOp.values().length];

        for (long seed = 0; seed < 500; seed++)
        {
            markComparisonOps(walker.generate(seed).where(), seenOps);
        }

        for (ComparisonOp op : ComparisonOp.values())
            assertTrue("Expected op " + op + " to appear", seenOps[op.ordinal()]);
    }

    private void markComparisonOps(Predicate p, boolean[] seenOps)
    {
        if (p instanceof Predicate.Comparison c)
            seenOps[c.op().ordinal()] = true;
        else if (p instanceof Predicate.And a)
            for (Predicate child : a.children()) markComparisonOps(child, seenOps);
        else if (p instanceof Predicate.Or o)
            for (Predicate child : o.children()) markComparisonOps(child, seenOps);
        else if (p instanceof Predicate.Not n)
            markComparisonOps(n.child(), seenOps);
    }

    @Test
    public void testCustomWeightsOnlyInlineParamInWhere()
    {
        DeleteStatementWalker.Weights w = DeleteStatementWalker.Weights.builder()
            .forkProbability(0.0)
            .whereParam(0)
            .whereInlineParam(100)
            .leafComparison(100)
            .leafIsNull(0)
            .leafIsNotNull(0)
            .leafFuture(0)
            .allPredicatePercent(0)
            .build();

        DeleteStatementWalker walker = new DeleteStatementWalker(spec, w);
        for (long seed = 0; seed < 100; seed++)
        {
            Predicate p = walker.generate(seed).where();
            assertTrue("Expected Comparison at fork=0", p instanceof Predicate.Comparison);
            Predicate.Comparison c = (Predicate.Comparison) p;
            assertTrue("Expected InlineParam in WHERE with whereParam=0",
                       c.value() instanceof Expression.InlineParam);
        }
    }

    @Test
    public void testCustomWeightsHighAllPercent()
    {
        DeleteStatementWalker.Weights w = DeleteStatementWalker.Weights.builder()
            .allPredicatePercent(100)
            .build();

        DeleteStatementWalker walker = new DeleteStatementWalker(spec, w);
        for (long seed = 0; seed < 100; seed++)
            assertTrue("Expected All at allPredicatePercent=100",
                       walker.generate(seed).where() instanceof Predicate.All);
    }

    @Test
    public void testCustomWeightsNoCompoundPredicates()
    {
        DeleteStatementWalker.Weights w = DeleteStatementWalker.Weights.builder()
            .forkProbability(0.0)
            .allPredicatePercent(0)
            .build();

        DeleteStatementWalker walker = new DeleteStatementWalker(spec, w);
        for (long seed = 0; seed < 100; seed++)
        {
            Predicate p = walker.generate(seed).where();
            assertFalse("No compound at fork=0", p instanceof Predicate.And);
            assertFalse("No compound at fork=0", p instanceof Predicate.Or);
            assertFalse("No compound at fork=0", p instanceof Predicate.Not);
        }
    }

    @Test
    public void testWeightsDefaults()
    {
        DeleteStatementWalker.Weights d = DeleteStatementWalker.Weights.defaults();
        assertEquals(80, d.whereParam);
        assertEquals(20, d.whereInlineParam);
        assertEquals(5, d.allPredicatePercent);
        assertEquals(4, d.maxPredicateDepth);
        assertEquals(60, d.leafComparison);
        assertEquals(15, d.leafIsNull);
        assertEquals(15, d.leafIsNotNull);
        assertEquals(10, d.leafFuture);
        assertEquals(45, d.compoundAnd);
        assertEquals(35, d.compoundOr);
        assertEquals(20, d.compoundNot);
    }
}
