package org.apache.cassandra.harry.dml.sql;

import org.junit.Before;
import org.junit.Test;

import static org.apache.cassandra.harry.dml.sql.DslPrelude.*;
import static org.junit.Assert.*;

public class SelectStatementWalkerTest
{
    private TableSpec spec;

    @Before
    public void setUp()
    {
        spec = builder("public", "walker_select")
            .column("pk", int64Type, pk())
            .column("v0", int32Type, population(20))
            .column("v1", asciiType, population(15))
            .column("v2", floatType, population(10))
            .build();
    }

    @Test
    public void testDeterministic()
    {
        SelectStatementWalker walker = new SelectStatementWalker(spec, 0.3);
        RenderedStatement r1 = SqlRenderer.render(walker.generate(123L));
        RenderedStatement r2 = SqlRenderer.render(walker.generate(123L));
        assertEquals(r1.sql(), r2.sql());
        assertEquals(r1.bindings(), r2.bindings());
    }

    @Test
    public void testDifferentSeedsDifferentOutput()
    {
        SelectStatementWalker walker = new SelectStatementWalker(spec, 0.3);
        int distinct = 0;
        String first = SqlRenderer.render(walker.generate(0L)).sql();
        for (long seed = 1; seed < 50; seed++)
        {
            String sql = SqlRenderer.render(walker.generate(seed)).sql();
            if (!sql.equals(first)) distinct++;
        }
        assertTrue("Expected variation across seeds", distinct > 0);
    }

    @Test
    public void testAlwaysStartsWithSelect()
    {
        SelectStatementWalker walker = new SelectStatementWalker(spec, 0.5);
        for (long seed = 0; seed < 200; seed++)
        {
            String sql = SqlRenderer.render(walker.generate(seed)).sql();
            assertTrue("Must start with SELECT", sql.startsWith("SELECT "));
            assertTrue("Must contain FROM", sql.contains(" FROM "));
        }
    }

    @Test
    public void testLowForkProducesSimple()
    {
        SelectStatementWalker walker = new SelectStatementWalker(spec, 0.0);
        for (long seed = 0; seed < 100; seed++)
        {
            SelectStatement stmt = walker.generate(seed);
            // No compound predicates at fork=0
            Predicate p = stmt.where();
            assertFalse(p instanceof Predicate.And);
            assertFalse(p instanceof Predicate.Or);
            assertFalse(p instanceof Predicate.Not);
            // No multi-column ORDER BY (at most 1 since no forking to add more)
            assertTrue(stmt.orderBy().length <= 1);
        }
    }

    @Test
    public void testHighForkProducesComplex()
    {
        SelectStatementWalker walker = new SelectStatementWalker(spec, 0.8);
        int compoundCount = 0;
        int multiOrderBy = 0;
        int withLimit = 0;
        int subsetProjection = 0;

        for (long seed = 0; seed < 200; seed++)
        {
            SelectStatement stmt = walker.generate(seed);
            if (stmt.where() instanceof Predicate.And || stmt.where() instanceof Predicate.Or
                || stmt.where() instanceof Predicate.Not)
                compoundCount++;
            if (stmt.orderBy().length > 1) multiOrderBy++;
            if (stmt.limit() >= 0) withLimit++;
            if (stmt.projection() instanceof Projection.Items) subsetProjection++;
        }
        assertTrue("Expected compound predicates at fork=0.8", compoundCount > 10);
        assertTrue("Expected multi-column ORDER BY", multiOrderBy > 5);
        assertTrue("Expected some LIMIT", withLimit > 5);
        assertTrue("Expected column subset projections", subsetProjection > 10);
    }

    @Test
    public void testNoProjectionDuplicateColumns()
    {
        SelectStatementWalker walker = new SelectStatementWalker(spec, 0.9);
        for (long seed = 0; seed < 200; seed++)
        {
            SelectStatement stmt = walker.generate(seed);
            if (stmt.projection() instanceof Projection.Items items)
            {
                java.util.Set<String> seen = new java.util.HashSet<>();
                for (SelectItem item : items.items())
                {
                    if (item.expression() instanceof Expression.ColumnRef ref)
                        assertTrue("Duplicate column in projection (seed=" + seed + ")",
                                   seen.add(ref.column().name));
                }
            }
        }
    }

    @Test
    public void testNoOrderByDuplicateColumns()
    {
        SelectStatementWalker walker = new SelectStatementWalker(spec, 0.9);
        for (long seed = 0; seed < 200; seed++)
        {
            SelectStatement stmt = walker.generate(seed);
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (OrderBy ob : stmt.orderBy())
                assertTrue("Duplicate column in ORDER BY (seed=" + seed + ")",
                           seen.add(ob.column().name));
        }
    }

    @Test
    public void testAllProjectionTypesAppear()
    {
        SelectStatementWalker walker = new SelectStatementWalker(spec, 0.5);
        boolean sawWildcard = false;
        boolean sawItems = false;
        for (long seed = 0; seed < 200; seed++)
        {
            Projection p = walker.generate(seed).projection();
            if (p instanceof Projection.Wildcard) sawWildcard = true;
            if (p instanceof Projection.Items) sawItems = true;
        }
        assertTrue("Expected wildcard projection", sawWildcard);
        assertTrue("Expected column subset projection", sawItems);
    }

    @Test
    public void testBothOrderByDirectionsAppear()
    {
        SelectStatementWalker walker = new SelectStatementWalker(spec, 0.5);
        boolean sawAsc = false;
        boolean sawDesc = false;
        for (long seed = 0; seed < 500; seed++)
        {
            for (OrderBy ob : walker.generate(seed).orderBy())
            {
                if (ob.desc()) sawDesc = true;
                else sawAsc = true;
            }
        }
        assertTrue("Expected ASC", sawAsc);
        assertTrue("Expected DESC", sawDesc);
    }

    @Test
    public void testLimitAppearsAndAbsent()
    {
        SelectStatementWalker walker = new SelectStatementWalker(spec, 0.5);
        boolean sawLimit = false;
        boolean sawNoLimit = false;
        for (long seed = 0; seed < 200; seed++)
        {
            int limit = walker.generate(seed).limit();
            if (limit >= 0) sawLimit = true;
            else sawNoLimit = true;
        }
        assertTrue("Expected LIMIT present sometimes", sawLimit);
        assertTrue("Expected LIMIT absent sometimes", sawNoLimit);
    }

    @Test
    public void testCustomWeightsAlwaysWildcard()
    {
        SelectStatementWalker.Weights w = SelectStatementWalker.Weights.builder()
            .projectionWildcard(100).projectionSubset(0)
            .build();
        SelectStatementWalker walker = new SelectStatementWalker(spec, w);
        for (long seed = 0; seed < 100; seed++)
            assertTrue(walker.generate(seed).projection() instanceof Projection.Wildcard);
    }

    @Test
    public void testCustomWeightsAlwaysLimit()
    {
        SelectStatementWalker.Weights w = SelectStatementWalker.Weights.builder()
            .limitNone(0).limitPresent(100)
            .build();
        SelectStatementWalker walker = new SelectStatementWalker(spec, w);
        for (long seed = 0; seed < 100; seed++)
            assertTrue(walker.generate(seed).limit() >= 1);
    }

    @Test
    public void testCustomWeightsNoOrderBy()
    {
        SelectStatementWalker.Weights w = SelectStatementWalker.Weights.builder()
            .orderByNone(100).orderByPresent(0)
            .build();
        SelectStatementWalker walker = new SelectStatementWalker(spec, w);
        for (long seed = 0; seed < 100; seed++)
            assertEquals(0, walker.generate(seed).orderBy().length);
    }
}
