package org.apache.cassandra.harry.dml.sql;

import org.junit.Before;
import org.junit.Test;

import static org.apache.cassandra.harry.dml.sql.DslPrelude.*;
import static org.junit.Assert.*;

public class UpdateStatementWalkerTest
{
    private TableSpec spec;

    @Before
    public void setUp()
    {
        spec = builder("public", "walker_test")
            .column("pk", int64Type, pk())
            .column("v0", int32Type, population(20))
            .column("v1", asciiType, population(15))
            .column("v2", floatType, population(10))
            .build();
    }

    @Test
    public void testDeterministic()
    {
        UpdateStatementWalker walker = new UpdateStatementWalker(spec, 0.3);

        RenderedStatement r1 = SqlRenderer.render(walker.generate(123L));
        RenderedStatement r2 = SqlRenderer.render(walker.generate(123L));

        assertEquals(r1.sql(), r2.sql());
        assertEquals(r1.bindings(), r2.bindings());
    }

    @Test
    public void testDifferentSeedsDifferentOutput()
    {
        UpdateStatementWalker walker = new UpdateStatementWalker(spec, 0.3);

        // With enough seeds, we should get at least some variation
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
    public void testAlwaysHasAtLeastOneAssignment()
    {
        UpdateStatementWalker walker = new UpdateStatementWalker(spec, 0.0);
        for (long seed = 0; seed < 100; seed++)
        {
            UpdateStatement stmt = walker.generate(seed);
            assertTrue("Must have at least one assignment",
                       stmt.assignments().length >= 1);
        }
    }

    @Test
    public void testLowForkProbabilityProducesSimpleStatements()
    {
        UpdateStatementWalker walker = new UpdateStatementWalker(spec, 0.0);
        for (long seed = 0; seed < 100; seed++)
        {
            UpdateStatement stmt = walker.generate(seed);
            // With forkProbability=0, predicates should always be leaf or All
            Predicate where = stmt.where();
            assertTrue("Expected leaf or All predicate at fork=0, got: " + where.getClass().getSimpleName(),
                       where instanceof Predicate.Comparison ||
                       where instanceof Predicate.IsNull ||
                       where instanceof Predicate.IsNotNull ||
                       where instanceof Predicate.All);
            // Only one assignment (no forking to add more)
            assertEquals(1, stmt.assignments().length);
        }
    }

    @Test
    public void testHighForkProbabilityProducesCompoundPredicates()
    {
        UpdateStatementWalker walker = new UpdateStatementWalker(spec, 0.8);
        int compoundCount = 0;
        int multiAssignCount = 0;
        for (long seed = 0; seed < 100; seed++)
        {
            UpdateStatement stmt = walker.generate(seed);
            Predicate where = stmt.where();
            if (where instanceof Predicate.And || where instanceof Predicate.Or ||
                where instanceof Predicate.Not)
                compoundCount++;
            if (stmt.assignments().length > 1)
                multiAssignCount++;
        }
        assertTrue("Expected some compound predicates at fork=0.8, got " + compoundCount,
                   compoundCount > 20);
        assertTrue("Expected some multi-assignment statements at fork=0.8, got " + multiAssignCount,
                   multiAssignCount > 10);
    }

    @Test
    public void testRendersValidSql()
    {
        UpdateStatementWalker walker = new UpdateStatementWalker(spec, 0.5);
        for (long seed = 0; seed < 200; seed++)
        {
            UpdateStatement stmt = walker.generate(seed);
            RenderedStatement rendered = SqlRenderer.render(stmt);
            String sql = rendered.sql();
            assertTrue("Must start with UPDATE", sql.startsWith("UPDATE public.walker_test SET "));
            // Must have at least one SET assignment
            assertTrue("Must have SET clause", sql.contains(" SET "));
            // Assignments should contain '=' for column = value
            String afterSet;
            if (sql.contains(" WHERE "))
                afterSet = sql.substring(sql.indexOf(" SET ") + 5, sql.indexOf(" WHERE "));
            else
                afterSet = sql.substring(sql.indexOf(" SET ") + 5);
            assertTrue("SET clause must contain assignment with =", afterSet.contains("="));
        }
    }

    @Test
    public void testNoAssignmentDuplicateColumns()
    {
        UpdateStatementWalker walker = new UpdateStatementWalker(spec, 0.9);
        for (long seed = 0; seed < 200; seed++)
        {
            UpdateStatement stmt = walker.generate(seed);
            Assignment[] assignments = stmt.assignments();
            // Check no duplicate columns in assignments
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (Assignment a : assignments)
            {
                String colName = a.column().column().name;
                assertTrue("Duplicate column in assignments: " + colName +
                           " (seed=" + seed + ")", seen.add(colName));
            }
        }
    }

    @Test
    public void testAllExpressionTypesAppear()
    {
        // Over many seeds with moderate fork probability, we should see
        // Param, InlineParam, and NullLiteral in assignments
        UpdateStatementWalker walker = new UpdateStatementWalker(spec, 0.5);
        boolean sawParam = false;
        boolean sawInlineParam = false;
        boolean sawNull = false;
        for (long seed = 0; seed < 500; seed++)
        {
            UpdateStatement stmt = walker.generate(seed);
            for (Assignment a : stmt.assignments())
            {
                if (a.value() instanceof Expression.Param) sawParam = true;
                if (a.value() instanceof Expression.InlineParam) sawInlineParam = true;
                if (a.value() instanceof Expression.NullLiteral) sawNull = true;
            }
        }
        assertTrue("Expected Param expressions", sawParam);
        assertTrue("Expected InlineParam expressions", sawInlineParam);
        assertTrue("Expected NullLiteral expressions", sawNull);
    }

    @Test
    public void testAllPredicateTypesAppear()
    {
        UpdateStatementWalker walker = new UpdateStatementWalker(spec, 0.5);
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
    public void testCustomWeightsNullHeavy()
    {
        // Weight SET expressions heavily toward NULL
        UpdateStatementWalker.Weights w = UpdateStatementWalker.Weights.builder()
            .forkProbability(0.3)
            .setParam(0)
            .setInlineParam(0)
            .setNull(100)
            .build();

        UpdateStatementWalker walker = new UpdateStatementWalker(spec, w);
        for (long seed = 0; seed < 100; seed++)
        {
            UpdateStatement stmt = walker.generate(seed);
            for (Assignment a : stmt.assignments())
                assertTrue("Expected NullLiteral with setNull=100, got " + a.value().getClass().getSimpleName(),
                           a.value() instanceof Expression.NullLiteral);
        }
    }

    @Test
    public void testCustomWeightsOnlyInlineParamInWhere()
    {
        // WHERE expressions always inlineParam
        UpdateStatementWalker.Weights w = UpdateStatementWalker.Weights.builder()
            .forkProbability(0.0)
            .whereParam(0)
            .whereInlineParam(100)
            .leafComparison(100)
            .leafIsNull(0)
            .leafIsNotNull(0)
            .leafFuture(0)
            .allPredicatePercent(0)
            .build();

        UpdateStatementWalker walker = new UpdateStatementWalker(spec, w);
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
    public void testCustomWeightsNoCompoundPredicates()
    {
        // forkProbability=0 means no compound predicates and no extra assignments
        UpdateStatementWalker.Weights w = UpdateStatementWalker.Weights.builder()
            .forkProbability(0.0)
            .allPredicatePercent(0)
            .build();

        UpdateStatementWalker walker = new UpdateStatementWalker(spec, w);
        for (long seed = 0; seed < 100; seed++)
        {
            UpdateStatement stmt = walker.generate(seed);
            assertEquals(1, stmt.assignments().length);
            Predicate p = stmt.where();
            assertFalse("No compound at fork=0", p instanceof Predicate.And);
            assertFalse("No compound at fork=0", p instanceof Predicate.Or);
            assertFalse("No compound at fork=0", p instanceof Predicate.Not);
        }
    }

    @Test
    public void testCustomWeightsHighAllPercent()
    {
        // 100% All (no WHERE)
        UpdateStatementWalker.Weights w = UpdateStatementWalker.Weights.builder()
            .allPredicatePercent(100)
            .build();

        UpdateStatementWalker walker = new UpdateStatementWalker(spec, w);
        for (long seed = 0; seed < 100; seed++)
            assertTrue("Expected All at allPredicatePercent=100",
                       walker.generate(seed).where() instanceof Predicate.All);
    }

    @Test
    public void testWeightsDefaults()
    {
        UpdateStatementWalker.Weights d = UpdateStatementWalker.Weights.defaults();
        assertEquals(70, d.setParam);
        assertEquals(10, d.setInlineParam);
        assertEquals(20, d.setNull);
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

    @Test
    public void testAllComparisonOpsAppear()
    {
        UpdateStatementWalker walker = new UpdateStatementWalker(spec, 0.5);
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
}
