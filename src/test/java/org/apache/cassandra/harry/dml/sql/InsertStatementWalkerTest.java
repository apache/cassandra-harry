package org.apache.cassandra.harry.dml.sql;

import org.junit.Before;
import org.junit.Test;

import static org.apache.cassandra.harry.dml.sql.DslPrelude.*;
import static org.junit.Assert.*;

public class InsertStatementWalkerTest
{
    private TableSpec spec;

    @Before
    public void setUp()
    {
        spec = builder("public", "ins_walker_test")
            .column("pk", int64Type, pk())
            .column("v0", int32Type, population(20))
            .column("v1", asciiType, population(15))
            .column("v2", floatType, population(10))
            .build();
    }

    @Test
    public void testDeterministic()
    {
        InsertStatementWalker walker = new InsertStatementWalker(spec, 0.3);

        RenderedStatement r1 = SqlRenderer.render(walker.generate(123L));
        RenderedStatement r2 = SqlRenderer.render(walker.generate(123L));

        assertEquals(r1.sql(), r2.sql());
        assertEquals(r1.bindings(), r2.bindings());
    }

    @Test
    public void testDifferentSeedsDifferentOutput()
    {
        InsertStatementWalker walker = new InsertStatementWalker(spec, 0.3);

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
    public void testAlwaysIncludesPkColumns()
    {
        InsertStatementWalker walker = new InsertStatementWalker(spec, 0.5);
        for (long seed = 0; seed < 200; seed++)
        {
            InsertStatement stmt = walker.generate(seed);
            InsertValue[] vals = stmt.values();

            // Find PK column names in values
            boolean foundPk = false;
            for (InsertValue v : vals)
            {
                if (v.column().column().name.equals("pk"))
                    foundPk = true;
            }
            assertTrue("PK column must always be included (seed=" + seed + ")", foundPk);
        }
    }

    @Test
    public void testAlwaysHasAtLeastOneRegularColumn()
    {
        InsertStatementWalker walker = new InsertStatementWalker(spec, 0.0);
        for (long seed = 0; seed < 100; seed++)
        {
            InsertStatement stmt = walker.generate(seed);
            // With PK + at least 1 regular column, minimum is 2
            assertTrue("Must have at least 2 values (pk + 1 regular)",
                       stmt.values().length >= 2);
        }
    }

    @Test
    public void testLowForkProbabilityProducesMinimalInserts()
    {
        InsertStatementWalker walker = new InsertStatementWalker(spec, 0.0);
        for (long seed = 0; seed < 100; seed++)
        {
            InsertStatement stmt = walker.generate(seed);
            // 1 PK + 1 regular column = 2
            assertEquals("Expected exactly 2 values at fork=0 (seed=" + seed + ")",
                         2, stmt.values().length);
        }
    }

    @Test
    public void testHighForkProbabilityProducesMultiColumnInserts()
    {
        InsertStatementWalker walker = new InsertStatementWalker(spec, 0.9);
        int multiColCount = 0;
        for (long seed = 0; seed < 100; seed++)
        {
            InsertStatement stmt = walker.generate(seed);
            if (stmt.values().length > 2)
                multiColCount++;
        }
        assertTrue("Expected some multi-column inserts at fork=0.9, got " + multiColCount,
                   multiColCount > 20);
    }

    @Test
    public void testNoInsertDuplicateColumns()
    {
        InsertStatementWalker walker = new InsertStatementWalker(spec, 0.9);
        for (long seed = 0; seed < 200; seed++)
        {
            InsertStatement stmt = walker.generate(seed);
            java.util.Set<String> seen = new java.util.HashSet<>();
            for (InsertValue v : stmt.values())
            {
                String colName = v.column().column().name;
                assertTrue("Duplicate column in insert: " + colName +
                           " (seed=" + seed + ")", seen.add(colName));
            }
        }
    }

    @Test
    public void testAllExpressionTypesAppear()
    {
        InsertStatementWalker walker = new InsertStatementWalker(spec, 0.5);
        boolean sawParam = false;
        boolean sawInlineParam = false;
        boolean sawNull = false;
        for (long seed = 0; seed < 500; seed++)
        {
            InsertStatement stmt = walker.generate(seed);
            for (InsertValue v : stmt.values())
            {
                if (v.value() instanceof Expression.Param) sawParam = true;
                if (v.value() instanceof Expression.InlineParam) sawInlineParam = true;
                if (v.value() instanceof Expression.NullLiteral) sawNull = true;
            }
        }
        assertTrue("Expected Param expressions", sawParam);
        assertTrue("Expected InlineParam expressions", sawInlineParam);
        assertTrue("Expected NullLiteral expressions", sawNull);
    }

    @Test
    public void testPkColumnsNeverGetNull()
    {
        InsertStatementWalker walker = new InsertStatementWalker(spec, 0.5);
        for (long seed = 0; seed < 500; seed++)
        {
            InsertStatement stmt = walker.generate(seed);
            for (InsertValue v : stmt.values())
            {
                if (v.column().column().isPk)
                {
                    assertFalse("PK column must never be NULL (seed=" + seed + ")",
                                v.value() instanceof Expression.NullLiteral);
                }
            }
        }
    }

    @Test
    public void testRendersValidSql()
    {
        InsertStatementWalker walker = new InsertStatementWalker(spec, 0.5);
        for (long seed = 0; seed < 200; seed++)
        {
            InsertStatement stmt = walker.generate(seed);
            RenderedStatement rendered = SqlRenderer.render(stmt);
            String sql = rendered.sql();
            assertTrue("Must start with INSERT INTO",
                       sql.startsWith("INSERT INTO public.ins_walker_test"));
            assertTrue("Must contain VALUES", sql.contains("VALUES"));
        }
    }

    @Test
    public void testCustomWeightsNullHeavy()
    {
        InsertStatementWalker.Weights w = InsertStatementWalker.Weights.builder()
            .forkProbability(0.3)
            .valParam(0)
            .valInlineParam(0)
            .valNull(100)
            .build();

        InsertStatementWalker walker = new InsertStatementWalker(spec, w);
        for (long seed = 0; seed < 100; seed++)
        {
            InsertStatement stmt = walker.generate(seed);
            for (InsertValue v : stmt.values())
            {
                if (!v.column().column().isPk)
                    assertTrue("Expected NullLiteral with valNull=100, got " +
                               v.value().getClass().getSimpleName(),
                               v.value() instanceof Expression.NullLiteral);
            }
        }
    }

    @Test
    public void testCustomWeightsOnlyInlineParam()
    {
        InsertStatementWalker.Weights w = InsertStatementWalker.Weights.builder()
            .forkProbability(0.0)
            .valParam(0)
            .valInlineParam(100)
            .valNull(0)
            .build();

        InsertStatementWalker walker = new InsertStatementWalker(spec, w);
        for (long seed = 0; seed < 100; seed++)
        {
            InsertStatement stmt = walker.generate(seed);
            for (InsertValue v : stmt.values())
                assertTrue("Expected InlineParam with valParam=0, valNull=0",
                           v.value() instanceof Expression.InlineParam);
        }
    }

    @Test
    public void testWeightsDefaults()
    {
        InsertStatementWalker.Weights d = InsertStatementWalker.Weights.defaults();
        assertEquals(70, d.valParam);
        assertEquals(10, d.valInlineParam);
        assertEquals(20, d.valNull);
        assertTrue(d.alwaysIncludePk);
    }
}
