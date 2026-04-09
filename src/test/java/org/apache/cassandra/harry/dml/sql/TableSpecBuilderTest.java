package org.apache.cassandra.harry.dml.sql;

import java.util.List;

import org.junit.Test;

import static org.apache.cassandra.harry.dml.sql.DslPrelude.*;
import static org.junit.Assert.*;

public class TableSpecBuilderTest
{
    @Test
    public void testBasicBuild()
    {
        TableSpec spec = builder("public", "users")
            .column("id", int64Type, pk())
            .column("name", asciiType)
            .column("age", int32Type)
            .build();

        assertEquals("public", spec.schema());
        assertEquals("users", spec.table());
        assertEquals(3, spec.columns().size());
    }

    @Test
    public void testOrdinalAssignment()
    {
        TableSpec spec = builder("public", "t")
            .column("a", int32Type, pk())
            .column("b", int32Type)
            .column("c", int32Type)
            .build();

        assertEquals(0, spec.column("a").ordinal);
        assertEquals(1, spec.column("b").ordinal);
        assertEquals(2, spec.column("c").ordinal);
    }

    @Test
    public void testColumnLookup()
    {
        TableSpec spec = builder("public", "t")
            .column("pk", int64Type, pk())
            .column("v0", asciiType)
            .column("v1", int32Type)
            .build();

        TableSpec.Column pk = spec.column("pk");
        assertEquals("pk", pk.name);
        assertTrue(pk.isPk);

        TableSpec.Column v0 = spec.column("v0");
        assertEquals("v0", v0.name);
        assertFalse(v0.isPk);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testColumnLookupNotFound()
    {
        TableSpec spec = builder("public", "t")
            .column("pk", int64Type, pk())
            .build();

        spec.column("nonexistent");
    }

    @Test
    public void testPrimaryKeyAndRegularColumns()
    {
        TableSpec spec = builder("public", "t")
            .column("pk1", int64Type, pk())
            .column("pk2", int32Type, pk())
            .column("v0", asciiType)
            .column("v1", int32Type)
            .build();

        List<TableSpec.Column> pks = spec.primaryKey();
        assertEquals(2, pks.size());
        assertEquals("pk1", pks.get(0).name);
        assertEquals("pk2", pks.get(1).name);

        List<TableSpec.Column> regs = spec.regularColumns();
        assertEquals(2, regs.size());
        assertEquals("v0", regs.get(0).name);
        assertEquals("v1", regs.get(1).name);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDuplicateColumnNameRejected()
    {
        builder("public", "t")
            .column("pk", int64Type, pk())
            .column("pk", asciiType)
            .build();
    }

    @Test(expected = IllegalStateException.class)
    public void testNoPrimaryKeyRejected()
    {
        builder("public", "t")
            .column("v0", asciiType)
            .column("v1", int32Type)
            .build();
    }

    @Test
    public void testPopulationOption()
    {
        TableSpec spec = builder("public", "t")
            .column("pk", int64Type, pk())
            .column("score", int32Type, population(50))
            .build();

        // pk has no population override -> uses default (128)
        assertEquals(128, spec.column("pk").population());
        assertEquals(50, spec.column("score").population());
    }

    @Test
    public void testEvolveAddColumn()
    {
        TableSpec v1 = builder("public", "t")
            .column("pk", int64Type, pk())       // ordinal 0
            .column("name", asciiType)            // ordinal 1
            .column("email", asciiType)           // ordinal 2
            .build();

        TableSpec v2 = v1.evolve()
            .addColumn("status", asciiType)       // should get ordinal 3, not 0/1/2
            .build();

        assertEquals(4, v2.columns().size());
        assertEquals(3, v2.column("status").ordinal);
    }

    @Test
    public void testEvolveDropColumn()
    {
        TableSpec v1 = builder("public", "t")
            .column("pk", int64Type, pk())       // ordinal 0
            .column("name", asciiType)            // ordinal 1
            .column("email", asciiType)           // ordinal 2
            .build();

        TableSpec v2 = v1.evolve()
            .dropColumn("email")
            .build();

        assertEquals(2, v2.columns().size());
        assertEquals("pk", v2.columns().get(0).name);
        assertEquals("name", v2.columns().get(1).name);
    }

    @Test
    public void testEvolveOrdinalNotReused()
    {
        TableSpec v1 = builder("public", "t")
            .column("pk", int64Type, pk())       // ordinal 0
            .column("name", asciiType)            // ordinal 1
            .column("email", asciiType)           // ordinal 2
            .build();

        TableSpec v2 = v1.evolve()
            .dropColumn("email")                  // ordinal 2 retired
            .addColumn("status", asciiType)       // should be ordinal 3
            .build();

        assertEquals(3, v2.columns().size());
        assertEquals(3, v2.column("status").ordinal);
        // Ordinal 2 is gone -- no column has it
        for (TableSpec.Column c : v2.columns())
            assertNotEquals(2, c.ordinal);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEvolveDuplicateNameRejected()
    {
        TableSpec v1 = builder("public", "t")
            .column("pk", int64Type, pk())
            .column("name", asciiType)
            .build();

        v1.evolve()
            .addColumn("name", asciiType)
            .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEvolveDropNonexistent()
    {
        TableSpec v1 = builder("public", "t")
            .column("pk", int64Type, pk())
            .build();

        v1.evolve().dropColumn("nonexistent");
    }

    @Test
    public void testCompileCreateTable()
    {
        TableSpec spec = builder("public", "users")
            .column("id", int64Type, pk())
            .column("name", asciiType)
            .column("score", floatType)
            .build();

        String ddl = spec.compileCreateTable();
        assertTrue(ddl.contains("CREATE TABLE public.users"));
        assertTrue(ddl.contains("id bigint NOT NULL"));
        assertTrue(ddl.contains("name text"));
        assertTrue(ddl.contains("score real"));
        assertTrue(ddl.contains("PRIMARY KEY (id)"));
    }

    @Test
    public void testCompileCreateTableCompositePk()
    {
        TableSpec spec = builder("public", "t")
            .column("pk1", int64Type, pk())
            .column("pk2", int32Type, pk())
            .column("v0", asciiType)
            .build();

        String ddl = spec.compileCreateTable();
        assertTrue(ddl.contains("PRIMARY KEY (pk1, pk2)"));
    }

    @Test
    public void testCompileDropTable()
    {
        TableSpec spec = builder("public", "users")
            .column("id", int64Type, pk())
            .build();

        assertEquals("DROP TABLE IF EXISTS public.users", spec.compileDropTable());
    }

    @Test
    public void testCompileDropTableNoSchema()
    {
        TableSpec spec = builder(null, "users")
            .column("id", int64Type, pk())
            .build();

        assertEquals("DROP TABLE IF EXISTS users", spec.compileDropTable());
    }

    @Test
    public void testColIdx()
    {
        TableSpec spec = builder("public", "t")
            .column("pk", int64Type, pk())
            .column("v0", int32Type)
            .build();

        assertEquals(1, spec.column("v0").ordinal);
    }
}
