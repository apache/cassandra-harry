package harry.generators;

import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.ColumnSpec;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.generators.distribution.Distribution;
import harry.model.NoOpChecker;
import harry.model.OpSelectors;
import harry.model.sut.SystemUnderTest;
import harry.visitors.MutatingVisitor;
import harry.visitors.MutatingRowVisitor;
import harry.visitors.SingleValidator;
import harry.util.TestRunner;
import harry.visitors.Visitor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.distributed.impl.RowUtil;
import relocated.shaded.com.google.common.collect.Iterators;

public class DataGeneratorsIntegrationTest extends CQLTester
{
    @Test
    public void testTimestampTieResolution() throws Throwable
    {
        Random rng = new Random(1);
        String ks = "test_timestamp_tie_resolution";
        createKeyspace(String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", ks));
        int counter = 0;
        for (ColumnSpec.DataType<?> dataType : new ColumnSpec.DataType[]{ ColumnSpec.int8Type,
                                                                       ColumnSpec.int16Type,
                                                                       ColumnSpec.int32Type,
                                                                       ColumnSpec.int64Type,
                                                                       ColumnSpec.asciiType,
                                                                       ColumnSpec.floatType,
                                                                       ColumnSpec.doubleType })
        {


            String tbl = "table_" + (counter++);
            createTable(String.format("CREATE TABLE %s.%s (pk int PRIMARY KEY, v %s)",
                                      ks, tbl,
                                      dataType));
            for (int i = 0; i < 10_000; i++)
            {
                long d1 = dataType.generator().adjustEntropyDomain(rng.nextLong());
                long d2 = dataType.generator().adjustEntropyDomain(rng.nextLong());
                for (long d : new long[]{ d1, d2 })
                {
                    execute(String.format("INSERT INTO %s.%s (pk, v) VALUES (?,?) USING TIMESTAMP 1", ks,tbl),
                            i, dataType.generator().inflate(d));
                }

                if (dataType.compareLexicographically(d1, d2) > 0)
                    assertRows(execute(String.format("SELECT v FROM %s.%s WHERE pk=?", ks, tbl), i),
                               row(dataType.generator().inflate(d1)));
                else
                    assertRows(execute(String.format("SELECT v FROM %s.%s WHERE pk=?", ks, tbl), i),
                               row(dataType.generator().inflate(d2)));
            }
        }
    }

    @Test
    public void queryParseabilityTest() throws Throwable
    {
        Generator<SchemaSpec> gen = new SchemaGenerators.Builder(KEYSPACE).partitionKeyColumnCount(2, 4)
                                                                          .clusteringColumnCount(1, 4)
                                                                          .regularColumnCount(1, 4)
                                                                          .staticColumnCount(1, 4)
                                                                          .generator();

        TestRunner.test(gen,
                        (schema) -> {
                            try
                            {
                                schema.validate();
                            }
                            catch (AssertionError e)
                            {
                                return;
                            }
                            createTable(schema.compile().cql());

                            Configuration.ConfigurationBuilder builder = Configuration.fromFile(getClass().getClassLoader().getResource("single_partition_test.yml").getFile())
                                         .unbuild()
                                         .setSchemaProvider(new Configuration.FixedSchemaProviderConfiguration(schema, null, null, null, null))
                                         .setSUT(CqlTesterSut::new);

                            for (OpSelectors.OperationKind opKind : OpSelectors.OperationKind.values())
                            {
                                Run run = builder
                                          .setClusteringDescriptorSelector((rng, schema_) -> {
                                              return new OpSelectors.DefaultDescriptorSelector(rng,
                                                                                               OpSelectors.columnSelectorBuilder().forAll(schema_).build(),
                                                                                               OpSelectors.OperationSelector.weighted(Surjections.weights(100), opKind),
                                                                                               new Distribution.ConstantDistribution(2),
                                                                                               100);
                                          })
                                          .build()
                                          .createRun();

                                Visitor visitor = new MutatingVisitor(run, MutatingRowVisitor::new);
                                for (int lts = 0; lts < 100; lts++)
                                    visitor.visit();
                            }

                            Run run = builder.build()
                                             .createRun();
                            SingleValidator visitor = new SingleValidator(100, run, NoOpChecker::new);
                            for (int lts = 0; lts < 100; lts++)
                                visitor.visit(lts);

                        });

    }

    public class CqlTesterSut implements SystemUnderTest
    {
        public boolean isShutdown()
        {
            return false;
        }

        public void shutdown()
        {
        }

        public void schemaChange(String statement)
        {
            createTable(statement);
        }

        public Object[][] execute(String statement, ConsistencyLevel cl, Object... bindings)
        {
            try
            {
                UntypedResultSet res = DataGeneratorsIntegrationTest.this.execute(statement, bindings);
                if (res == null)
                    return new Object[][] {};

                return Iterators.toArray(RowUtil.toIter(res), Object[].class);
            }
            catch (Throwable throwable)
            {
                throw new RuntimeException(throwable);
            }
        }

        public CompletableFuture<Object[][]> executeAsync(String statement, ConsistencyLevel cl, Object... bindings)
        {
            return CompletableFuture.completedFuture(execute(statement, cl, bindings));
        }
    }
}

