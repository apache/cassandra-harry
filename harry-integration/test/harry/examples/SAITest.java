package harry.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;

import harry.ddl.ColumnSpec;
import harry.ddl.SchemaSpec;
import harry.dsl.ReplayingHistoryBuilder;
import harry.generators.DataGenerators;
import harry.generators.EntropySource;
import harry.generators.JdkRandomEntropySource;
import harry.model.IntegrationTestBase;
import harry.model.QuiescentChecker;
import harry.model.SelectHelper;
import harry.operations.FilteringQuery;
import harry.operations.Query;
import harry.operations.Relation;
import harry.reconciler.Reconciler;
import harry.runner.DataTracker;
import harry.runner.DefaultDataTracker;

@Ignore
public class SAITest extends IntegrationTestBase
{
    long seed = 1;

    @Test
    public void basicSaiTest() throws Throwable
    {
        SchemaSpec schema = new SchemaSpec(KEYSPACE, "tbl1",
                                           Arrays.asList(ColumnSpec.ck("pk1", ColumnSpec.int64Type),
                                                         ColumnSpec.ck("pk2", ColumnSpec.asciiType(4, 100)),
                                                         ColumnSpec.ck("pk3", ColumnSpec.int64Type)),
                                           Arrays.asList(ColumnSpec.ck("ck1", ColumnSpec.asciiType(4, 100)),
                                                         ColumnSpec.ck("ck2", ColumnSpec.asciiType, true),
                                                         ColumnSpec.ck("ck3", ColumnSpec.int64Type)
                                                         ),
                                           Arrays.asList(ColumnSpec.regularColumn("v1", ColumnSpec.asciiType(40, 100)),
                                                         ColumnSpec.regularColumn("v2", ColumnSpec.int64Type),
                                                         ColumnSpec.regularColumn("v3", ColumnSpec.int64Type)),
                                           Arrays.asList());

        beforeEach();

        sut.schemaChange(schema.compile().cql());
        sut.schemaChange(schema.cloneWithName(schema.keyspace, schema.table + "_debug").compile().cql());
        sut.schemaChange(String.format("CREATE CUSTOM INDEX %s_sai_idx ON %s.%s (%s) " +
                                       "USING 'StorageAttachedIndex' " +
                                       "WITH OPTIONS = {'case_sensitive': 'false', 'normalize': 'true', 'ascii': 'true'}; ;",
                                       schema.regularColumns.get(0).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.regularColumns.get(0).name));
        sut.schemaChange(String.format("CREATE CUSTOM INDEX %s_sai_idx ON %s.%s (%s) " +
                                       "USING 'StorageAttachedIndex';",
                                       schema.regularColumns.get(1).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.regularColumns.get(1).name));
        sut.schemaChange(String.format("CREATE CUSTOM INDEX %s_sai_idx ON %s.%s (%s) " +
                                       "USING 'StorageAttachedIndex';",
                                       schema.regularColumns.get(2).name,
                                       schema.keyspace,
                                       schema.table,
                                       schema.regularColumns.get(2).name));

//        cluster.disableAutoCompaction(schema.keyspace);

        int maxPartitionSize = 10_000;
        DataTracker tracker = new DefaultDataTracker();
        ReplayingHistoryBuilder history = new ReplayingHistoryBuilder(seed, maxPartitionSize, 10_000, schema, tracker, sut);
        for (int p = 0; p < 100; p++)
        {
            EntropySource entropySource = new JdkRandomEntropySource(p);
            long[] values = new long[5];
            for (int i = 0; i < values.length; i++)
                values[i] = entropySource.next();
            for (int i = 0; i < 100_000; i++)
            {
                int partition = entropySource.nextInt(0, 100);
                System.out.println(i);
                history.visitPartition(partition)
                       .insert(entropySource.nextInt(maxPartitionSize),
                               new long[]{ entropySource.nextBoolean() ? DataGenerators.UNSET_DESCR : values[entropySource.nextInt(values.length)],
                                           entropySource.nextBoolean() ? DataGenerators.UNSET_DESCR : values[entropySource.nextInt(values.length)],
                                           entropySource.nextBoolean() ? DataGenerators.UNSET_DESCR : values[entropySource.nextInt(values.length)] });

                if (entropySource.nextFloat() > 0.99f)
                {
                    int row1 = entropySource.nextInt(maxPartitionSize);
                    int row2 = entropySource.nextInt(maxPartitionSize);
                    history.visitPartition(partition).deleteRowRange(Math.min(row1, row2),
                                                                     Math.max(row1, row2),
                                                                     entropySource.nextBoolean(),
                                                                     entropySource.nextBoolean());
                }
                else if (entropySource.nextFloat() > 0.999f)
                {
                    history.visitPartition(partition).deleteRowSlice();
                }

                if (entropySource.nextFloat() > 0.995f)
                {
                    history.visitPartition(partition).deleteColumns();
                }

                if (entropySource.nextFloat() > 0.9995f)
                {
                    new Thread(() -> cluster.get(1).nodetool("flush", schema.keyspace, schema.table)).start();
                }

                if (entropySource.nextFloat() > 0.9995f)
                    history.visitPartition(partition).deletePartition();

                if (i % 1000 != 0)
                    continue;

                for (int j = 0; j < 10; j++)
                {
                    List<Relation> relations = new ArrayList<>();

                    int num = entropySource.nextInt(1, 5);

                    List<List<Relation.RelationKind>> pick = new ArrayList<>();
                    pick.add(new ArrayList<>(Arrays.asList(Relation.RelationKind.EQ)));
                    pick.add(new ArrayList<>(Arrays.asList(Relation.RelationKind.EQ, Relation.RelationKind.GT, Relation.RelationKind.LT)));
                    pick.add(new ArrayList<>(Arrays.asList(Relation.RelationKind.EQ, Relation.RelationKind.GT, Relation.RelationKind.LT)));

                    if (entropySource.nextBoolean())
                    {
                        relations.addAll(Query.clusteringSliceQuery(schema, partition,
                                                                    entropySource.next(),
                                                                    entropySource.next(),
                                                                    entropySource.nextBoolean(),
                                                                    entropySource.nextBoolean(),
                                                                    false).relations);
                    }

                    for (int k = 0; k < num; k++)
                    {
                        int column = entropySource.nextInt(schema.regularColumns.size());
                        Relation.RelationKind relationKind = null;
;
                        if (!pick.get(column).isEmpty())
                        {
                            List<Relation.RelationKind> possible = pick.get(column);
                            int chosen = entropySource.nextInt(possible.size());
                            relationKind = possible.remove(chosen);
                            if (relationKind == Relation.RelationKind.EQ)
                                possible.clear(); // EQ precludes LT and GT
                            else
                                possible.remove(Relation.RelationKind.EQ); // LT GT preclude EQ
                        }

                        if (relationKind != null)
                        {
                            relations.add(Relation.relation(relationKind,
                                                            schema.regularColumns.get(column),
                                                            values[entropySource.nextInt(values.length)]));
                        }
                    }

                    long pd = history.presetSelector.pdAtPosition(partition);
                    FilteringQuery query = new FilteringQuery(pd,
                                                              false,
                                                              relations,
                                                              schema);


                    Reconciler reconciler = new Reconciler(history.presetSelector,
                                                           schema,
                                                           history::visitor);
                    Set<ColumnSpec<?>> columns = new HashSet<>();
                    columns.addAll(schema.allColumns);
                    QuiescentChecker.validate(schema, tracker, columns,
                                              reconciler.inflatePartitionState(pd, tracker, query).filter(query),
                                              SelectHelper.execute(sut, history.clock(), query),
                                              query);

                    //new AgainstSutChecker;;a().validate();
//                    try
//                    {
//                        new AgainstSutChecker(tracker, history.clock(), sut, schema, schema.cloneWithName(schema.keyspace, schema.table + "_debug"))
//                        .validate(query);
//                    }
//                    catch (IllegalStateException e)
//                    {
//                        e.printStackTrace();
//                    }

                }
            }
        }
    }
}