package harry.model;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.SchemaGenerators;
import harry.ddl.SchemaSpec;
import harry.generators.distribution.Distribution;
import harry.operations.CompiledStatement;
import harry.operations.Query;
import harry.operations.Relation;
import harry.visitors.LoggingPartitionVisitor;
import harry.visitors.MutatingRowVisitor;
import harry.visitors.PartitionVisitor;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

public class TestEveryClustering extends IntegrationTestBase
{
    int CYCLES = 1000;

    @Test
    public void basicQuerySelectorTest()
    {
        Supplier<SchemaSpec> schemaGen = SchemaGenerators.progression(SchemaGenerators.DEFAULT_SWITCH_AFTER);
        for (int cnt = 0; cnt < Integer.MAX_VALUE; cnt++)
        {
            beforeEach();
            SchemaSpec schemaSpec = schemaGen.get();

            System.out.println(schemaSpec.compile().cql());
            int partitionSize = 1000;

            Configuration config = sharedConfiguration(cnt, schemaSpec)
                                   .setPartitionDescriptorSelector(new Configuration.DefaultPDSelectorConfiguration(1, partitionSize))
                                   .setClusteringDescriptorSelector(sharedCDSelectorConfiguration()
                                                                    .setNumberOfModificationsDistribution(() -> new Distribution.ConstantDistribution(1L))
                                                                    .setRowsPerModificationDistribution(() -> new Distribution.ConstantDistribution(1L))
                                                                    .setMaxPartitionSize(250)
                                                                    .build())
                                   .build();

            Run run = config.createRun();
            run.sut.schemaChange(run.schemaSpec.compile().cql());
            OpSelectors.MonotonicClock clock = run.clock;

            Set<Long> visitedCds = new HashSet<>();
            PartitionVisitor partitionVisitor = new LoggingPartitionVisitor(run, (r) -> {
                return new MutatingRowVisitor(r) {
                    public CompiledStatement perform(OpSelectors.OperationKind op, long lts, long pd, long cd, long opId)
                    {
                        visitedCds.add(cd);
                        return super.perform(op, lts, pd, cd, opId);
                    }
                };
            });
            sut.cluster().stream().forEach((IInvokableInstance node) -> node.nodetool("disableautocompaction"));
            for (int i = 0; i < CYCLES; i++)
            {
                long lts = clock.nextLts();
                partitionVisitor.visitPartition(lts);

                if (i > 0 && i % 250 == 0)
                    sut.cluster().stream().forEach((IInvokableInstance node) -> node.nodetool("flush", schemaSpec.keyspace, schemaSpec.table));
            }

            for (Long cd : visitedCds)
            {
                Query query = new Query.SingleClusteringQuery(Query.QueryKind.SINGLE_CLUSTERING,
                                                              run.pdSelector.pd(0),
                                                              cd,
                                                              false,
                                                              Relation.eqRelations(run.schemaSpec.ckGenerator.slice(cd), run.schemaSpec.clusteringKeys),
                                                              run.schemaSpec);
                Model model = new QuiescentChecker(run);
                model.validate(query);
            }
        }
    }
}
