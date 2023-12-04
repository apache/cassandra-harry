package harry.operations;

import java.util.List;

import harry.ddl.SchemaSpec;
import harry.reconciler.Reconciler;
import harry.util.Ranges;

public class FilteringQuery extends Query
{
    public FilteringQuery(long pd,
                          boolean reverse,
                          List<Relation> relations,
                          SchemaSpec schemaSpec)
    {
        super(QueryKind.SINGLE_PARTITION, pd, reverse, relations, schemaSpec);
    }

    public boolean match(Reconciler.RowState rowState)
    {
        for (Relation relation : relations)
        {
            switch (relation.columnSpec.kind)
            {
                case CLUSTERING:
                    if (!matchCd(rowState.cd))
                        return false;
                    break;
                case REGULAR:
                    if (!relation.match(rowState.vds[relation.columnSpec.getColumnIndex()]))
                        return false;
                    break;
                case STATIC:
                    if (!relation.match(rowState.partitionState.staticRow().vds[relation.columnSpec.getColumnIndex()]))
                        return false;
                    break;
                case PARTITION_KEY:
                    if (!relation.match(rowState.partitionState.pd))
                        return false;
                    break;
            }
        }
        return true;
    }

    public Ranges.Range toRange(long ts)
    {
        throw new IllegalStateException("not implemented for filtering query");
    }
}
