package harry.visitors;

import java.util.Set;

public class SkippingPartitionVisitor extends AbstractPartitionVisitor
{
    private final AbstractPartitionVisitor delegate;
    private final Set<Long> ltsToSkip;
    private final Set<Long> pdsToSkip;

    public SkippingPartitionVisitor(AbstractPartitionVisitor delegate,
                                    Set<Long> ltsToSkip,
                                    Set<Long> pdsToSkip)
    {
        super(delegate);
        this.delegate = delegate;
        this.ltsToSkip = ltsToSkip;
        this.pdsToSkip = pdsToSkip;
    }

    protected void beforeLts(long lts, long pd)
    {
        delegate.beforeLts(lts, pd);
    }

    protected void afterLts(long lts, long pd)
    {
        delegate.afterLts(lts, pd);
    }

    protected void beforeBatch(long lts, long pd, long m)
    {
        delegate.beforeBatch(lts, pd, m);
    }

    protected void operation(long lts, long pd, long cd, long m, long opId)
    {
        if (pdsToSkip.contains(pd) || ltsToSkip.contains(lts))
            return;

        delegate.operation(lts, pd, cd, m, opId);
    }

    protected void afterBatch(long lts, long pd, long m)
    {
        delegate.afterBatch(lts, pd, m);
    }

    public void shutdown() throws InterruptedException
    {
        delegate.shutdown();
    }
}
