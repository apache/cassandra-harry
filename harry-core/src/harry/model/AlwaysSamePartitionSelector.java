package harry.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Configuration;

/**
 * A simple test-only descriptor selector that can used for testing things where you only need one partition
 */
public class AlwaysSamePartitionSelector extends OpSelectors.PdSelector
{
    private final long pd;

    public AlwaysSamePartitionSelector(long pd)
    {
        this.pd = pd;
    }

    protected long pd(long lts)
    {
        return 0;
    }

    public long nextLts(long lts)
    {
        return lts + 1;
    }

    public long prevLts(long lts)
    {
        return lts - 1;
    }

    public long maxLtsFor(long pd)
    {
        return 1000;
    }

    public long minLtsAt(long position)
    {
        return 0;
    }

    public long minLtsFor(long pd)
    {
        return 0;
    }

    public long positionFor(long lts)
    {
        return 0;
    }

    public long maxPosition(long maxLts)
    {
        return 0;
    }

    @JsonTypeName("always_same")
    public static class AlwaysSamePartitionSelectorConfiguration implements Configuration.PDSelectorConfiguration
    {
        private final long pd;

        public AlwaysSamePartitionSelectorConfiguration(@JsonProperty("pd") long pd)
        {
            this.pd = pd;
        }

        public OpSelectors.PdSelector make(OpSelectors.PureRng rng)
        {
            return new AlwaysSamePartitionSelector(pd);
        }
    }
}
