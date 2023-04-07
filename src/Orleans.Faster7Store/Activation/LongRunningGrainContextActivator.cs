using Orleans.Runtime;

namespace Orleans.Faster7Store;

public class LongRunningGrainContextActivator : IGrainContextActivator
{
    public IGrainContext CreateContext(GrainAddress address)
    {
        return new LongRunningGrainContext();
    }
}