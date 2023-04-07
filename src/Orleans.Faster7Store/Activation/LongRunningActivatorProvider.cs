using Orleans.Metadata;
using Orleans.Runtime;

namespace Orleans.Faster7Store;

public class LongRunningActivatorProvider : IGrainContextActivatorProvider
{
    private readonly GrainClassMap _grainClassMap;

    public LongRunningActivatorProvider(GrainClassMap grainClassMap)
    {
        _grainClassMap = grainClassMap;
    }
    
    public bool TryGet(GrainType grainType, out IGrainContextActivator activator)
    {
        if (_grainClassMap.TryGetGrainClass(grainType, out var grainClass) &&
            typeof(IFasterStorageGrain).IsAssignableTo(grainClass))
        {
            activator = new LongRunningGrainContextActivator();
            return true;
        }

        activator = null;
        return false;
    }
}