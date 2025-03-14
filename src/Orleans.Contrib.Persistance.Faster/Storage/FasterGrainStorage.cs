using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Contrib.Persistance.Faster.Storage;

internal class FasterGrainStorage : IGrainStorage
{
    private readonly IGrainStorageSerializer serializer;
    private readonly IGrainFactory factory;
    private int NumberOfShards = 1;

    public FasterGrainStorage(IGrainStorageSerializer serializer, IGrainFactory factory)
    {
        this.serializer = serializer;
        this.factory = factory;
    }
    
    public async Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        //TODO: support other grain ids
        var grain = factory.GetGrain<IFasterStorageGrain>(grainId.GetUniformHashCode() % NumberOfShards);
        var data = await grain.GetAsync(grainId, stateName);
        if (data != null && data.Length > 0)
        {
            grainState.State = serializer.Deserialize<T>(new BinaryData(data)) ?? Activator.CreateInstance<T>();
        }
        else
        {
            grainState.State = Activator.CreateInstance<T>();
        }
    }

    public async Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var grain = factory.GetGrain<IFasterStorageGrain>(grainId.GetUniformHashCode() % NumberOfShards );
        var data = serializer.Serialize(grainState.State);
        await grain.SetAsync(grainId, stateName, data.ToArray());
    }

    public async Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
    {
        var grain = factory.GetGrain<IFasterStorageGrain>(grainId.GetUniformHashCode() % NumberOfShards );
        await grain.SetAsync(grainId, stateName, null);
        grainState.State = Activator.CreateInstance<T>();
    }
}