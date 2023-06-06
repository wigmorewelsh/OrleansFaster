using Microsoft.Extensions.Options;
using Orleans.Persistence.Faster.Session;
using Orleans.Runtime;

namespace Orleans.Persistence.Faster.Storage;

internal class FasterStorageGrain : Grain, IFasterStorageGrain
{
    private readonly FasterSessionInstance session;

    public FasterStorageGrain()
    {
        session = new FasterSessionInstance(Options.Create(new FasterSettings()));
    }
    
    public async Task SetAsync(GrainId key, string storageName, byte[] value)
    {
        await session.WriteAsync(key, storageName, value);
    }
    
    public async Task<byte[]> GetAsync(GrainId key, string storageName)
    {
        return  await session.ReadAsync(key, storageName);
    }
}