using Microsoft.Extensions.Options;
using Orleans.Concurrency;
using Orleans.Contrib.Persistance.Faster.Session;
using Orleans.Runtime;

namespace Orleans.Contrib.Persistance.Faster.Storage;

[Reentrant]
public class FasterStorageGrain : Grain, IFasterStorageGrain
{
    private readonly FasterSessionInstance session;

    public FasterStorageGrain()
    {
        session = new FasterSessionInstance(Options.Create(new FasterSettings()));
    }

    public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        session.Dispose();
        return Task.CompletedTask;
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