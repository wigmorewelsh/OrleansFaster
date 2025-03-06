using Orleans.Runtime;

namespace Orleans.Contrib.Persistance.Faster.Storage;

public interface IFasterStorageGrain : IGrainWithIntegerKey
{
    Task SetAsync(GrainId key, string storageName, byte[] value);
    Task<byte[]> GetAsync(GrainId key, string storageName);
}