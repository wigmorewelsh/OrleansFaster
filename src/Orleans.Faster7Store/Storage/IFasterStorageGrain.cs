using Orleans.Runtime;

namespace Orleans.Faster7Store;

public interface IFasterStorageGrain : IGrainWithIntegerKey
{
    Task SetAsync(GrainId key, string storageName, byte[] value);
    Task<byte[]> GetAsync(GrainId key, string storageName);
}