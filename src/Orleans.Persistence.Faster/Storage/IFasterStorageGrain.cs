using Orleans.Runtime;

namespace Orleans.Faster7Store;

internal interface IFasterStorageGrain : IGrainWithIntegerKey
{
    Task SetAsync(GrainId key, string storageName, byte[] value);
    Task<byte[]> GetAsync(GrainId key, string storageName);
}