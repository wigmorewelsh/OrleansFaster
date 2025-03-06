using System.Buffers;
using FASTER.core;
using Microsoft.Extensions.ObjectPool;

namespace Orleans.Contrib.Persistance.Faster.Session;

internal sealed class FasterSessionPool
{
    private readonly FasterKV<ReadOnlyMemory<byte>, Memory<byte>> fasterKv;
    private readonly AsyncPool<ClientSession<ReadOnlyMemory<byte>, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), byte, CustomMemoryFunctions<byte>>> sessionPool;

    public FasterSessionPool(FasterKV<ReadOnlyMemory<byte>, Memory<byte>> fasterKv, LogSettings logSettings)
    {
        this.fasterKv = fasterKv;
        // var sessionPoolPolicy = new SerializerSessionPoolPolicy(fasterKv);
        sessionPool = new AsyncPool<ClientSession<ReadOnlyMemory<byte>, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), byte, CustomMemoryFunctions<byte>>>(
            logSettings.LogDevice.ThrottleLimit,
            () => fasterKv.For(new CustomMemoryFunctions<byte>()).NewSession<CustomMemoryFunctions<byte>>());
    }

    public ValueTask<ClientSession<ReadOnlyMemory<byte>, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), byte, CustomMemoryFunctions<byte>>> GetSession() => sessionPool.GetAsync();

    public void ReturnSession(ClientSession<ReadOnlyMemory<byte>, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), byte, CustomMemoryFunctions<byte>> session) => sessionPool.Return(session);

    private class SerializerSessionPoolPolicy : IPooledObjectPolicy<ClientSession<ReadOnlyMemory<byte>, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), byte, CustomMemoryFunctions<byte>>>
    {
        private readonly FasterKV<ReadOnlyMemory<byte>, Memory<byte>> store;

        public SerializerSessionPoolPolicy(FasterKV<ReadOnlyMemory<byte>, Memory<byte>> store)
        {
            this.store = store;
        }

        public ClientSession<ReadOnlyMemory<byte>, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), byte, CustomMemoryFunctions<byte>> Create()
        {
            return store.For(new CustomMemoryFunctions<byte>()).NewSession<CustomMemoryFunctions<byte>>();
        }

        public bool Return(ClientSession<ReadOnlyMemory<byte>, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), byte, CustomMemoryFunctions<byte>> obj)
        {
            return obj.CompletePending();
            // obj.FullReset();
            return true;
        }
    }
}