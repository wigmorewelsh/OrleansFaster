using System;
using System.Buffers;
using System.Reflection;
using System.Runtime.Intrinsics.Arm;
using FASTER.core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.ObjectPool;

namespace Orleans.Persistence.Faster
{
    using FasterStore = FasterKV<ReadOnlyMemory<byte>, Memory<byte>>;
    using FasterSession = ClientSession<ReadOnlyMemory<byte>, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), byte, CustomMemoryFunctions<byte>>;

    public sealed class FasterSessionPool
    {
        private readonly FasterStore fasterKv;
        private readonly ObjectPool<FasterSession> sessionPool;

        public FasterSessionPool(FasterStore fasterKv)
        {
            this.fasterKv = fasterKv;
            var sessionPoolPolicy = new SerializerSessionPoolPolicy(fasterKv);
            sessionPool = new DefaultObjectPool<FasterSession>(sessionPoolPolicy);
        }

        public FasterSession GetSession() => sessionPool.Get();

        public void ReturnSession(FasterSession session) => sessionPool.Return(session);

        private class SerializerSessionPoolPolicy : IPooledObjectPolicy<FasterSession>
        {
            private readonly FasterStore store;

            public SerializerSessionPoolPolicy(FasterStore store)
            {
                this.store = store;
            }

            public FasterSession Create()
            {
                return store.For(new CustomMemoryFunctions<byte>()).NewSession<CustomMemoryFunctions<byte>>();
            }

            public bool Return(FasterSession obj)
            {
                // obj.FullReset();
                return true;
            }
        }
    }
}