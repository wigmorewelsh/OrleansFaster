using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Reflection;
using System.Runtime.Intrinsics.Arm;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.ObjectPool;

namespace Orleans.Persistence.Faster
{
    using FasterStore = FasterKV<ReadOnlyMemory<byte>, Memory<byte>>;
    using FasterSession = ClientSession<ReadOnlyMemory<byte>, Memory<byte>, Memory<byte>, (IMemoryOwner<byte>, int), byte, CustomMemoryFunctions<byte>>;

        /// <summary>
    /// Asynchronous pool of fixed pre-filled capacity
    /// Supports sync get (TryGet) for fast path
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class AsyncPool<T> : IDisposable where T : IDisposable
    {
        readonly int size;
        readonly SemaphoreSlim handleAvailable;
        readonly ConcurrentQueue<T> itemQueue;
        bool disposed = false;
        int disposedCount = 0;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="size"></param>
        /// <param name="creator"></param>
        public AsyncPool(int size, Func<T> creator)
        {
            this.size = size;
            this.handleAvailable = new SemaphoreSlim(size);
            this.itemQueue = new ConcurrentQueue<T>();
            for (int i = 0; i < size; i++)
                itemQueue.Enqueue(creator());
        }

        /// <summary>
        /// Get item
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async ValueTask<T> GetAsync(CancellationToken token = default)
        {
            for (; ; )
            {
                if (disposed)
                    throw new FasterException("Getting handle in disposed device");

                await handleAvailable.WaitAsync(token);
                if (itemQueue.TryDequeue(out T item))
                    return item;
            }
        }

        /// <summary>
        /// Try get item
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool TryGet(out T item)
        {
            if (disposed)
            {
                item = default;
                return false;
            }
            return itemQueue.TryDequeue(out item);
        }

        /// <summary>
        /// Return item to pool
        /// </summary>
        /// <param name="item"></param>
        public void Return(T item)
        {
            itemQueue.Enqueue(item);
            if (handleAvailable.CurrentCount < itemQueue.Count)
                handleAvailable.Release();
        }

       /// <summary>
       /// Dispose
       /// </summary>
        public void Dispose()
        {
            disposed = true;

            while (disposedCount < size)
            {
                while (itemQueue.TryDequeue(out var item))
                {
                    item.Dispose();
                    disposedCount++;
                }
                if (disposedCount < size)
                    handleAvailable.Wait();
            }
        }
    }
    
    public sealed class FasterSessionPool
    {
        private readonly FasterStore fasterKv;
        private readonly AsyncPool<FasterSession> sessionPool;

        public FasterSessionPool(FasterStore fasterKv, LogSettings logSettings)
        {
            this.fasterKv = fasterKv;
            // var sessionPoolPolicy = new SerializerSessionPoolPolicy(fasterKv);
            sessionPool = new AsyncPool<FasterSession>(
                logSettings.LogDevice.ThrottleLimit,
                () => fasterKv.For(new CustomMemoryFunctions<byte>()).NewSession<CustomMemoryFunctions<byte>>());
        }

        public ValueTask<FasterSession> GetSession() => sessionPool.GetAsync();

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
                return obj.CompletePending();
                // obj.FullReset();
                return true;
            }
        }
    }
}