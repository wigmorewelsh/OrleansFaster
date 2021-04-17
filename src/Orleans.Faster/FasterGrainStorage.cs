using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using FASTER.core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Configuration;
using Orleans.Faster;
using Orleans.Persistence.Faster.Converters;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Persistence.Faster
{
    public class FasterGrainStorage : IGrainStorage, 
        ILifecycleParticipant<ISiloLifecycle>, 
        IDisposable

    {
        private readonly ILogger<FasterGrainStorage> logger;
        private readonly IGrainReferenceConverter locator;
        private readonly string name;
        private FasterKV<ReadOnlyMemory<byte>, Memory<byte>> store;
        private IDevice? log;

        private CancellableTaskCollection _cancellableTaskCollection = new CancellableTaskCollection();

        public FasterGrainStorage(
            ILogger<FasterGrainStorage> logger,
            IProviderRuntime providerRuntime,
            IGrainReferenceConverter locator,
            IOptions<FasterGrainStorageOptions> options,
            IOptions<ClusterOptions> clusterOptions,
            ISerializer defaultSerializer, 
            string name)
        {
            this.logger = logger;
            this.locator = locator;
            this.name = name;
            _defaultSerializer = defaultSerializer;
        }


        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var key = ComputeKey(grainType, grainReference);
            var session = sessionPool.GetSession();
            try
            {
                var result = await session.ReadAsync(key.AsMemory());
                var (status, data) = result.Complete();

                if (status == Status.PENDING)
                {
                    await session.CompletePendingAsync(true);
                }

                if (status == Status.OK)
                {
                    var buffer = data.Item1.Memory.ToArray();

                    var state = _defaultSerializer.Deserialize(buffer, grainState.Type);

                    grainState.State = state;
                    data.Item1.Dispose();
                }
                else
                {

                }
            }
            finally
            {
                sessionPool.ReturnSession(session);
            }
        }


        private static byte[] ComputeKey(string grainType, GrainReference grainReference)
        {
            var grainId = Utils.GrainIdAndExtensionAsString(grainReference);
            var baseGrainType = grainType; // ExtractBaseClass(grainType); there is a bug in ExtractBaseClass that strips the state name off generic grains 
            // so that multiple persistance state have the same name
            var type = Encoding.UTF8.GetBytes(baseGrainType);

            using var memStr = new MemoryStream();
            memStr.Write(type);

            memStr.Write(grainId.GetHashBytes());
            memStr.Flush();
            memStr.Position = 0;

            var hashBytes = memStr.ToArray();

            using var hashAlgo = SHA256Managed.Create();
            var key = hashAlgo.ComputeHash(hashBytes);
            return key;
        }

        public async Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var session = sessionPool.GetSession();
            try
            {
                var key = ComputeKey(grainType, grainReference);

                var array = await _defaultSerializer.Serialize(grainState);
                var desiredValue = array.AsMemory();
                session.Upsert(key.AsMemory(), desiredValue);
                writeTrigger.Writer.TryWrite(0);
            }
            finally
            {
                sessionPool.ReturnSession(session);
            }
        
        }

        public Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            return Task.CompletedTask;
        }
        
        private async Task Init(CancellationToken cancellationToken)
        {
            await Task.Run(async () =>
            {
                logger.Info("Starting Faster Log");

                log = Devices.CreateLogDevice(@$"data/{name}/hlog.log");

                var logSettings = new LogSettings
                {
                    LogDevice = log,
                    MutableFraction = 0.2,
                    //warning changing the settings will reset the log files
                    // PageSizeBits = 12, // (4K Pages)
                    SegmentSizeBits = 28, // about 250MB
                    MemorySizeBits = 31 // 2GB
                };

                var checkpointDir = @$"data/{name}";
                var deviceLogCommitCheckpointManager = new DeviceLogCommitCheckpointManager(
                    new LocalStorageNamedDeviceFactory(),
                    new DefaultCheckpointNamingScheme(checkpointDir));

                store = new FasterKV<ReadOnlyMemory<byte>, Memory<byte>>(1L << 10, logSettings, new CheckpointSettings
                {
                    CheckpointDir = checkpointDir,
                    CheckPointType = CheckpointType.FoldOver,
                });

                sessionPool = new FasterSessionPool(store);

                try
                {
                    await store.RecoverAsync();
                }
                catch (Exception err)
                {
                    logger.LogWarning(err, "Issue when recovering storage '{message}'", err.Message);
                }

                IssuePeriodicCheckpoints();
                IssuePeriodicCompaction();
            });
        }

        private Channel<int> writeTrigger = Channel.CreateBounded<int>(1);
        private IServiceProvider serviceProvider;
        private FasterSessionPool sessionPool;
        private readonly ISerializer _defaultSerializer;

        private CancellableTask IssuePeriodicCheckpoints()
        {
            return _cancellableTaskCollection.Run(async token => 
            {
                while (!token.IsCancellationRequested)
                {
                    await writeTrigger.Reader.WaitToReadAsync(token);
                    await Task.Delay(100, token);
                    if (store != null)
                    {
                        await store.TakeFullCheckpointAsync(CheckpointType.FoldOver, token);
                    }

                    if(!token.IsCancellationRequested)
                        writeTrigger.Reader.TryRead(out var _);
                }
            });
        }
        
        private CancellableTask IssuePeriodicCompaction()
        {
            return _cancellableTaskCollection.Run(async token => 
            {
                while (!token.IsCancellationRequested)
                {
                    var session = sessionPool.GetSession();
                    try
                    {
                        await store.TakeFullCheckpointAsync(CheckpointType.FoldOver, token);
                        await store.CompleteCheckpointAsync(token);
                        Console.WriteLine("Compacting log");
                        session.Compact(store.Log.SafeReadOnlyAddress, true);
                        // FasterKV keeps files in memory, compacting after unloading
                        // GC.Collect(2, GCCollectionMode.Optimized, false, true);
                        Console.WriteLine("Log begin address: {0}", store.Log.BeginAddress);
                        Console.WriteLine("Log tail address: {0}", store.Log.TailAddress);
                    }
                    finally
                    {
                        sessionPool.ReturnSession(session);
                    }

                    await Task.Delay(60 * 5 * 1000, token);
                }
            });
        }
        
        private async Task Close(CancellationToken token)
        {
            await Task.Run(async () =>
            {
                logger.Info("Stopping faster storage");
                writeTrigger.Writer.TryComplete();
                await _cancellableTaskCollection.DisposeAsync();
                logger.Info("Disposed Jobs");
                await store.TakeFullCheckpointAsync(CheckpointType.Snapshot);
                logger.Info("Checkpoint complete");
                store.Dispose();
                store = null;
                log.Dispose();
                log = null;
                logger.Info("Faster shutdown complete");
            });
        }

        public void Dispose()
        {
            // store?.Dispose();
            // store = null;
            // olog?.Dispose();
            // log?.Dispose();
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(OptionFormattingUtilities.Name<FasterGrainStorage>("FasterGrainStorage"), ServiceLifecycleStage.ApplicationServices, Init, Close);
        }
    }
}