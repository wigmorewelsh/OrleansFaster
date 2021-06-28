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
        private readonly IOptions<FasterGrainStorageOptions> _options;
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
            _options = options;
            this.name = name;
            _defaultSerializer = defaultSerializer;
        }


        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var key = ComputeKey(grainType, grainReference);
            // var state = await Task.Run(async () =>
            // {
                var session = await sessionPool.GetSession();
                try
                {
                    var result = await session.ReadAsync(key.AsMemory());
                    var (status, data) = result.Complete();

                    while (status == Status.PENDING)
                    {
                        await session.CompletePendingAsync();
                        (status, data) = result.Complete();
                    }

                    if (status == Status.OK)
                    {
                        var buffer = data.Item1.Memory.ToArray();

                        var state = _defaultSerializer.Deserialize(buffer, grainState.Type);

                        data.Item1.Dispose();
            grainState.State = state;
            return;
            // return state;
                    }
                }
                finally
                {
                    sessionPool.ReturnSession(session);
                }

                // return Activator.CreateInstance(grainState.Type);
            grainState.State = Activator.CreateInstance(grainState.Type);
            // });
            // grainState.State = state;
        }


        private static byte[] ComputeKey(string grainType, GrainReference grainReference)
        {
            var grainId = Utils.GrainIdAndExtensionAsString(grainReference);
            var baseGrainType =
                grainType; // ExtractBaseClass(grainType); there is a bug in ExtractBaseClass that strips the state name off generic grains 
            // so that multiple persistance state have the same name
            var type = Encoding.UTF8.GetBytes(baseGrainType);

            using var memStr = new MemoryStream();
            // memStr.Write(type);

            memStr.Write(grainId.GetHashBytes());
            memStr.Flush();
            memStr.Position = 0;

            var hashBytes = memStr.ToArray();

            return hashBytes;
        }


        public async Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var key = ComputeKey(grainType, grainReference);

            var array = await _defaultSerializer.Serialize(grainState);

            var desiredValue = array.AsMemory();

            var session = await sessionPool.GetSession();
            try
            {
                writeTrigger.Writer.TryWrite(0);
                var res = await session.UpsertAsync(key.AsMemory(), desiredValue);
                while (res.Complete() == Status.PENDING)
                    res = await res.CompleteAsync();
                // logger.Info("Written grain");
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

                log = Devices.CreateLogDevice(Path.Combine(_options.Value.StorageBaseDirectory, @$"{name}/hlog.log"));

                var logSettings = new LogSettings
                {
                    LogDevice = log,
                    MutableFraction = 0.3,
                    //warning changing the settings will reset the log files
                    // PageSizeBits = 12, // (4K Pages)
                    SegmentSizeBits = 28, // about 32MB
                    MemorySizeBits = 31 // 250MB,
                };

                logger.Info("Limiting devices to {limit}", logSettings.LogDevice.ThrottleLimit);

                var checkpointDir = Path.Combine(_options.Value.StorageBaseDirectory, name);

                store = new FasterKV<ReadOnlyMemory<byte>, Memory<byte>>(1L << 10, logSettings, new CheckpointSettings
                {
                    CheckpointDir = checkpointDir,
                    CheckPointType = CheckpointType.FoldOver,
                    RemoveOutdated = true
                });

                sessionPool = new FasterSessionPool(store, logSettings);

                try
                {
                    await store.RecoverAsync();
                    await store.TakeFullCheckpointAsync(CheckpointType.Snapshot);
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
                    try
                    {
                        await writeTrigger.Reader.WaitToReadAsync(token);
                        await Task.Delay(100, token);
                        if (store != null)
                        {
                            // logger.Info("Starting checkpoint");
                            var (success, guid) = await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);
                            logger.Info("Written checkpoint {success} {guid}", success, guid);
                        }

                        writeTrigger.Reader.TryRead(out var _);
                    }
                    catch (Exception err)
                    {
                        logger.LogError(err.Message, err);
                    }
                }
            }, TaskCreationOptions.LongRunning);
        }

        private CancellableTask IssuePeriodicCompaction()
        {
            return _cancellableTaskCollection.Run(async token =>
            {
                try
                {
                    while (!token.IsCancellationRequested)
                    {
                        var session = await sessionPool.GetSession().ConfigureAwait(false);
                        Console.WriteLine("Compacting log");
                        var logSafeReadOnlyAddress = store.Log.BeginAddress +
                                                     0.2 * (store.Log.SafeReadOnlyAddress - store.Log.BeginAddress);
                        // session.Compact(logSafeReadOnlyAddress, false);
                        var (fullSuccess, fullGuid) =
                            await store.TakeFullCheckpointAsync(CheckpointType.FoldOver, token);
                        logger.Info("Written full checkpoint {succ} {guid}", fullSuccess, fullGuid);
                        session.Compact((long) logSafeReadOnlyAddress, true);
                        sessionPool.ReturnSession(session);
                        await Task.Delay(60 * 5 * 1000, token);
                    }
                }
                finally
                {
                }
            }, TaskCreationOptions.LongRunning);
        }

        private async Task Close(CancellationToken token)
        {
            await Task.Run(async () =>
            {
                logger.Info("Stopping faster storage");
                writeTrigger.Writer.TryComplete();
                await _cancellableTaskCollection.DisposeAsync();
                logger.Info("Disposed Jobs");
                var (success, guid) =
                    await store.TakeFullCheckpointAsync(CheckpointType.FoldOver).ConfigureAwait(false);
                logger.Info("Written full checkpoint {yes} {guid}", success, guid);
                store.Dispose();
                store = null;
                log.Dispose();
                log = null;
                logger.Info("Faster shutdown complete");
            });
        }

        public void Dispose()
        {
         
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(OptionFormattingUtilities.Name<FasterGrainStorage>("FasterGrainStorage"),
                ServiceLifecycleStage.ApplicationServices, Init, Close);
        }
    }
}