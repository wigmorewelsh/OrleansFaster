using System.Text;
using FASTER.core;
using Microsoft.Extensions.Options;
using Orleans.Runtime;

namespace Orleans.Contrib.Persistance.Faster.Session;

internal class FasterSessionInstance : IDisposable
{
    private readonly FasterSessionPool sessionPool;
    private readonly FasterKV<ReadOnlyMemory<byte>,Memory<byte>> store;

    public FasterSessionInstance(IOptions<FasterSettings> _options)
    {
        var log = Devices.CreateLogDevice(Path.Combine(_options.Value.StorageBaseDirectory, @$"Test/hlog.log"));

        var logSettings = new LogSettings
        {
            LogDevice = log,
            MutableFraction = 0.3,
            //warning changing the settings will reset the log files
            // PageSizeBits = 12, // (4K Pages)
            SegmentSizeBits = 28, // about 32MB
            MemorySizeBits = 31 // 250MB,
        };

        var checkpointDir = Path.Combine(_options.Value.StorageBaseDirectory, "Test");

        store = new FasterKV<ReadOnlyMemory<byte>, Memory<byte>>(1L << 10, logSettings, new CheckpointSettings
        {
            CheckpointDir = checkpointDir,
            RemoveOutdated = true
        });

        try
        {
            store.Recover();
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }

        sessionPool = new FasterSessionPool(store, logSettings);
        
        Task.Run(BackgroundCheckpoint);
    }
    
    private async Task BackgroundCheckpoint()
    {
        var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        while (await timer.WaitForNextTickAsync())
        {
            try
            {
                await store.TakeHybridLogCheckpointAsync(CheckpointType.FoldOver);
                var (complete, _) = await store.TakeFullCheckpointAsync(CheckpointType.FoldOver, CancellationToken.None); } catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }

    public async Task WriteAsync(GrainId key, string storageName, byte[] value)
    {
        var session = await sessionPool.GetSession();
        try
        {
            var keyBytes = ComputeKey(storageName, key);
            var valueBytes = value;
            var keySpan = new ReadOnlyMemory<byte>(keyBytes);
            var valueSpan = new Memory<byte>(valueBytes);
            var status = await session.UpsertAsync(ref keySpan, ref valueSpan);
            while (status.Status.IsPending)
            {
                status = await status.CompleteAsync();
            }
        }
        catch (FasterException ex)
        {
            throw new Exception(ex.ToString());
        }
        finally
        {
            sessionPool.ReturnSession(session);
        }

    }

    private static byte[] ComputeKey(string storageName, GrainId grainReference)
    {
        var grainId = grainReference.Key.AsSpan();
        var type = Encoding.UTF8.GetBytes(storageName);

        using var memStr = new MemoryStream();
        memStr.Write(type);

        memStr.Write(grainId.ToArray());
        memStr.Flush();
        memStr.Position = 0;

        var hashBytes = memStr.ToArray();

        return hashBytes;
    }

    public async Task<byte[]> ReadAsync(GrainId key, string storageName)
    {
        var session = await sessionPool.GetSession();
        try
        {
            var keyBytes = ComputeKey(storageName, key);
            var keySpan = new ReadOnlyMemory<byte>(keyBytes);
            var res = await session.ReadAsync(ref keySpan);
            res.Complete();
            var (mem, len) = res.Output;
            if (len is 0 || mem is null)
            {
                return Array.Empty<byte>();
            }

            var bytes = mem.Memory.Slice(0, len).ToArray();
            return bytes;
        }
        finally
        {
            sessionPool.ReturnSession(session);
        }
    }

    public void Dispose()
    {
        store.Dispose();
    }
}