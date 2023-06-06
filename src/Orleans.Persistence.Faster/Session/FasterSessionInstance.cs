using System.Text;
using FASTER.core;
using Microsoft.Extensions.Options;
using Orleans.Runtime;

namespace Orleans.Persistence.Faster.Session;

internal class FasterSessionInstance
{
    private readonly FasterSessionPool sessionPool;

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

        var store = new FasterKV<ReadOnlyMemory<byte>, Memory<byte>>(1L << 10, logSettings, new CheckpointSettings
        {
            CheckpointDir = checkpointDir,
            // CheckPointType = CheckpointType.FoldOver,
            RemoveOutdated = true
        });

        sessionPool = new FasterSessionPool(store, logSettings);
    }

    public async Task WriteAsync(GrainId key, string storageName, byte[] value)
    {
        var session = await sessionPool.GetSession();
        var keyBytes = ComputeKey(storageName, key);
        var valueBytes = value;
        var keySpan = new ReadOnlyMemory<byte>(keyBytes);
        var valueSpan = new Memory<byte>(valueBytes);
        var status = await session.UpsertAsync(ref keySpan, ref valueSpan);
        while(status.Status.IsPending)
        {
            status = await status.CompleteAsync();
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
        var keyBytes = ComputeKey(storageName, key);
        var keySpan = new ReadOnlyMemory<byte>(keyBytes);
        var res = await session.ReadAsync(ref keySpan);
        var (mem, len) = res.Output;
        if(len is 0 || mem is null)
        {
            return Array.Empty<byte>();
        }
        var bytes = mem.Memory.Slice(0, len).ToArray();
        return bytes;
    }
}