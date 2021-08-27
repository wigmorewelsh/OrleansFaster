using System;
using System.Threading.Tasks;

namespace Orleans.Persistence.Faster
{
    public interface ISerializer
    {
        object Deserialize(Memory<byte> buffer, Type grainStateType);
        Task<ArraySegment<byte>> Serialize(IGrainState grainState);
    }
}