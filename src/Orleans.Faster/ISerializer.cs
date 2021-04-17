using System;
using System.Threading.Tasks;

namespace Orleans.Persistence.Faster
{
    public interface ISerializer
    {
        object Deserialize(byte[] buffer, Type grainStateType);
        Task<byte[]> Serialize(IGrainState grainState);
    }
}