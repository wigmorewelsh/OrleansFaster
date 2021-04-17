using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;
using Orleans.Storage;

namespace Sample
{
    internal class HelloGrain : Grain, IHelloGrain
    {
        readonly IPersistentState<HelloState> _storage;

        public HelloGrain([PersistentState("Default")]IPersistentState<HelloState> storage)
        {
            _storage = storage;
        }
        
        public async Task DoOne()
        {
            _storage.State.Number++;
            await _storage.WriteStateAsync();
        }

        public Task<int> Current()
        {
            return Task.FromResult(_storage.State.Number);
        }
    }

    internal class HelloState
    {
        public int Number { get; set; }
    }

    internal interface IHelloGrain : IGrainWithIntegerKey
    {
        Task DoOne();
        Task<int> Current();
    }
}