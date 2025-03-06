using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;
using Orleans.Runtime;
using Orleans.Storage;

namespace Sample;

[Reentrant]
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
    public HelloState()
    {
            
    }
        
    public HelloState(int number)
    {
        Number = number;
    }
    public int Number { get; set; }
}

internal interface IHelloGrain : IGrainWithIntegerKey
{
    // [OneWay]
    Task DoOne();
    Task<int> Current();
}