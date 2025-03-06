using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Faster.Tests.Grains;

public class SampleGrain : Grain, ISampleGrain
{
    private readonly IPersistentState<SampleState> _state;

    public SampleGrain([PersistentState("sample")] IPersistentState<SampleState> state)
    {
        _state = state;
    }
    
    public async Task Store(string data)
    {
        _state.State.Data = data;
        await _state.WriteStateAsync();
    }

    public Task<string> Fetch()
    {
        return Task.FromResult(_state.State.Data);
    }
}