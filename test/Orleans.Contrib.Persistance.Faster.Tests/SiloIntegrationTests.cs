using System;
using System.Threading.Tasks;
using Orleans.Contrib.Persistance.Faster.Tests.Grains;
using Orleans.Contrib.Streaming.NATS.Tests.Fixtures;
using Xunit;

namespace Orleans.Contrib.Persistance.Faster.Tests;

public class SiloIntegrationTests : IClassFixture<TestFixture<SiloIntegrationTests.TestSetting>>
{
    private readonly TestFixture<TestSetting> _fixture;

    public class TestSetting : ITestSettings
    {
    }

    public SiloIntegrationTests(TestFixture<TestSetting> fixture)
    {
        _fixture = fixture;
    }
        
    [Fact]
    public async Task StoreAndFetchGuidGrain()
    {
        var grainId = Guid.NewGuid();

        var client = _fixture.Client.GetGrain<ISampleGrain>(grainId);

        var data = "sample data " + grainId;

        await client.Store(data);

        await _fixture.RestartSiloAsync();
        
        var client2 = _fixture.Client.GetGrain<ISampleGrain>(grainId);
        
        var state = await client2.Fetch();
        
        Assert.Equal(data, state);
    }
}