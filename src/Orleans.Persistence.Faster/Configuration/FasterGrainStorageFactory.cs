using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration.Overrides;
using Orleans.Persistence.Faster.Storage;
using Orleans.Storage;

namespace Orleans.Persistence.Faster.Configuration;

internal static class FasterGrainStorageFactory
{
    public static IGrainStorage Create(IServiceProvider services, object? key)
    {
        var name = key as string;
        if(string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("Storage name cannot be null or empty", nameof(key));
        }
        
        var optionsMonitor = services.GetRequiredService<IOptionsMonitor<FasterGrainStorageOptions>>();
        var clusterOptions = services.GetProviderClusterOptions(name);
        return ActivatorUtilities.CreateInstance<FasterGrainStorage>(services);//, Microsoft.Extensions.Options.Options.Create(optionsMonitor.Get(name)), name, clusterOptions);
    }
}