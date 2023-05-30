using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration.Overrides;
using Orleans.Faster7Store;
using Orleans.Persistence.Faster;
using Orleans.Storage;

namespace Orleans.Faster
{
    internal static class FasterGrainStorageFactory
    {
        public static IGrainStorage Create(IServiceProvider services, string name)
        {
            var optionsMonitor = services.GetRequiredService<IOptionsMonitor<FasterGrainStorageOptions>>();
            var clusterOptions = services.GetProviderClusterOptions(name);
            return ActivatorUtilities.CreateInstance<FasterGrainStorage>(services, Microsoft.Extensions.Options.Options.Create(optionsMonitor.Get(name)), name, clusterOptions);
        }
    }
}