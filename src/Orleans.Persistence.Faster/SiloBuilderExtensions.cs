using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Persistence.Faster.Configuration;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Persistence.Faster;
#nullable disable

public static class SiloBuilderExtensions
{
    /// <summary>
    /// Configure silo to use memory grain storage as the default grain storage.
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="configureOptions">The configuration delegate.</param>
    /// <returns>The silo builder.</returns>
    public static
        ISiloBuilder AddFasterStorageAsDefault(
            this ISiloBuilder builder,
            Action<FasterGrainStorageOptions> configureOptions)
    {
        return builder.AddFasterStorageAsDefault(ob => ob.Configure(configureOptions));
    }

    /// <summary>Configure silo to use memory grain storage.</summary>
    /// <param name="builder">The builder.</param>
    /// <param name="name">The name of the storage provider. This must match with the <c>StorageName</c> property specified when injecting state into a grain.</param>
    /// <param name="configureOptions">The configuration delegate.</param>
    /// <returns>The silo builder.</returns>
    public static ISiloBuilder AddFasterStorage(
        this ISiloBuilder builder,
        string name,
        Action<FasterGrainStorageOptions> configureOptions)
    {
        return builder.AddFasterStorage(name, ob => ob.Configure(configureOptions));
    }

    /// <summary>
    /// Configure silo to use memory grain storage as the default grain storage.
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="configureOptions">The configuration delegate.</param>
    /// <returns>The silo builder.</returns>
    public static ISiloBuilder AddFasterStorageAsDefault(
        this ISiloBuilder builder,
        Action<OptionsBuilder<FasterGrainStorageOptions>> configureOptions = null)
    {
        return builder.AddFasterStorage("Default", configureOptions);
    }

    /// <summary>Configure silo to use memory grain storage.</summary>
    /// <param name="builder">The builder.</param>
    /// <param name="name">The name of the storage provider. This must match with the <c>StorageName</c> property specified when injecting state into a grain.</param>
    /// <param name="configureOptions">The configuration delegate.</param>
    /// <returns>The silo builder.</returns>
    public static ISiloBuilder AddFasterStorage(
        this ISiloBuilder builder,
        string name,
        Action<OptionsBuilder<FasterGrainStorageOptions>> configureOptions = null)
    {
        return builder.ConfigureServices((Action<IServiceCollection>)(services =>
        {
            if (configureOptions != null)
                configureOptions(services.AddOptions<FasterGrainStorageOptions>(name));
            services.AddTransient<IPostConfigureOptions<FasterGrainStorageOptions>, DefaultStorageProviderSerializerOptionsConfigurator<FasterGrainStorageOptions>>();
            services.ConfigureNamedOptionForLogging<FasterGrainStorageOptions>(name);
            if (string.Equals(name, "Default", StringComparison.Ordinal))
                services.TryAddSingleton<IGrainStorage>(sp => sp.GetKeyedService<IGrainStorage>("Default"));
            services.AddKeyedSingleton<IGrainStorage>(name, FasterGrainStorageFactory.Create);
        }));
    }
}