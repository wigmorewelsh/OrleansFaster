using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Faster;
using Orleans.Hosting;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Persistence.Faster
{
    public static class SiloBuilderExtensions
    {
        /// <summary>
        /// Configure silo to use AdoNet grain storage as the default grain storage. Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </summary>
        /// <remarks>
        /// Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </remarks>
        public static ISiloHostBuilder AddFasterNetGrainStorageAsDefault(this ISiloHostBuilder builder, Action<FasterGrainStorageOptions> configureOptions)
        {
            return builder.AddFasterNetGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use  AdoNet grain storage for grain storage. Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </summary>
        /// <remarks>
        /// Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </remarks>
        public static ISiloHostBuilder AddFasterNetGrainStorage(this ISiloHostBuilder builder, string name, Action<FasterGrainStorageOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddFasterNetGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Configure silo to use  AdoNet grain storage as the default grain storage. Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </summary>
        /// <remarks>
        /// Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </remarks>
        public static ISiloHostBuilder AddFasterNetGrainStorageAsDefault(this ISiloHostBuilder builder, Action<OptionsBuilder<FasterGrainStorageOptions>> configureOptions = null)
        {
            return builder.AddFasterNetGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use AdoNet grain storage for grain storage. Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </summary>
        /// <remarks>
        /// Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </remarks>
        public static ISiloHostBuilder AddFasterNetGrainStorage(this ISiloHostBuilder builder, string name, Action<OptionsBuilder<FasterGrainStorageOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.AddFasterNetGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Configure silo to use AdoNet grain storage as the default grain storage. Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </summary>
        /// <remarks>
        /// Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </remarks>
        public static ISiloBuilder AddFasterNetGrainStorageAsDefault(this ISiloBuilder builder, Action<FasterGrainStorageOptions> configureOptions)
        {
            return builder.AddFasterNetGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use  AdoNet grain storage for grain storage. Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </summary>
        /// <remarks>
        /// Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </remarks>
        public static ISiloBuilder AddFasterNetGrainStorage(this ISiloBuilder builder, string name, Action<FasterGrainStorageOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddFasterNetGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Configure silo to use  AdoNet grain storage as the default grain storage. Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </summary>
        /// <remarks>
        /// Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </remarks>
        public static ISiloBuilder AddFasterNetGrainStorageAsDefault(this ISiloBuilder builder, Action<OptionsBuilder<FasterGrainStorageOptions>> configureOptions = null)
        {
            return builder.AddFasterNetGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        /// <summary>
        /// Configure silo to use AdoNet grain storage for grain storage. Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </summary>
        /// <remarks>
        /// Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </remarks>
        public static ISiloBuilder AddFasterNetGrainStorage(this ISiloBuilder builder, string name, Action<OptionsBuilder<FasterGrainStorageOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.AddFasterNetGrainStorage(name, configureOptions));
        }

        /// <summary>
        /// Configure silo to use  AdoNet grain storage as the default grain storage. Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </summary>
        /// <remarks>
        /// Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </remarks>
        public static IServiceCollection AddFasterNetGrainStorage(this IServiceCollection services, Action<FasterGrainStorageOptions> configureOptions)
        {
            return services.AddFasterNetGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, (ob) => ob.Configure(configureOptions));
        }

        /// <summary>
        /// Configure silo to use AdoNet grain storage for grain storage. Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </summary>
        /// <remarks>
        /// Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </remarks>
        public static IServiceCollection AddFasterNetGrainStorage(this IServiceCollection services, string name, Action<FasterGrainStorageOptions> configureOptions)
        {
            return services.AddFasterNetGrainStorage(name, (ob) => ob.Configure(configureOptions));
        }

        /// <summary>
        /// Configure silo to use AdoNet grain storage as the default grain storage. Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </summary>
        /// <remarks>
        /// Instructions on configuring your database are available at <see href="http://aka.ms/orleans-sql-scripts"/>.
        /// </remarks>
        public static IServiceCollection AddFasterNetGrainStorageAsDefault(this IServiceCollection services, Action<OptionsBuilder<FasterGrainStorageOptions>> configureOptions = null)
        {
            return services.AddFasterNetGrainStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        public static IServiceCollection AddFasterNetGrainStorage(this IServiceCollection services, string name,
            Action<OptionsBuilder<FasterGrainStorageOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<FasterGrainStorageOptions>(name));

            services.TryAddSingleton<ISerializer, DefaultSerializer>();
            
            services.ConfigureNamedOptionForLogging<FasterGrainStorageOptions>(name);
            services.TryAddSingleton<IGrainStorage>(sp => sp.GetServiceByName<IGrainStorage>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
            services.AddTransient<IConfigurationValidator>(sp => new FasterGrainStorageOptionsValidator(sp.GetRequiredService<IOptionsMonitor<FasterGrainStorageOptions>>().Get(name), name));
            return services.AddSingletonNamedService<IGrainStorage>(name, FasterGrainStorageFactory.Create)
                           .AddSingletonNamedService<ILifecycleParticipant<ISiloLifecycle>>(name, (s, n) => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredServiceByName<IGrainStorage>(n));
        }
    }
}