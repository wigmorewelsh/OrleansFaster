using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Persistence.Faster;

namespace Sample
{
    class Program
    {
        public static async Task<int> Main(string[] args)
        {            
            var main = CreateHost(args).Build();
            await main.RunAsync();
            return 0;
        }

        public static IHostBuilder CreateHost(string[] args)
        {
            return new HostBuilder()
                .ConfigureLogging(logging =>
                {
                    // logging.ClearProviders();
                    logging.SetMinimumLevel(LogLevel.Error);
                    logging.AddConsole();
                })
                .UseOrleans(orleans =>
                {
                    orleans
                        // .ConfigureDefaults()
                        .UseLocalhostClustering()
                        .Configure<ClusterOptions>(options =>
                        {
                            options.ClusterId = "dev";
                            options.ServiceId = "OrleansBasics";
                        });

                    orleans.AddFasterNetGrainStorage("Default");

                    orleans.ConfigureApplicationParts(parts =>
                        parts.AddApplicationPart(typeof(HelloGrain).Assembly));
                })
                .UseOrleansRepl();
        }
    }

    public static class HostBuilderExtensions
    {
        public static IHostBuilder UseOrleansRepl(this IHostBuilder builder)
        {
            return builder.ConfigureServices(services =>
            {
                services.AddHostedService<ConsoleService>();
            });
        } 
    }
}
