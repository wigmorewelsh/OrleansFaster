using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Orleans.Storage;
using Spectre.Console;

namespace Sample
{
    internal class ConsoleService : IHostedService
    {
        private readonly ILogger<ConsoleService> _logger;
        private readonly IClusterClient _client;
        private CancellationTokenSource cancellationTokenSource;
        private Task task;

        public ConsoleService(ILogger<ConsoleService> logger, IClusterClient client)
        {
            _logger = logger;
            _client = client;
        }

        public Task StartAsync(CancellationToken cancellationToken)
        {
            cancellationTokenSource = new CancellationTokenSource();
            task = Task.Factory.StartNew(() => ConsoleLoop(cancellationTokenSource.Token), TaskCreationOptions.None);
            return Task.CompletedTask;
        }

        private async Task ConsoleLoop(CancellationToken cancellationToken)
        {
            await _client.Connect();
            var store = _client.ServiceProvider.GetRequiredService<IGrainStorage>();

            _logger.Info("Starting batch");
            var sw = Stopwatch.StartNew();

            var tasks = new List<Task>();
            for (int j = 0; j < 200; j++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    for (int i = 0; i < 50_000 / 2; i++)
                    {
                        var grain2 = _client.GetGrain<IHelloGrain>(i + j);
                        await grain2.DoOne();
                    }
                }));
            }

            await Task.WhenAll(tasks);

            var grain = _client.GetGrain<IHelloGrain>(0);

            var current = await grain.Current();
            _logger.Info("Current value {current}", current);

            _logger.LogError($"Complete batch in {sw.Elapsed}");
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            cancellationTokenSource.Cancel();
            var delay = Task.Delay(300);
            try
            {
                var result = await Task.WhenAny(task, delay);
                if (result == delay)
                {
                    _logger.Info("Failed to stop task in time");
                }
            }
            catch (Exception err)
            {
                _logger.LogError(err, err.Message);
            }
        }
    }
}