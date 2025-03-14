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

namespace Sample;

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
        var store = _client.ServiceProvider.GetRequiredService<IGrainStorage>();

        _logger.LogInformation("Starting batch");
        var sw = Stopwatch.StartNew();

        var tasks = new List<Task>();
        for (int j = 0; j < 2; j++)
        {
            tasks.Add(Task.Run(async () =>
            {
                for (int i = 0; i < 100 / 2; i++)
                {
                    var grain2 = _client.GetGrain<IHelloGrain>(i + j);
                    await grain2.DoOne();
                }
            }));
        }

        await Task.WhenAll(tasks);

        var grain = _client.GetGrain<IHelloGrain>(0);

        var current = await grain.Current();
        _logger.LogInformation("Current value {current}", current);

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
                _logger.LogInformation("Failed to stop task in time");
            }
        }
        catch (Exception err)
        {
            _logger.LogError(err, err.Message);
        }
    }
}