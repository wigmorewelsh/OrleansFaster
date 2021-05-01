using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
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
          
            _logger.Info("Starting batch");
            

            for (int i = 0; i < 50_000; i++)
            {
                var grain2 = _client.GetGrain<IHelloGrain>(i);
                await grain2.DoOne();
            
            
                for (int j = 0; j < 10; j++)
                {
                    grain2.DoOne();
                    // await Task.Delay(1);
                    // var current2 = await grain2.Current();
                }
            }
            
            var grain = _client.GetGrain<IHelloGrain>(0);

            var current = await grain.Current();
            _logger.Info("Current value {current}", current);

            _logger.Info("Complete batch");
 

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