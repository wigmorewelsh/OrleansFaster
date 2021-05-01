using System;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Persistence.Faster
{
    public class CancellableTask : IAsyncDisposable
    {
        private CancellationTokenSource _token;
        private Task _task;

        private CancellableTask(Func<CancellationToken, Task> action, TaskCreationOptions taskCreationOptions,
            CancellationToken cancellationToken = default)
        {
            _token = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            _task = Task.Factory.StartNew(() => action(_token.Token), taskCreationOptions).Unwrap();
        }

        public static CancellableTask Run(Func<CancellationToken, Task> task, TaskCreationOptions taskCreationOptions,
            CancellationToken cancellationToken = default)
        {
            return new CancellableTask(task, taskCreationOptions, cancellationToken);
        }

        public async ValueTask DisposeAsync()
        {
            _token.Cancel();
            var delay = Task.Delay(100);
            try
            {
                var res = await Task.WhenAny(_task, delay);
                if(res == delay)
                    _task.Ignore();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}