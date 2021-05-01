using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans.Persistence.Faster
{
    public class CancellableTaskCollection : IAsyncDisposable
    {
        private List<CancellableTask> _jobs = new List<CancellableTask>();
        
        public CancellableTask Run(Func<CancellationToken, Task> action, TaskCreationOptions taskCreationOptions = TaskCreationOptions.None, CancellationToken cancellationToken = default)
        {
            var job = CancellableTask.Run(action, taskCreationOptions, cancellationToken);
            _jobs.Add(job);
            return job;
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var job in _jobs)
            {
                await job.DisposeAsync();
            }
        }
    }
}