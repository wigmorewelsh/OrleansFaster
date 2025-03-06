using System.Threading.Tasks;

namespace Orleans.Faster.Tests.Grains;

public interface ISampleGrain : IGrainWithGuidKey
{
    Task Store(string data);
    Task<string> Fetch();
}