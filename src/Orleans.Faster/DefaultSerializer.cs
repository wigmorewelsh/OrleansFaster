using System;
using System.IO;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Orleans.Persistence.Faster.Converters;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Persistence.Faster
{
    public class DefaultSerializer : ISerializer
    {
        private JsonSerializerSettings jsonSettings;

        public DefaultSerializer(IGrainReferenceConverter grainReferenceConverter)
        {
            this.jsonSettings = JsonSettings(grainReferenceConverter);
        }

        public object Deserialize(byte[] buffer, Type grainStateType)
        {
            var serializer = JsonSerializer.Create(jsonSettings);
            var reader = new StreamReader(new MemoryStream(buffer));
            var json = new JsonTextReader(reader);
            var res = serializer.Deserialize(json, grainStateType);
            return res;
        }

        public async Task<byte[]> Serialize(IGrainState grainState)
        {
            var serializer = JsonSerializer.Create(jsonSettings);
            var ms = new MemoryStream();
            var writer = new StreamWriter(ms);
            serializer.Serialize(writer, grainState.State);
            await writer.FlushAsync();
            ms.Position = 0;

            var array = ms.ToArray();
            return array;
        }

        public JsonSerializerSettings JsonSettings(IGrainReferenceConverter locator)
        {
            var converter = new OrleansGrainConverter(locator);

            var newSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Auto
            };
            newSettings.Converters.Add(new IPAddressConverter());
            newSettings.Converters.Add(new IPEndPointConverter());
            newSettings.Converters.Add(new GrainIdConverter());
            newSettings.Converters.Add(new SiloAddressConverter());
            newSettings.Converters.Add(new UniqueKeyConverter());
            newSettings.Converters.Add(converter);
            return newSettings;
        }
    }
}