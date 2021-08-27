using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Orleans.Persistence.Faster.Converters;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Persistence.Faster
{
    public class DefaultSerializer : ISerializer
    {
        private JsonSerializerSettings jsonSettings;
        private RecyclableMemoryStreamManager _manager = new RecyclableMemoryStreamManager();
        private JsonSerializer serializer;

        public DefaultSerializer(IGrainReferenceConverter grainReferenceConverter)
        {
            this.jsonSettings = JsonSettings(grainReferenceConverter);
            serializer = JsonSerializer.Create(jsonSettings);
        }

        public object Deserialize(Memory<byte> buffer, Type grainStateType)
        {
            return SpanJson.JsonSerializer.NonGeneric.Utf8.Deserialize(buffer.Span, grainStateType);
        }

        public async Task<ArraySegment<byte>> Serialize(IGrainState grainState)
        {
            return SpanJson.JsonSerializer.NonGeneric.Utf8.SerializeToArrayPool(grainState.State);
        }

        public JsonSerializerSettings JsonSettings(IGrainReferenceConverter locator)
        {
            var converter = new OrleansGrainConverter(locator);

            var newSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.Auto,
                SerializationBinder = new DefaultSerializationBinder()
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