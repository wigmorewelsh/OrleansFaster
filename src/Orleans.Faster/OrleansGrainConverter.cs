using System;
using System.Globalization;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Orleans.Runtime;
using Orleans.Serialization;

namespace Orleans.Persistence.Faster.Converters
{
    /// <summary>
    /// Store references to orleans grains without type information
    /// </summary>
    public class OrleansGrainConverter : JsonConverter
    {
        private readonly IGrainReferenceConverter grainReferenceConverter;

        public OrleansGrainConverter(IGrainReferenceConverter grainReferenceConverter)
        {
            this.grainReferenceConverter = grainReferenceConverter;
        }

        public override object ReadJson(JsonReader reader, Type objectType, object? existingValue,
            JsonSerializer serializer)
        {
            GrainReferenceKeyInfo ToGrainKeyInfo(string grainId, string? observerId)
            {
                var grainKey = Parse(grainId);
                if (objectType.IsGenericType)
                {
                    var genericArgument = objectType.FullName?.Substring(objectType.FullName.IndexOf('['));
                    genericArgument = genericArgument?.Substring(1, genericArgument.Length - 2);

                    return new GrainReferenceKeyInfo(grainKey, genericArgument);
                }

                if (!string.IsNullOrWhiteSpace(observerId) && Guid.TryParse(observerId, out var guid))
                {
                    return new GrainReferenceKeyInfo(grainKey, guid);
                }

                return new GrainReferenceKeyInfo(grainKey);
            }

            if (reader.TokenType == JsonToken.Null) return existingValue!;
            var jo = JObject.Load(reader);
            var grainId = jo["GrainId"]?.ToObject<string>();
            var observerId = jo["ObserverId"]?.ToObject<string>();

            if(grainId == null)
                throw new ArgumentException("Expected field missing", "GrainId");

            var key = ToGrainKeyInfo(grainId, observerId);
            var reference = grainReferenceConverter.GetGrainFromKeyInfo(key);
            return reference.Cast(objectType);
        }

        public override void WriteJson(JsonWriter writer, object? value, JsonSerializer serializer)
        {
            var grain = value as GrainReference;
            var keyInfo = grain!.ToKeyInfo();
            var grainReference = ToHexString(keyInfo.Key);

            writer.WriteStartObject();
            writer.WritePropertyName("TypeName");
            writer.WriteValue(value?.GetType().Name);
            writer.WritePropertyName("GrainId");
            writer.WriteValue(grainReference);
            if (keyInfo.HasObserverId)
            {
                writer.WritePropertyName("ObserverId");
                writer.WriteValue(keyInfo.ObserverId.ToString());
            }

            writer.WriteEndObject();
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(IGrain).IsAssignableFrom(objectType);
        }

        private string ToHexString((ulong N0, ulong N1, ulong TypeCodeData, string KeyExt) key)
        {
            var s = new StringBuilder();
            var (n0, n1, typeCodeData, keyExt) = key;
            s.AppendFormat("{0:x16}{1:x16}{2:x16}", n0, n1, typeCodeData);
            if (string.IsNullOrWhiteSpace(keyExt)) return s.ToString();

            s.Append("+");
            s.Append(keyExt ?? "null");
            return s.ToString();
        }

        private static readonly char[] KeyExtSeparationChar = {'+'};

        private static (ulong N0, ulong N1, ulong TypeCodeData, string? KeyExt) Parse(ReadOnlySpan<char> input)
        {
            var trimmed = input.Trim().ToString();

            var fields = trimmed.Split(KeyExtSeparationChar, 2);
            var n0           = ulong.Parse(fields[0][00..16], NumberStyles.HexNumber);
            var n1           = ulong.Parse(fields[0][16..32], NumberStyles.HexNumber);
            var typeCodeData = ulong.Parse(fields[0][32..48], NumberStyles.HexNumber);
            string? keyExt = null;
            switch (fields.Length)
            {
                default:
                    throw new InvalidDataException("UniqueKey hex strings cannot contain more than one + separator.");
                case 1:
                    break;
                case 2:
                    if (fields[1] != "null")
                    {
                        keyExt = fields[1];
                    }

                    break;
            }

            return (n0, n1, typeCodeData, keyExt);
        }
    }
}