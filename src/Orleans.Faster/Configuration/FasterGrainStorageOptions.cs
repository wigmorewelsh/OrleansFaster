using System;
using Newtonsoft.Json;

namespace Orleans.Faster
{
    public class FasterGrainStorageOptions
    {
        /// <summary>
        /// Connection string for AdoNet storage.
        /// </summary>
        [Redact]
        public string ConnectionString { get; set; }

        /// <summary>
        /// Stage of silo lifecycle where storage should be initialized.  Storage must be initialized prior to use.
        /// </summary>
        public int InitStage { get; set; } = DEFAULT_INIT_STAGE;
        /// <summary>
        /// Default init stage in silo lifecycle.
        /// </summary>
        public const int DEFAULT_INIT_STAGE = ServiceLifecycleStage.ApplicationServices;

        public Action<JsonSerializerSettings> ConfigureJsonSerializerSettings { get; set; }
    }
}