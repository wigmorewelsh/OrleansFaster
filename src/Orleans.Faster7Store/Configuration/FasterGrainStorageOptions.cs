using System;
using Newtonsoft.Json;

namespace Orleans.Faster
{
    public class FasterGrainStorageOptions
    {
        [Redact] public string StorageBaseDirectory { get; set; } = "data";

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