using Orleans.Runtime;

namespace Orleans.Persistence.Faster.Configuration
{
    /// <summary>
    /// ConfigurationValidator for AdoNetGrainStorageOptions
    /// </summary>
    internal class FasterGrainStorageOptionsValidator : IConfigurationValidator
    {
        private readonly FasterGrainStorageOptions options;
        private readonly string name;
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="configurationOptions">The option to be validated.</param>
        /// <param name="name">The name of the option to be validated.</param>
        public FasterGrainStorageOptionsValidator(FasterGrainStorageOptions configurationOptions, string name)
        {
            if(configurationOptions == null)
                throw new OrleansConfigurationException($"Invalid AdoNetGrainStorageOptions for AdoNetGrainStorage {name}. Options is required.");
            this.options = configurationOptions;
            this.name = name;
        }
        /// <inheritdoc cref="IConfigurationValidator"/>
        public void ValidateConfiguration()
        {
            // if (string.IsNullOrWhiteSpace(this.options.ConnectionString))
            // {
            //     throw new OrleansConfigurationException($"Invalid {nameof(FasterGrainStorageOptions)} values for {nameof(FasterGrainStorage)} \"{name}\". {nameof(options.ConnectionString)} is required.");
            // }

            // if (options.UseXmlFormat&&options.UseJsonFormat)
            // {
            //     throw new OrleansConfigurationException($"Invalid {nameof(FasterGrainStorageOptions)} values for {nameof(FasterGrainStorage)} \"{name}\". {nameof(options.UseXmlFormat)} and {nameof(options.UseJsonFormat)} cannot both be set to true");
            // }
        }
    }
}