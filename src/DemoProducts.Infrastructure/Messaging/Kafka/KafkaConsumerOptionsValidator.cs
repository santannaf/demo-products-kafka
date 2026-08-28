using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <inheritdoc cref="KafkaProducerOptionsValidator"/>
internal sealed class KafkaConsumerOptionsValidator : IValidateOptions<KafkaConsumerOptions>
{
    public ValidateOptionsResult Validate(string? name, KafkaConsumerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();

        OptionsValidation.RequireValue(failures, options.BootstrapServers, "Kafka:BootstrapServers");
        OptionsValidation.RequireValue(failures, options.ClientId, "Kafka:ClientId");
        OptionsValidation.RequireValue(failures, options.SchemaRegistry.Url, "Kafka:SchemaRegistry:Url");
        OptionsValidation.RequireValue(failures, options.Topics.ProductCreated, "Kafka:Topics:ProductCreated");
        OptionsValidation.RequireValue(failures, options.Consumer.GroupId, "Kafka:Consumer:GroupId");

        OptionsValidation.RequireEnum<AutoOffsetReset>(
            failures, options.Consumer.AutoOffsetReset, "Kafka:Consumer:AutoOffsetReset");

        OptionsValidation.RequirePositive(failures, options.Consumer.SessionTimeoutMs, "Kafka:Consumer:SessionTimeoutMs");
        OptionsValidation.RequirePositive(failures, options.Consumer.MaxPollIntervalMs, "Kafka:Consumer:MaxPollIntervalMs");
        OptionsValidation.RequirePositive(failures, options.Consumer.RetryDelayMs, "Kafka:Consumer:RetryDelayMs");

        // The setting stays configurable, as the goal asks, but the sample's contract is that the offset
        // is committed only after the handler succeeds. Auto-commit would silently break that, so it is
        // rejected at boot instead of being assumed.
        if (options.Consumer.EnableAutoCommit)
        {
            failures.Add(
                "Kafka:Consumer:EnableAutoCommit must be false: the listener commits only after the handler succeeds.");
        }

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }
}
