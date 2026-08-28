using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// Hand-written on purpose: <c>ValidateDataAnnotations()</c> and <c>IValidatableObject</c> both reach
/// <c>[RequiresUnreferencedCode]</c> code, and under trimming the first can end up validating nothing.
/// Explicit checks are reflection-free and name the offending configuration key.
/// </summary>
internal sealed class KafkaProducerOptionsValidator : IValidateOptions<KafkaProducerOptions>
{
    public ValidateOptionsResult Validate(string? name, KafkaProducerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();

        OptionsValidation.RequireValue(failures, options.BootstrapServers, "Kafka:BootstrapServers");
        OptionsValidation.RequireValue(failures, options.ClientId, "Kafka:ClientId");
        OptionsValidation.RequireValue(failures, options.SchemaRegistry.Url, "Kafka:SchemaRegistry:Url");
        OptionsValidation.RequireValue(failures, options.Topics.ProductCreated, "Kafka:Topics:ProductCreated");

        OptionsValidation.RequireEnum<Acks>(failures, options.Producer.Acks, "Kafka:Producer:Acks");
        OptionsValidation.RequirePositive(failures, options.Producer.MessageTimeoutMs, "Kafka:Producer:MessageTimeoutMs");

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }
}
