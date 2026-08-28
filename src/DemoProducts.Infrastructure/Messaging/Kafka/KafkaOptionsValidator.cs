using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// Hand-written on purpose: <c>ValidateDataAnnotations()</c> and <c>IValidatableObject</c> both reach
/// <c>[RequiresUnreferencedCode]</c> code, and under trimming the first can end up validating nothing.
/// Explicit checks are reflection-free and name the offending configuration key.
/// </summary>
internal sealed class KafkaOptionsValidator : IValidateOptions<KafkaOptions>
{
    public ValidateOptionsResult Validate(string? name, KafkaOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();

        RequireValue(failures, options.BootstrapServers, "Kafka:BootstrapServers");
        RequireValue(failures, options.ClientId, "Kafka:ClientId");
        RequireValue(failures, options.SchemaRegistry.Url, "Kafka:SchemaRegistry:Url");
        RequireValue(failures, options.Topics.ProductCreated, "Kafka:Topics:ProductCreated");
        RequireValue(failures, options.Consumer.GroupId, "Kafka:Consumer:GroupId");

        RequireEnum<Acks>(failures, options.Producer.Acks, "Kafka:Producer:Acks");
        RequireEnum<AutoOffsetReset>(failures, options.Consumer.AutoOffsetReset, "Kafka:Consumer:AutoOffsetReset");

        RequirePositive(failures, options.Producer.MessageTimeoutMs, "Kafka:Producer:MessageTimeoutMs");
        RequirePositive(failures, options.Consumer.SessionTimeoutMs, "Kafka:Consumer:SessionTimeoutMs");
        RequirePositive(failures, options.Consumer.MaxPollIntervalMs, "Kafka:Consumer:MaxPollIntervalMs");
        RequirePositive(failures, options.Consumer.RetryDelayMs, "Kafka:Consumer:RetryDelayMs");

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

    private static void RequireValue(List<string> failures, string value, string key)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            failures.Add($"{key} is required.");
        }
    }

    private static void RequirePositive(List<string> failures, int value, string key)
    {
        if (value <= 0)
        {
            failures.Add($"{key} must be greater than zero.");
        }
    }

    private static void RequireEnum<TEnum>(List<string> failures, string value, string key)
        where TEnum : struct, Enum
    {
        if (!Enum.TryParse<TEnum>(value, ignoreCase: true, out _))
        {
            failures.Add($"{key} has an unsupported value '{value}'. Allowed: {string.Join(", ", Enum.GetNames<TEnum>())}.");
        }
    }
}
