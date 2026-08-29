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
        OptionsValidation.RequirePositive(
            failures, options.Consumer.MaxAttemptsPerRecord, "Kafka:Consumer:MaxAttemptsPerRecord");
        OptionsValidation.RequirePositive(failures, options.Consumer.MaxPollRecords, "Kafka:Consumer:MaxPollRecords");
        OptionsValidation.RequirePositive(failures, options.Consumer.FetchMinBytes, "Kafka:Consumer:FetchMinBytes");
        OptionsValidation.RequirePositive(failures, options.Consumer.FetchWaitMaxMs, "Kafka:Consumer:FetchWaitMaxMs");

        // A fetch the broker may hold for longer than the poll deadline starves the poll loop into a
        // rebalance while the consumer is perfectly healthy. Neither value is wrong alone, which is
        // exactly why this is checked here.
        //
        // Skipped when either operand already failed its own check: a relational failure derived from a
        // value that is independently invalid is noise, and it buries the line that names the real typo.
        if (options.Consumer.FetchWaitMaxMs > 0 &&
            options.Consumer.MaxPollIntervalMs > 0 &&
            options.Consumer.FetchWaitMaxMs >= options.Consumer.MaxPollIntervalMs)
        {
            failures.Add(
                $"Kafka:Consumer:FetchWaitMaxMs ({options.Consumer.FetchWaitMaxMs}) must be well below " +
                $"Kafka:Consumer:MaxPollIntervalMs ({options.Consumer.MaxPollIntervalMs}).");
        }

        // The setting stays configurable, as the goal asks, but the sample's contract is that the offset
        // is committed only after the handler succeeds. Auto-commit would silently break that, so it is
        // rejected at boot instead of being assumed.
        if (options.Consumer.EnableAutoCommit)
        {
            failures.Add(
                "Kafka:Consumer:EnableAutoCommit must be false: the listener commits only after the handler succeeds.");
        }

        if (options.Consumer.AsyncAck)
        {
            failures.Add(
                "Kafka:Consumer:AsyncAck must be false: the listener commits synchronously so that a " +
                "committed offset means a handled record.");
        }

        if (options.Consumer.EnableBatchListener)
        {
            failures.Add(
                "Kafka:Consumer:EnableBatchListener must be false: the listener handles one record at a " +
                "time, and Kafka:Consumer:MaxAttemptsPerRecord is counted per record.");
        }

        return failures.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(failures);
    }
}
