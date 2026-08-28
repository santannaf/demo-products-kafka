namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// What the consuming side reads from the "Kafka" section of appsettings.json — and nothing else. The
/// producer's keys are absent by design.
/// </summary>
/// <remarks>
/// <c>AutoRegisterSchemas</c> is missing from <see cref="SchemaRegistrySettings"/> on purpose: it
/// governs schema publication, which only the producing side does.
/// </remarks>
public sealed class KafkaConsumerOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = string.Empty;

    public string ClientId { get; set; } = string.Empty;

    public ConsumerSettings Consumer { get; set; } = new();

    public SchemaRegistrySettings SchemaRegistry { get; set; } = new();

    public TopicSettings Topics { get; set; } = new();

    public sealed class ConsumerSettings
    {
        public string GroupId { get; set; } = string.Empty;

        public string AutoOffsetReset { get; set; } = "Earliest";

        public bool EnableAutoCommit { get; set; }

        public int SessionTimeoutMs { get; set; } = 45_000;

        public int MaxPollIntervalMs { get; set; } = 300_000;

        /// <summary>
        /// How long the listener pauses after a failed handler before re-consuming the same offset, so a
        /// permanently failing message does not become a hot loop.
        /// </summary>
        public int RetryDelayMs { get; set; } = 5_000;
    }

    public sealed class SchemaRegistrySettings
    {
        public string Url { get; set; } = string.Empty;
    }

    public sealed class TopicSettings
    {
        public string ProductCreated { get; set; } = string.Empty;
    }
}
