namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// The "Kafka" section of appsettings.json. Every broker URL, topic name, group id and timeout lives
/// here rather than in code; the defaults below are only safety nets for the numeric knobs.
/// </summary>
public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = string.Empty;

    public string ClientId { get; set; } = string.Empty;

    public KafkaProducerOptions Producer { get; set; } = new();

    public KafkaConsumerOptions Consumer { get; set; } = new();

    public SchemaRegistryOptions SchemaRegistry { get; set; } = new();

    public KafkaTopicsOptions Topics { get; set; } = new();
}

public sealed class KafkaProducerOptions
{
    public string Acks { get; set; } = "All";

    public bool EnableIdempotence { get; set; } = true;

    public int MessageTimeoutMs { get; set; } = 30_000;
}

public sealed class KafkaConsumerOptions
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

public sealed class SchemaRegistryOptions
{
    public string Url { get; set; } = string.Empty;

    public bool AutoRegisterSchemas { get; set; } = true;
}

public sealed class KafkaTopicsOptions
{
    public string ProductCreated { get; set; } = string.Empty;
}
