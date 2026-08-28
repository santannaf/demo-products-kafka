namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// What the publishing side reads from the "Kafka" section of appsettings.json — and nothing else. The
/// consumer's keys are absent by design: a host that never polls should not fail to boot over a missing
/// group id.
/// </summary>
/// <remarks>
/// The nesting mirrors the configuration file exactly, so <c>Kafka:Producer:Acks</c> stays
/// <c>Kafka:Producer:Acks</c>. Splitting the options changed which host reads which key, not the keys.
/// </remarks>
public sealed class KafkaProducerOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = string.Empty;

    public string ClientId { get; set; } = string.Empty;

    public ProducerSettings Producer { get; set; } = new();

    public SchemaRegistrySettings SchemaRegistry { get; set; } = new();

    public TopicSettings Topics { get; set; } = new();

    public sealed class ProducerSettings
    {
        public string Acks { get; set; } = "All";

        public bool EnableIdempotence { get; set; } = true;

        public int MessageTimeoutMs { get; set; } = 30_000;
    }

    public sealed class SchemaRegistrySettings
    {
        public string Url { get; set; } = string.Empty;

        public bool AutoRegisterSchemas { get; set; } = true;
    }

    public sealed class TopicSettings
    {
        public string ProductCreated { get; set; } = string.Empty;
    }
}
