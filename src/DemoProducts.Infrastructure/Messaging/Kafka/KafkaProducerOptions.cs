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

        /// <summary>
        /// librdkafka's <c>message.send.max.retries</c>: how many times a produce request is retried
        /// before the delivery report reports failure.
        /// </summary>
        /// <remarks>
        /// Bounded only because <see cref="EnableIdempotence"/> is on. Without idempotence a retry can
        /// duplicate or reorder a message, and the safe retry count would be zero.
        /// </remarks>
        public int MaxRetries { get; set; } = 3;

        /// <summary>
        /// One of Confluent's <c>CompressionType</c> values: None, Gzip, Snappy, Lz4, Zstd.
        /// </summary>
        public string CompressionType { get; set; } = "Snappy";

        /// <summary>
        /// One of Confluent's <c>Partitioner</c> values: Random, Consistent, ConsistentRandom, Murmur2,
        /// Murmur2Random.
        /// </summary>
        /// <remarks>
        /// There is no librdkafka equivalent of the Java client's <c>UniformStickyPartitioner</c>, which
        /// batches keyless messages onto one partition at a time. <c>ConsistentRandom</c> is the closest
        /// available and is librdkafka's own default: keyed messages hash to a stable partition, keyless
        /// ones go to a random one. It makes no difference to this application either way — every message
        /// carries the product id as its key, so the keyed branch is the only one ever taken.
        /// </remarks>
        public string Partitioner { get; set; } = "ConsistentRandom";
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
