using DemoProducts.Infrastructure.Messaging.Kafka;

namespace DemoProducts.IntegrationTests.TestSupport;

/// <summary>
/// The two options objects the adapters bind from <c>appsettings.json</c>, pointed at the fixture.
/// </summary>
/// <remarks>
/// Built here rather than through <c>ConfigurationBuilder</c> on purpose: what these tests exercise is
/// the adapter against a broker, and binding is already covered by
/// <c>DemoProducts.UnitTests.Infrastructure.Options</c>. Going through configuration would add a second
/// thing that can fail a test without the broker being wrong.
/// </remarks>
internal static class KafkaTestOptions
{
    public static KafkaProducerOptions Producer(
        KafkaFixture fixture,
        string topic,
        bool autoRegisterSchemas = true,
        string? schemaRegistryUrl = null)
    {
        ArgumentNullException.ThrowIfNull(fixture);

        return new KafkaProducerOptions
        {
            BootstrapServers = fixture.BootstrapServers,
            ClientId = "demo-products-integration-producer",
            Producer = new KafkaProducerOptions.ProducerSettings
            {
                Acks = "All",
                EnableIdempotence = true,
                // Well under the read deadlines below, so a broker that never answers fails as a
                // delivery error naming the topic rather than as a test timeout naming nothing.
                MessageTimeoutMs = 15_000,
                MaxRetries = 3,
                CompressionType = "Snappy",
                Partitioner = "ConsistentRandom",
            },
            SchemaRegistry = new KafkaProducerOptions.SchemaRegistrySettings
            {
                Url = schemaRegistryUrl ?? fixture.SchemaRegistryUrl,
                AutoRegisterSchemas = autoRegisterSchemas,
            },
            Topics = new KafkaProducerOptions.TopicSettings { ProductCreated = topic },
        };
    }

    public static KafkaConsumerOptions Consumer(
        KafkaFixture fixture,
        string topic,
        string groupId,
        bool enableAvroReader = false,
        int maxAttemptsPerRecord = 2,
        int retryDelayMs = 200)
    {
        ArgumentNullException.ThrowIfNull(fixture);

        return new KafkaConsumerOptions
        {
            BootstrapServers = fixture.BootstrapServers,
            ClientId = "demo-products-integration-consumer",
            Consumer = new KafkaConsumerOptions.ConsumerSettings
            {
                GroupId = groupId,

                // Earliest, where the shipped configuration says Latest. Every group id here is new, so
                // this makes the order between producing and subscribing irrelevant instead of racing
                // partition assignment - the same reasoning the produce smoke in ci.yml records.
                AutoOffsetReset = "Earliest",

                EnableAutoCommit = false,
                SessionTimeoutMs = 10_000,
                MaxPollIntervalMs = 300_000,
                MaxPollRecords = 1_000,

                // The shipped 200_000/400 pair buys throughput on a busy topic. Here it would only mean
                // every single-record read waits for the fetch to time out.
                FetchMinBytes = 1,
                FetchWaitMaxMs = 100,

                RetryDelayMs = retryDelayMs,
                MaxAttemptsPerRecord = maxAttemptsPerRecord,
                EnableAvroReader = enableAvroReader,
                AsyncAck = false,
                EnableBatchListener = false,
            },
            SchemaRegistry = new KafkaConsumerOptions.SchemaRegistrySettings { Url = fixture.SchemaRegistryUrl },
            Topics = new KafkaConsumerOptions.TopicSettings { ProductCreated = topic },
        };
    }
}
