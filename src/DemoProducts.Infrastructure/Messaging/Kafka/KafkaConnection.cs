using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Generated;
using Microsoft.Extensions.Options;

namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// The long-lived, reusable Kafka connection: one Schema Registry client and one producer for the whole
/// process. Both are expensive to build and are designed to be shared, so this is registered as a
/// singleton and disposed with the host.
/// </summary>
public sealed class KafkaConnection : IDisposable
{
    private static readonly TimeSpan FlushTimeout = TimeSpan.FromSeconds(10);

    private readonly CachedSchemaRegistryClient schemaRegistryClient;
    private bool disposed;

    public KafkaConnection(IOptions<KafkaOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var kafka = options.Value;

        schemaRegistryClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig
        {
            Url = kafka.SchemaRegistry.Url,
        });

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            ClientId = kafka.ClientId,
            Acks = Enum.Parse<Acks>(kafka.Producer.Acks, ignoreCase: true),
            EnableIdempotence = kafka.Producer.EnableIdempotence,
            MessageTimeoutMs = kafka.Producer.MessageTimeoutMs,
        };

        var serializerConfig = new AvroSerializerConfig
        {
            AutoRegisterSchemas = kafka.SchemaRegistry.AutoRegisterSchemas,
        };

        Producer = new ProducerBuilder<string, ProductCreatedAvro>(producerConfig)
            .SetValueSerializer(new AvroSerializer<ProductCreatedAvro>(schemaRegistryClient, serializerConfig))
            .Build();
    }

    public IProducer<string, ProductCreatedAvro> Producer { get; }

    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        // Flush before disposing so messages still in flight at shutdown are not dropped.
        Producer.Flush(FlushTimeout);
        Producer.Dispose();
        schemaRegistryClient.Dispose();

        disposed = true;
    }
}
