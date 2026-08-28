using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DemoProducts.Application.Abstractions.Messaging;
using DemoProducts.Domain.Events;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Generated;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Mappers;
using Microsoft.Extensions.Options;

namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// The Kafka adapter behind <see cref="ISendProductCreatedEventProvider"/>. It owns the producer for the
/// life of the process — building one is expensive and it is designed to be shared — so it is registered
/// as a singleton and flushed on dispose.
/// </summary>
/// <remarks>
/// The producer is built here rather than handed in: exposing an <c>IProducer&lt;string,
/// ProductCreatedAvro&gt;</c> would put the whole Confluent surface, and the generated Avro record, into
/// this module's interface. Callers see <see cref="SendAsync"/> and nothing else.
/// </remarks>
internal sealed class KafkaProductCreatedProducer : ISendProductCreatedEventProvider, IDisposable
{
    private static readonly TimeSpan FlushTimeout = TimeSpan.FromSeconds(10);

    private readonly IProducer<string, ProductCreatedAvro> producer;
    private readonly string topic;
    private bool disposed;

    public KafkaProductCreatedProducer(
        IOptions<KafkaOptions> options,
        ISchemaRegistryClient schemaRegistryClient)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(schemaRegistryClient);

        var kafka = options.Value;
        topic = kafka.Topics.ProductCreated;

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

        producer = new ProducerBuilder<string, ProductCreatedAvro>(producerConfig)
            .SetValueSerializer(new AvroSerializer<ProductCreatedAvro>(schemaRegistryClient, serializerConfig))
            .Build();
    }

    public async Task SendAsync(
        ProductCreatedEvent productCreatedEvent,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(productCreatedEvent);

        var message = new Message<string, ProductCreatedAvro>
        {
            Key = productCreatedEvent.ProductId.ToString(),
            Value = ProductCreatedAvroMapper.ToAvro(productCreatedEvent),
        };

        try
        {
            await producer.ProduceAsync(topic, message, cancellationToken).ConfigureAwait(false);
        }
        catch (KafkaException exception)
        {
            // ProduceException<TKey, TValue> derives from KafkaException, so this one catch covers both
            // a delivery failure and a serialization failure surfaced by the producer.
            throw new EventPublishFailedException(
                $"Failed to publish ProductCreated to topic '{topic}': {exception.Error.Reason}",
                exception);
        }
        catch (SchemaRegistryException exception)
        {
            throw new EventPublishFailedException(
                $"Failed to register or resolve the Avro schema for topic '{topic}': {exception.Message}",
                exception);
        }
    }

    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        // Flush before disposing so messages still in flight at shutdown are not dropped. The schema
        // registry client is not disposed here: the container owns it.
        producer.Flush(FlushTimeout);
        producer.Dispose();

        disposed = true;
    }
}
