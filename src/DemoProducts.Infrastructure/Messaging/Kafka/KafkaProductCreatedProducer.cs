using Confluent.Kafka;
using DemoProducts.Application.Abstractions.Messaging;
using DemoProducts.Domain.Events;
using DemoProducts.Infrastructure.Messaging.Kafka.SchemaRegistry;
using DemoProducts.Infrastructure.Messaging.Kafka.Wire;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// The Kafka adapter behind <see cref="ISendProductCreatedEventProvider"/>. It owns the producer for the
/// life of the process — building one is expensive and it is designed to be shared — so it is registered
/// as a singleton and flushed on dispose.
/// </summary>
/// <remarks>
/// <para>
/// The producer is built here rather than handed in: exposing an <c>IProducer&lt;string, byte[]&gt;</c>
/// would put the whole Confluent surface into this module's interface. Callers see
/// <see cref="SendAsync"/> and nothing else.
/// </para>
/// <para>
/// The value type is <c>byte[]</c> and the Avro encoding happens in
/// <see cref="ProductCreatedAvroEncoder"/> rather than in a <c>Confluent.SchemaRegistry.Serdes</c>
/// serializer. That is the whole point: Confluent's Avro serde reaches both Newtonsoft.Json and Avro's
/// reflective schema parser, which together produced 34 of the Api's 43 ILC trim warnings. The consuming
/// side still uses the Confluent serde — it runs on the CLR, where none of that is a problem, and it
/// needs schema-by-id resolution this direction does not.
/// </para>
/// </remarks>
internal sealed partial class KafkaProductCreatedProducer : ISendProductCreatedEventProvider, IDisposable
{
    private static readonly TimeSpan FlushTimeout = TimeSpan.FromSeconds(10);

    private readonly IProducer<string, byte[]> producer;
    private readonly ProductCreatedSchemaId schemaId;
    private readonly ILogger<KafkaProductCreatedProducer> logger;
    private readonly string topic;
    private bool disposed;

    public KafkaProductCreatedProducer(
        IOptions<KafkaProducerOptions> options,
        ProductCreatedSchemaId schemaId,
        ILogger<KafkaProductCreatedProducer> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(schemaId);

        this.schemaId = schemaId;
        this.logger = logger;

        var kafka = options.Value;
        topic = kafka.Topics.ProductCreated;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            ClientId = kafka.ClientId,
            Acks = Enum.Parse<Acks>(kafka.Producer.Acks, ignoreCase: true),
            EnableIdempotence = kafka.Producer.EnableIdempotence,
            MessageTimeoutMs = kafka.Producer.MessageTimeoutMs,
            MessageSendMaxRetries = kafka.Producer.MaxRetries,
            CompressionType = Enum.Parse<CompressionType>(kafka.Producer.CompressionType, ignoreCase: true),
            Partitioner = Enum.Parse<Partitioner>(kafka.Producer.Partitioner, ignoreCase: true),
        };

        producer = new ProducerBuilder<string, byte[]>(producerConfig).Build();
    }

    public async Task SendAsync(
        ProductCreatedEvent productCreatedEvent,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(productCreatedEvent);

        int resolvedSchemaId;

        try
        {
            resolvedSchemaId = await schemaId.ResolveAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (SchemaRegistryUnavailableException exception)
        {
            throw new EventPublishFailedException(
                $"Failed to register or resolve the Avro schema for topic '{topic}': {exception.Message}",
                exception);
        }
        catch (HttpRequestException exception)
        {
            throw new EventPublishFailedException(
                $"Failed to reach the Schema Registry while publishing to topic '{topic}': {exception.Message}",
                exception);
        }

        var message = new Message<string, byte[]>
        {
            Key = productCreatedEvent.ProductId.ToString(),
            Value = ProductCreatedAvroEncoder.Encode(productCreatedEvent, resolvedSchemaId),
        };

        try
        {
            var delivery = await producer.ProduceAsync(topic, message, cancellationToken).ConfigureAwait(false);

            // Logged after the await, so the line means the broker acknowledged it under the configured
            // Acks - not that a message was handed to librdkafka's queue.
            LogPublished(
                logger,
                productCreatedEvent.EventId,
                productCreatedEvent.ProductId,
                productCreatedEvent.Name,
                delivery.Topic,
                delivery.Partition.Value,
                delivery.Offset.Value);
        }
        catch (KafkaException exception)
        {
            // ProduceException<TKey, TValue> derives from KafkaException, so this one catch covers both
            // a delivery failure and a serialization failure surfaced by the producer.
            throw new EventPublishFailedException(
                $"Failed to publish ProductCreated to topic '{topic}': {exception.Error.Reason}",
                exception);
        }
    }

    // DomainEventId rather than EventId: see ProductCreatedEventHandler for why the reserved name cannot
    // be used here. The two lines share the placeholder so one event id correlates publish with consume.
    [LoggerMessage(
        EventId = 2002,
        Level = LogLevel.Information,
        Message = "ProductCreated published. DomainEventId={DomainEventId} ProductId={ProductId} Name={Name} Topic={Topic} Partition={Partition} Offset={Offset}")]
    private static partial void LogPublished(
        ILogger logger,
        Guid domainEventId,
        Guid productId,
        string name,
        string topic,
        int partition,
        long offset);

    public void Dispose()
    {
        if (disposed)
        {
            return;
        }

        // Flushing before disposing is what makes an in-flight message survive shutdown.
        producer.Flush(FlushTimeout);
        producer.Dispose();

        disposed = true;
    }
}
