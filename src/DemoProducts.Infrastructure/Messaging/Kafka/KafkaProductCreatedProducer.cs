using Confluent.Kafka;
using Confluent.SchemaRegistry;
using DemoProducts.Application.Abstractions.Messaging;
using DemoProducts.Domain.Events;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Generated;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Mappers;
using Microsoft.Extensions.Options;

namespace DemoProducts.Infrastructure.Messaging.Kafka;

internal sealed class KafkaProductCreatedProducer(
    KafkaConnection connection,
    IOptions<KafkaOptions> options) : ISendProductCreatedEventProvider
{
    private readonly string topic = options.Value.Topics.ProductCreated;

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
            await connection.Producer
                .ProduceAsync(topic, message, cancellationToken)
                .ConfigureAwait(false);
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
}
