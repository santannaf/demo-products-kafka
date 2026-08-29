using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Generated;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Mappers;
using Microsoft.Extensions.Logging;
using GenericRecord = Avro.Generic.GenericRecord;

namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// The single place that reads <c>Kafka:Consumer:EnableAvroReader</c> and turns it into a closed
/// subscription. Keeping the branch here is what lets
/// <see cref="KafkaProductCreatedSubscription{TValue}"/> stay unaware that the choice exists.
/// </summary>
internal static class ProductCreatedSubscriptionFactory
{
    public static IKafkaProductCreatedSubscription Create(
        KafkaConsumerOptions kafka,
        ISchemaRegistryClient schemaRegistryClient,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(kafka);
        ArgumentNullException.ThrowIfNull(schemaRegistryClient);

        // AsSyncOverAsync on both branches for the same reason: librdkafka's Consume is synchronous, and
        // the bridge belongs in the adapter rather than in the delivery protocol.
        return kafka.Consumer.EnableAvroReader
            ? new KafkaProductCreatedSubscription<ProductCreatedAvro>(
                kafka,
                new AvroDeserializer<ProductCreatedAvro>(schemaRegistryClient, new AvroDeserializerConfig())
                    .AsSyncOverAsync(),
                ProductCreatedAvroMapper.ToEvent,
                logger)
            : new KafkaProductCreatedSubscription<GenericRecord>(
                kafka,
                new AvroDeserializer<GenericRecord>(schemaRegistryClient, new AvroDeserializerConfig())
                    .AsSyncOverAsync(),
                ProductCreatedGenericRecordMapper.ToEvent,
                logger);
    }
}
