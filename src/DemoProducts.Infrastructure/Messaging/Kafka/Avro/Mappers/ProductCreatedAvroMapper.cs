using DemoProducts.Domain.Events;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Generated;

namespace DemoProducts.Infrastructure.Messaging.Kafka.Avro.Mappers;

/// <summary>
/// The only place where the application event and the Avro wire contract know about each other.
/// </summary>
public static class ProductCreatedAvroMapper
{
    public static ProductCreatedAvro ToAvro(ProductCreatedEvent productCreatedEvent)
    {
        ArgumentNullException.ThrowIfNull(productCreatedEvent);

        return new ProductCreatedAvro
        {
            EventId = productCreatedEvent.EventId.ToString(),
            ProductId = productCreatedEvent.ProductId.ToString(),
            Name = productCreatedEvent.Name,

            // Avro's timestamp-millis converts through DateTime.ToUniversalTime(). A DateTime whose Kind
            // is Unspecified or Local would be reinterpreted as local time and silently shift the
            // instant on the wire, so the kind is pinned here rather than trusted.
            OccurredAtUtc = DateTime.SpecifyKind(productCreatedEvent.OccurredAtUtc, DateTimeKind.Utc),
        };
    }

    public static ProductCreatedEvent ToEvent(ProductCreatedAvro productCreatedAvro)
    {
        ArgumentNullException.ThrowIfNull(productCreatedAvro);

        return new ProductCreatedEvent(
            Guid.Parse(productCreatedAvro.EventId),
            Guid.Parse(productCreatedAvro.ProductId),
            productCreatedAvro.Name,
            DateTime.SpecifyKind(productCreatedAvro.OccurredAtUtc, DateTimeKind.Utc));
    }
}
