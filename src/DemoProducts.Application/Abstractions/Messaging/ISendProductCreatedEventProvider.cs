using DemoProducts.Domain.Events;

namespace DemoProducts.Application.Abstractions.Messaging;

/// <summary>
/// Outbound port for publishing <see cref="ProductCreatedEvent"/>. No Kafka, Avro or Schema Registry
/// type may appear in this signature: it is the seam that keeps the broker out of Application.
/// </summary>
public interface ISendProductCreatedEventProvider
{
    Task SendAsync(ProductCreatedEvent productCreatedEvent, CancellationToken cancellationToken = default);
}
