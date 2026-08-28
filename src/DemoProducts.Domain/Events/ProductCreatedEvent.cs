using DemoProducts.Domain.Products;

namespace DemoProducts.Domain.Events;

/// <summary>
/// The event published when a product is created. <paramref name="OccurredAtUtc"/> always carries
/// <see cref="DateTimeKind.Utc"/>: the Avro timestamp-millis logical type converts through
/// <see cref="DateTime.ToUniversalTime"/>, so any other kind would silently shift the instant.
/// </summary>
public sealed record ProductCreatedEvent(
    Guid EventId,
    Guid ProductId,
    string Name,
    DateTime OccurredAtUtc)
{
    public static ProductCreatedEvent From(Product product, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(product);
        ArgumentNullException.ThrowIfNull(timeProvider);

        return new ProductCreatedEvent(
            Guid.NewGuid(),
            product.Id,
            product.Name,
            timeProvider.GetUtcNow().UtcDateTime);
    }
}
