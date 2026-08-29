using DemoProducts.Domain.Events;
using Microsoft.Extensions.Logging;

namespace DemoProducts.Application.Events.ProductCreated;

public sealed partial class ProductCreatedEventHandler(ILogger<ProductCreatedEventHandler> logger)
{
    public Task HandleAsync(
        ProductCreatedEvent productCreatedEvent,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(productCreatedEvent);
        cancellationToken.ThrowIfCancellationRequested();

        LogProductCreatedConsumed(
            logger,
            productCreatedEvent.EventId,
            productCreatedEvent.ProductId,
            productCreatedEvent.Name,
            productCreatedEvent.OccurredAtUtc);

        return Task.CompletedTask;
    }

    // The placeholder is DomainEventId, not EventId: `EventId` is a reserved logging property, already
    // occupied by the [LoggerMessage] id above. Naming a template argument EventId does not override it —
    // the reserved one wins and the line renders `EventId={"Id": 1001, "Name": "..."}` instead of the id
    // of the event that was consumed, which is the one piece of correlation this line exists to carry.
    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Information,
        Message = "ProductCreated consumed. DomainEventId={DomainEventId} ProductId={ProductId} Name={Name} OccurredAt={OccurredAt}")]
    private static partial void LogProductCreatedConsumed(
        ILogger logger,
        Guid domainEventId,
        Guid productId,
        string name,
        DateTime occurredAt);
}
