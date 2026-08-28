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

    [LoggerMessage(
        EventId = 1001,
        Level = LogLevel.Information,
        Message = "ProductCreated consumed. EventId={EventId} ProductId={ProductId} Name={Name} OccurredAt={OccurredAt}")]
    private static partial void LogProductCreatedConsumed(
        ILogger logger,
        Guid eventId,
        Guid productId,
        string name,
        DateTime occurredAt);
}
