using DemoProducts.Domain.Events;
using Microsoft.Extensions.Logging;

namespace DemoProducts.Infrastructure.Messaging.Delivery;

/// <summary>
/// The at-least-once delivery protocol: read, hand to the application, and commit only if that
/// succeeded — otherwise rewind and pause. Knows nothing about Kafka, Avro, topics or consumer groups.
/// </summary>
/// <remarks>
/// This is the whole reason <see cref="IProductCreatedSubscription"/> exists. Dropping the
/// <see cref="IProductCreatedSubscription.SeekBack"/> call below breaks nothing loudly — messages are
/// simply skipped for the rest of the session — so the invariant is defended by tests against a fake
/// subscription rather than by a broker.
/// </remarks>
internal sealed partial class AtLeastOnceDelivery(ILogger<AtLeastOnceDelivery> logger)
{
    /// <summary>
    /// Runs until <paramref name="cancellationToken"/> is cancelled, then returns normally.
    /// <paramref name="handle"/> signals failure by throwing.
    /// </summary>
    public void Run(
        IProductCreatedSubscription subscription,
        Action<ProductCreatedEvent, CancellationToken> handle,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(subscription);
        ArgumentNullException.ThrowIfNull(handle);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var received = subscription.TryRead(cancellationToken);

                if (received is null)
                {
                    continue;
                }

                if (TryHandle(received, handle, cancellationToken))
                {
                    subscription.Commit(received);
                    continue;
                }

                subscription.SeekBack(received);
                subscription.PauseBeforeRetry(cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            LogStopping(logger);
        }
    }

    private bool TryHandle(
        ReceivedProductCreated received,
        Action<ProductCreatedEvent, CancellationToken> handle,
        CancellationToken cancellationToken)
    {
        try
        {
            handle(received.Event, cancellationToken);
            return true;
        }
        catch (OperationCanceledException)
        {
            // Shutdown, not a handler failure: let Run's catch end the loop without rewinding.
            throw;
        }
        catch (Exception exception)
        {
            LogHandleFailed(logger, received.PositionDescription, exception);
            return false;
        }
    }

    [LoggerMessage(EventId = 3003, Level = LogLevel.Error, Message = "Handler failed for offset {TopicPartitionOffset}; the offset was not committed and will be re-consumed.")]
    private static partial void LogHandleFailed(ILogger logger, string topicPartitionOffset, Exception exception);

    [LoggerMessage(EventId = 3005, Level = LogLevel.Information, Message = "Shutdown requested; leaving the consumer group.")]
    private static partial void LogStopping(ILogger logger);
}
