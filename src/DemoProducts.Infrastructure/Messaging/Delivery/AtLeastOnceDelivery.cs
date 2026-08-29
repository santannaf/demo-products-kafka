using DemoProducts.Domain.Events;
using Microsoft.Extensions.Logging;

namespace DemoProducts.Infrastructure.Messaging.Delivery;

/// <summary>
/// The delivery protocol: read, hand to the application, and commit only if that succeeded — otherwise
/// rewind and pause, up to <paramref name="maxAttemptsPerRecord"/> attempts. Knows nothing about Kafka,
/// Avro, topics or consumer groups.
/// </summary>
/// <remarks>
/// <para>
/// This is the whole reason <see cref="IProductCreatedSubscription"/> exists. Dropping the
/// <see cref="IProductCreatedSubscription.SeekBack"/> call below breaks nothing loudly — messages are
/// simply skipped for the rest of the session — so the invariant is defended by tests against a fake
/// subscription rather than by a broker.
/// </para>
/// <para>
/// <b>At-least-once holds only up to the attempt cap.</b> A record that fails
/// <paramref name="maxAttemptsPerRecord"/> times is committed past and lost, because the alternative —
/// retrying forever — stops the partition for every message behind it. Neither is safe; this is the one
/// that keeps the consumer moving. A dead-letter topic is what would make it both, and there is none
/// here, so the error log line is the record's only remaining trace.
/// </para>
/// </remarks>
/// <param name="maxAttemptsPerRecord">
/// From <c>Kafka:Consumer:MaxAttemptsPerRecord</c>, passed as a number so this class keeps no Kafka
/// types.
/// </param>
internal sealed partial class AtLeastOnceDelivery(ILogger<AtLeastOnceDelivery> logger, int maxAttemptsPerRecord)
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

        // Attempts are counted against the position rather than kept as a plain counter: a rebalance can
        // hand the loop a different record between two failures, and resetting on that is what stops one
        // poison record from spending another record's budget.
        var attemptedPosition = string.Empty;
        var attempts = 0;

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var received = subscription.TryRead(cancellationToken);

                if (received is null)
                {
                    continue;
                }

                if (!string.Equals(received.PositionDescription, attemptedPosition, StringComparison.Ordinal))
                {
                    attemptedPosition = received.PositionDescription;
                    attempts = 0;
                }

                if (TryHandle(received, handle, cancellationToken))
                {
                    subscription.Commit(received);
                    attemptedPosition = string.Empty;
                    continue;
                }

                attempts++;

                if (attempts >= maxAttemptsPerRecord)
                {
                    // Commit rather than rewind: this is the give-up branch, and it drops the record.
                    LogGivingUp(logger, received.PositionDescription, attempts);
                    subscription.Commit(received);
                    attemptedPosition = string.Empty;
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

    [LoggerMessage(EventId = 3006, Level = LogLevel.Error, Message = "Giving up on offset {TopicPartitionOffset} after {Attempts} attempts; the record is committed past and dropped.")]
    private static partial void LogGivingUp(ILogger logger, string topicPartitionOffset, int attempts);

    [LoggerMessage(EventId = 3005, Level = LogLevel.Information, Message = "Shutdown requested; leaving the consumer group.")]
    private static partial void LogStopping(ILogger logger);
}
