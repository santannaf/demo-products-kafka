using Microsoft.Extensions.Logging;

namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// The subscription's log events, kept in a non-generic class on purpose: the LoggerMessage generator
/// emits one method per containing type, so leaving these inside the generic subscription would produce
/// a separate copy — and a separate set of the same EventIds — per closed type.
/// </summary>
internal static partial class KafkaSubscriptionLog
{
    [LoggerMessage(EventId = 3001, Level = LogLevel.Information, Message = "Subscribed to topic {Topic} as group {GroupId} reading {ValueType}.")]
    public static partial void Subscribed(ILogger logger, string topic, string groupId, string valueType);

    [LoggerMessage(EventId = 3002, Level = LogLevel.Error, Message = "Failed to consume a ProductCreated message.")]
    public static partial void ConsumeFailed(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 3004, Level = LogLevel.Warning, Message = "Kafka consumer error: {Reason}")]
    public static partial void ConsumerError(ILogger logger, string reason);
}
