using Confluent.Kafka;
using DemoProducts.Domain.Events;
using DemoProducts.Infrastructure.Messaging.Delivery;
using Microsoft.Extensions.Logging;

namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// The Kafka adapter behind <see cref="IProductCreatedSubscription"/>. Subscribes when constructed and
/// leaves the consumer group when disposed, so the delivery protocol never sees a topic or a group id.
/// </summary>
/// <remarks>
/// Generic over the Avro value type because <c>Kafka:Consumer:EnableAvroReader</c> chooses between the
/// generated record and <c>GenericRecord</c>, and everything below the deserializer — offsets, commits,
/// rewinds, group membership — is identical either way. The deserializer and the mapping to
/// <see cref="ProductCreatedEvent"/> are handed in rather than chosen here:
/// <see cref="ProductCreatedSubscriptionFactory"/> owns that decision, so this class has no branch on a
/// configuration flag and no reference to either Avro representation.
/// </remarks>
internal sealed class KafkaProductCreatedSubscription<TValue> : IKafkaProductCreatedSubscription
    where TValue : class
{
    private readonly IConsumer<string, TValue> _consumer;
    private readonly Func<TValue, ProductCreatedEvent> _toEvent;
    private readonly int _retryDelayMs;
    private readonly ILogger _logger;
    private bool _disposed;

    public KafkaProductCreatedSubscription(
        KafkaConsumerOptions kafka,
        IDeserializer<TValue> valueDeserializer,
        Func<TValue, ProductCreatedEvent> toEvent,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(kafka);
        ArgumentNullException.ThrowIfNull(valueDeserializer);
        ArgumentNullException.ThrowIfNull(toEvent);

        _toEvent = toEvent;
        _logger = logger;
        _retryDelayMs = kafka.Consumer.RetryDelayMs;

        _consumer = new ConsumerBuilder<string, TValue>(BuildConsumerConfig(kafka))
            .SetValueDeserializer(valueDeserializer)
            .SetErrorHandler((_, error) => KafkaSubscriptionLog.ConsumerError(logger, error.Reason))
            .Build();

        _consumer.Subscribe(kafka.Topics.ProductCreated);
        KafkaSubscriptionLog.Subscribed(
            logger, kafka.Topics.ProductCreated, kafka.Consumer.GroupId, typeof(TValue).Name);
    }

    public ReceivedProductCreated? TryRead(CancellationToken cancellationToken)
    {
        ConsumeResult<string, TValue> result;

        try
        {
            result = _consumer.Consume(cancellationToken);
        }
        catch (ConsumeException exception)
        {
            // Reported here rather than raised: a failed read is not a failed delivery, and the protocol
            // answers a null by reading again.
            KafkaSubscriptionLog.ConsumeFailed(_logger, exception);
            return null;
        }

        if (result?.Message?.Value is null)
        {
            return null;
        }

        return new ReceivedProductCreated(
            _toEvent(result.Message.Value),
            result,
            result.TopicPartitionOffset.ToString());
    }

    public void Commit(ReceivedProductCreated received)
    {
        ArgumentNullException.ThrowIfNull(received);

        // Commit(ConsumeResult) stores offset + 1, which is what "already handled" means to Kafka.
        _consumer.Commit(Position(received));
    }

    public void SeekBack(ReceivedProductCreated received)
    {
        ArgumentNullException.ThrowIfNull(received);

        _consumer.Seek(Position(received).TopicPartitionOffset);
    }

    public void PauseBeforeRetry(CancellationToken cancellationToken) =>
        cancellationToken.WaitHandle.WaitOne(_retryDelayMs);

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        // Leaves the consumer group cleanly instead of waiting for the session timeout.
        _consumer.Close();
        _consumer.Dispose();

        _disposed = true;
    }

    private static ConsumeResult<string, TValue> Position(ReceivedProductCreated received) =>
        (ConsumeResult<string, TValue>)received.Position;

    private static ConsumerConfig BuildConsumerConfig(KafkaConsumerOptions kafka) => new()
    {
        BootstrapServers = kafka.BootstrapServers,
        ClientId = kafka.ClientId,
        GroupId = kafka.Consumer.GroupId,
        AutoOffsetReset = Enum.Parse<AutoOffsetReset>(kafka.Consumer.AutoOffsetReset, ignoreCase: true),
        EnableAutoCommit = kafka.Consumer.EnableAutoCommit,
        SessionTimeoutMs = kafka.Consumer.SessionTimeoutMs,
        MaxPollIntervalMs = kafka.Consumer.MaxPollIntervalMs,
        MaxPollRecords = kafka.Consumer.MaxPollRecords,
        FetchMinBytes = kafka.Consumer.FetchMinBytes,
        FetchWaitMaxMs = kafka.Consumer.FetchWaitMaxMs,

        // Not configurable on purpose: committing only after the handler succeeds requires the offset
        // store to stay manual. Exposing this key would let configuration silently break the contract.
        EnableAutoOffsetStore = false,
    };
}
