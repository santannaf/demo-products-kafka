using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DemoProducts.Infrastructure.Messaging.Delivery;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Generated;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Mappers;
using Microsoft.Extensions.Logging;

namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// The Kafka adapter behind <see cref="IProductCreatedSubscription"/>. Subscribes when constructed and
/// leaves the consumer group when disposed, so the delivery protocol never sees a topic or a group id.
/// </summary>
internal sealed partial class KafkaProductCreatedSubscription : IProductCreatedSubscription, IDisposable
{
    private readonly IConsumer<string, ProductCreatedAvro> _consumer;
    private readonly int _retryDelayMs;
    private readonly ILogger _logger;
    private bool _disposed;

    public KafkaProductCreatedSubscription(
        KafkaConsumerOptions kafka,
        ISchemaRegistryClient schemaRegistryClient,
        ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(kafka);
        ArgumentNullException.ThrowIfNull(schemaRegistryClient);

        this._logger = logger;
        _retryDelayMs = kafka.Consumer.RetryDelayMs;

        _consumer = new ConsumerBuilder<string, ProductCreatedAvro>(BuildConsumerConfig(kafka))
            .SetValueDeserializer(
                new AvroDeserializer<ProductCreatedAvro>(schemaRegistryClient, new AvroDeserializerConfig())
                    .AsSyncOverAsync())
            .SetErrorHandler((_, error) => LogConsumerError(logger, error.Reason))
            .Build();

        _consumer.Subscribe(kafka.Topics.ProductCreated);
        LogSubscribed(logger, kafka.Topics.ProductCreated, kafka.Consumer.GroupId);
    }

    public ReceivedProductCreated? TryRead(CancellationToken cancellationToken)
    {
        ConsumeResult<string, ProductCreatedAvro> result;

        try
        {
            result = _consumer.Consume(cancellationToken);
        }
        catch (ConsumeException exception)
        {
            // Reported here rather than raised: a failed read is not a failed delivery, and the protocol
            // answers a null by reading again.
            LogConsumeFailed(_logger, exception);
            return null;
        }

        if (result?.Message?.Value is null)
        {
            return null;
        }

        return new ReceivedProductCreated(
            ProductCreatedAvroMapper.ToEvent(result.Message.Value),
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

    private static ConsumeResult<string, ProductCreatedAvro> Position(ReceivedProductCreated received) =>
        (ConsumeResult<string, ProductCreatedAvro>)received.Position;

    private static ConsumerConfig BuildConsumerConfig(KafkaConsumerOptions kafka) => new()
    {
        BootstrapServers = kafka.BootstrapServers,
        ClientId = kafka.ClientId,
        GroupId = kafka.Consumer.GroupId,
        AutoOffsetReset = Enum.Parse<AutoOffsetReset>(kafka.Consumer.AutoOffsetReset, ignoreCase: true),
        EnableAutoCommit = kafka.Consumer.EnableAutoCommit,
        SessionTimeoutMs = kafka.Consumer.SessionTimeoutMs,
        MaxPollIntervalMs = kafka.Consumer.MaxPollIntervalMs,

        // Not configurable on purpose: committing only after the handler succeeds requires the offset
        // store to stay manual. Exposing this key would let configuration silently break the contract.
        EnableAutoOffsetStore = false,
    };

    [LoggerMessage(EventId = 3001, Level = LogLevel.Information, Message = "Subscribed to topic {Topic} as group {GroupId}.")]
    private static partial void LogSubscribed(ILogger logger, string topic, string groupId);

    [LoggerMessage(EventId = 3002, Level = LogLevel.Error, Message = "Failed to consume a ProductCreated message.")]
    private static partial void LogConsumeFailed(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 3004, Level = LogLevel.Warning, Message = "Kafka consumer error: {Reason}")]
    private static partial void LogConsumerError(ILogger logger, string reason);
}
