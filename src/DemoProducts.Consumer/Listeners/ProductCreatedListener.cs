using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DemoProducts.Application.Events.ProductCreated;
using DemoProducts.Infrastructure.Messaging.Kafka;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Generated;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Mappers;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DemoProducts.Consumer.Listeners;

internal sealed partial class ProductCreatedListener(
    IOptions<KafkaConsumerOptions> options,
    ISchemaRegistryClient schemaRegistryClient,
    IServiceScopeFactory serviceScopeFactory,
    ILogger<ProductCreatedListener> logger) : BackgroundService
{
    private readonly KafkaConsumerOptions kafka = options.Value;

    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        // IConsumer.Consume blocks the calling thread; running the loop inline would stall host startup.
        Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);

    private void ConsumeLoop(CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<string, ProductCreatedAvro>(BuildConsumerConfig())
            .SetValueDeserializer(
                new AvroDeserializer<ProductCreatedAvro>(schemaRegistryClient, new AvroDeserializerConfig())
                    .AsSyncOverAsync())
            .SetErrorHandler((_, error) => LogConsumerError(logger, error.Reason))
            .Build();

        consumer.Subscribe(kafka.Topics.ProductCreated);
        LogSubscribed(logger, kafka.Topics.ProductCreated, kafka.Consumer.GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, ProductCreatedAvro> result;

                try
                {
                    result = consumer.Consume(stoppingToken);
                }
                catch (ConsumeException exception)
                {
                    LogConsumeFailed(logger, exception);
                    continue;
                }

                if (result?.Message?.Value is null)
                {
                    continue;
                }

                if (TryHandle(result, stoppingToken))
                {
                    consumer.Commit(result);
                    continue;
                }

                // Not committing is not enough: the consumer's in-memory position has already advanced,
                // so without this Seek the failed message would be skipped for the rest of the session
                // and only reappear on restart or rebalance.
                consumer.Seek(result.TopicPartitionOffset);
                PauseBeforeRetry(stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            LogStopping(logger);
        }
        finally
        {
            // Leaves the consumer group cleanly instead of waiting for the session timeout.
            consumer.Close();
        }
    }

    private ConsumerConfig BuildConsumerConfig() => new()
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

    private bool TryHandle(ConsumeResult<string, ProductCreatedAvro> result, CancellationToken cancellationToken)
    {
        try
        {
            var productCreatedEvent = ProductCreatedAvroMapper.ToEvent(result.Message.Value);

            // The handler is scoped and this listener is a singleton, so each message gets its own scope.
            using var scope = serviceScopeFactory.CreateScope();
            var handler = scope.ServiceProvider.GetRequiredService<ProductCreatedEventHandler>();

            handler.HandleAsync(productCreatedEvent, cancellationToken).GetAwaiter().GetResult();

            return true;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception exception)
        {
            LogHandleFailed(logger, result.TopicPartitionOffset.ToString(), exception);
            return false;
        }
    }

    private void PauseBeforeRetry(CancellationToken stoppingToken) =>
        // Returns early on shutdown, and bounds a permanently failing message to one attempt per delay
        // instead of a hot loop.
        stoppingToken.WaitHandle.WaitOne(kafka.Consumer.RetryDelayMs);

    [LoggerMessage(EventId = 3001, Level = LogLevel.Information, Message = "Subscribed to topic {Topic} as group {GroupId}.")]
    private static partial void LogSubscribed(ILogger logger, string topic, string groupId);

    [LoggerMessage(EventId = 3002, Level = LogLevel.Error, Message = "Failed to consume a ProductCreated message.")]
    private static partial void LogConsumeFailed(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 3003, Level = LogLevel.Error, Message = "Handler failed for offset {TopicPartitionOffset}; the offset was not committed and will be re-consumed.")]
    private static partial void LogHandleFailed(ILogger logger, string topicPartitionOffset, Exception exception);

    [LoggerMessage(EventId = 3004, Level = LogLevel.Warning, Message = "Kafka consumer error: {Reason}")]
    private static partial void LogConsumerError(ILogger logger, string reason);

    [LoggerMessage(EventId = 3005, Level = LogLevel.Information, Message = "Shutdown requested; leaving the consumer group.")]
    private static partial void LogStopping(ILogger logger);
}
