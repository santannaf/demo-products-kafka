using Confluent.SchemaRegistry;
using DemoProducts.Application.Events.ProductCreated;
using DemoProducts.Infrastructure.Messaging.Delivery;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// Hosts the delivery protocol: opens a Kafka subscription, hands it to
/// <see cref="AtLeastOnceDelivery"/>, and closes it on shutdown. Everything this class knows about is a
/// lifetime — the offset rules live in the protocol.
/// </summary>
internal sealed class ProductCreatedListener(
    IOptions<KafkaConsumerOptions> options,
    ISchemaRegistryClient schemaRegistryClient,
    IServiceScopeFactory serviceScopeFactory,
    AtLeastOnceDelivery delivery,
    ILogger<ProductCreatedListener> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken) =>
        // IConsumer.Consume blocks the calling thread; running the loop inline would stall host startup.
        Task.Run(() => Consume(stoppingToken), stoppingToken);

    private void Consume(CancellationToken stoppingToken)
    {
        using var subscription = new KafkaProductCreatedSubscription(
            options.Value,
            schemaRegistryClient,
            logger);

        delivery.Run(subscription, Handle, stoppingToken);
    }

    private void Handle(Domain.Events.ProductCreatedEvent productCreatedEvent, CancellationToken cancellationToken)
    {
        // The handler is scoped and this listener is a singleton, so each message gets its own scope.
        using var scope = serviceScopeFactory.CreateScope();
        var handler = scope.ServiceProvider.GetRequiredService<ProductCreatedEventHandler>();

        // The protocol is synchronous because librdkafka's consume is; bridging here keeps the
        // sync-over-async in the adapter rather than in the protocol.
        handler.HandleAsync(productCreatedEvent, cancellationToken).GetAwaiter().GetResult();
    }
}
