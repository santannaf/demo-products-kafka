using Confluent.SchemaRegistry;
using DemoProducts.Infrastructure.Messaging.Kafka;

namespace DemoProducts.IntegrationTests.TestSupport;

/// <summary>
/// The consuming side wired as <c>DependencyInjection.AddKafkaConsumer</c> wires it: the cached registry
/// client, and the subscription the factory closes over the <c>EnableAvroReader</c> flag.
/// </summary>
/// <remarks>
/// Going through <see cref="ProductCreatedSubscriptionFactory"/> rather than constructing a closed
/// subscription directly is deliberate — the factory is the only place that reads the flag, so a test
/// that skipped it would never exercise the branch it means to.
/// </remarks>
internal sealed class TestSubscription : IDisposable
{
    private readonly CachedSchemaRegistryClient _schemaRegistryClient;

    private TestSubscription(
        CachedSchemaRegistryClient schemaRegistryClient,
        IKafkaProductCreatedSubscription subscription,
        RecordingLogger logger)
    {
        _schemaRegistryClient = schemaRegistryClient;
        Subscription = subscription;
        Logger = logger;
    }

    public IKafkaProductCreatedSubscription Subscription { get; }

    public RecordingLogger Logger { get; }

    public static TestSubscription Create(KafkaConsumerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var schemaRegistryClient = new CachedSchemaRegistryClient(new SchemaRegistryConfig
        {
            Url = options.SchemaRegistry.Url,
        });

        var logger = new RecordingLogger();

        return new TestSubscription(
            schemaRegistryClient,
            ProductCreatedSubscriptionFactory.Create(options, schemaRegistryClient, logger),
            logger);
    }

    public void Dispose()
    {
        // The subscription first: closing the consumer group is what it does on dispose, and it needs
        // the deserializer's registry client to still be alive while it drains.
        Subscription.Dispose();
        _schemaRegistryClient.Dispose();
    }
}
