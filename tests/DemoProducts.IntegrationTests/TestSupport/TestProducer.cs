using DemoProducts.Infrastructure.Messaging.Kafka;
using DemoProducts.Infrastructure.Messaging.Kafka.SchemaRegistry;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace DemoProducts.IntegrationTests.TestSupport;

/// <summary>
/// The publishing side wired exactly as <c>DependencyInjection.AddKafkaProducer</c> wires it, but
/// without a container: the REST client over its own <see cref="HttpClient"/>, the lazily-resolved schema
/// id, and the producer adapter on top.
/// </summary>
/// <remarks>
/// The trailing slash on the base address is copied from the registration and is not cosmetic — without
/// it <see cref="Uri"/> replaces the last path segment instead of appending to it.
/// </remarks>
internal sealed class TestProducer : IDisposable
{
    private readonly HttpClient _httpClient;

    private TestProducer(HttpClient httpClient, KafkaProductCreatedProducer producer, ILogger logger)
    {
        _httpClient = httpClient;
        Producer = producer;
        Logger = logger;
    }

    public KafkaProductCreatedProducer Producer { get; }

    /// <summary>The logger the producer writes to, so a test can read event 2002 back.</summary>
    public ILogger Logger { get; }

    public static TestProducer Create(KafkaProducerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var httpClient = new HttpClient
        {
            BaseAddress = new Uri(options.SchemaRegistry.Url.TrimEnd('/') + '/'),
        };

        var wrapped = Options.Create(options);
        var schemaId = new ProductCreatedSchemaId(
            new SchemaRegistryRestClient(httpClient),
            wrapped,
            NullLogger<ProductCreatedSchemaId>.Instance);

        var logger = new RecordingLogger<KafkaProductCreatedProducer>();

        return new TestProducer(httpClient, new KafkaProductCreatedProducer(wrapped, schemaId, logger), logger);
    }

    public void Dispose()
    {
        Producer.Dispose();
        _httpClient.Dispose();
    }
}
