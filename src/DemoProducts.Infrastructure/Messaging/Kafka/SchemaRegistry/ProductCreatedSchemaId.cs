using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DemoProducts.Infrastructure.Messaging.Kafka.SchemaRegistry;

/// <summary>
/// Resolves the schema id once per process and hands it to every encode after that.
/// </summary>
/// <remarks>
/// Resolved lazily rather than in the constructor so the host still boots when the registry is down —
/// the same behaviour Confluent's serializer had, and what keeps a registry outage a 502 per request
/// instead of a process that will not start. A failed attempt is deliberately NOT cached: caching a
/// Task would make the first outage permanent for the life of the process.
/// </remarks>
internal sealed partial class ProductCreatedSchemaId(
    SchemaRegistryRestClient client,
    IOptions<KafkaProducerOptions> options,
    ILogger<ProductCreatedSchemaId> logger)
{
    private readonly SemaphoreSlim _gate = new(1, 1);
    private int _schemaId;

    public async Task<int> ResolveAsync(CancellationToken cancellationToken)
    {
        if (_schemaId != 0)
        {
            return _schemaId;
        }

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            if (_schemaId != 0)
            {
                return _schemaId;
            }

            var kafka = options.Value;
            var subject = ProductCreatedSchema.SubjectFor(kafka.Topics.ProductCreated);

            _schemaId = kafka.SchemaRegistry.AutoRegisterSchemas
                ? await client.RegisterAsync(subject, ProductCreatedSchema.Json, cancellationToken).ConfigureAwait(false)
                : await client.LookUpAsync(subject, ProductCreatedSchema.Json, cancellationToken).ConfigureAwait(false);

            LogResolved(logger, subject, _schemaId, kafka.SchemaRegistry.AutoRegisterSchemas);

            return _schemaId;
        }
        finally
        {
            _gate.Release();
        }
    }

    [LoggerMessage(
        EventId = 2001,
        Level = LogLevel.Information,
        Message = "Avro schema resolved for subject {Subject}: id {SchemaId} (auto-registered: {AutoRegistered}).")]
    private static partial void LogResolved(ILogger logger, string subject, int schemaId, bool autoRegistered);
}
