using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace DemoProducts.Infrastructure.Messaging.Kafka.SchemaRegistry;

/// <summary>
/// The two Schema Registry calls the producing side makes: register a schema, or look up the id of one
/// already registered. Nothing else — this is not a general client.
/// </summary>
/// <remarks>
/// Written here rather than taken from <c>Confluent.SchemaRegistry</c> because that client serialises
/// through Newtonsoft.Json, which a trimmed binary cannot do safely. The consuming side still uses
/// Confluent's client: it runs on the CLR, where reflection is intact, and it needs the schema-by-id
/// lookups and caching that this class deliberately does not implement.
/// </remarks>
internal sealed class SchemaRegistryRestClient(HttpClient httpClient)
{
    /// <summary>The registry rejects a request that does not ask for its own media type.</summary>
    private static readonly MediaTypeHeaderValue ContentType = new("application/vnd.schemaregistry.v1+json");

    /// <summary>
    /// Registers <paramref name="schemaJson"/> under <paramref name="subject"/> and returns its id.
    /// Registering a schema that is already there returns the existing id rather than creating a version.
    /// </summary>
    public Task<int> RegisterAsync(string subject, string schemaJson, CancellationToken cancellationToken) =>
        PostAsync($"subjects/{Uri.EscapeDataString(subject)}/versions", subject, schemaJson, cancellationToken);

    /// <summary>
    /// Returns the id of an already-registered identical schema, and fails if there is none. This is what
    /// <c>Kafka:SchemaRegistry:AutoRegisterSchemas = false</c> buys: an environment where schemas are
    /// published by a pipeline refuses to invent one at runtime.
    /// </summary>
    public Task<int> LookUpAsync(string subject, string schemaJson, CancellationToken cancellationToken) =>
        PostAsync($"subjects/{Uri.EscapeDataString(subject)}", subject, schemaJson, cancellationToken);

    private async Task<int> PostAsync(
        string path,
        string subject,
        string schemaJson,
        CancellationToken cancellationToken)
    {
        var payload = JsonSerializer.Serialize(
            new SchemaRequest { Schema = schemaJson },
            SchemaRegistryJsonContext.Default.SchemaRequest);

        using var content = new StringContent(payload, Encoding.UTF8);
        content.Headers.ContentType = ContentType;

        using var response = await httpClient
            .PostAsync(path, content, cancellationToken)
            .ConfigureAwait(false);

        var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
        {
            throw new SchemaRegistryUnavailableException(Describe(subject, response.StatusCode, body));
        }

        var parsed = JsonSerializer.Deserialize(body, SchemaRegistryJsonContext.Default.SchemaIdResponse);

        return parsed is { Id: > 0 }
            ? parsed.Id
            : throw new SchemaRegistryUnavailableException(
                $"The Schema Registry answered {(int)response.StatusCode} for subject '{subject}' with no usable schema id: {body}");
    }

    /// <summary>
    /// Turns the registry's error body into a sentence, falling back to the raw body when it is not the
    /// shape the registry documents — a reverse proxy answering instead of the registry, most often.
    /// </summary>
    private static string Describe(string subject, System.Net.HttpStatusCode statusCode, string body)
    {
        SchemaRegistryError? error = null;

        try
        {
            error = JsonSerializer.Deserialize(body, SchemaRegistryJsonContext.Default.SchemaRegistryError);
        }
        catch (JsonException)
        {
            // Falls through to the raw body below, which is more useful than a parse failure here.
        }

        return error is { ErrorCode: > 0 }
            ? $"The Schema Registry refused subject '{subject}': {error.Message} (error code {error.ErrorCode})."
            : $"The Schema Registry answered {(int)statusCode} for subject '{subject}': {body}";
    }
}
