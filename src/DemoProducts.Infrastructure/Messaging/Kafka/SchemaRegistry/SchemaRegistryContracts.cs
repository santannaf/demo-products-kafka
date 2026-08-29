using System.Text.Json.Serialization;

namespace DemoProducts.Infrastructure.Messaging.Kafka.SchemaRegistry;

/// <summary>The body of a register or lookup call.</summary>
internal sealed class SchemaRequest
{
    /// <summary>The Avro schema as a JSON string — a string field whose content is itself JSON.</summary>
    public string Schema { get; set; } = string.Empty;

    public string SchemaType { get; set; } = "AVRO";
}

/// <summary>
/// What both calls answer with. Only the id is read: the registry echoes the subject, version and schema
/// back, and this client has all three already.
/// </summary>
internal sealed class SchemaIdResponse
{
    public int Id { get; set; }
}

/// <summary>
/// The registry's error body. Worth parsing rather than reporting a bare status code: <c>40403</c>
/// (schema not found) and <c>42201</c> (invalid schema) are the two a misconfiguration produces, and they
/// share the same 404/422 as several others.
/// </summary>
internal sealed class SchemaRegistryError
{
    [JsonPropertyName("error_code")]
    public int ErrorCode { get; set; }

    public string? Message { get; set; }
}

/// <summary>
/// The reflection-free JSON contract for the calls above.
/// </summary>
/// <remarks>
/// This context is the entire reason the Api no longer carries Newtonsoft.Json:
/// <c>Confluent.SchemaRegistry</c>'s own client serialises these same three shapes reflectively, which is
/// what produced 34 of the 43 ILC warnings — and, before the trimmer roots, an empty request body on the
/// native binary. Generated code cannot have that failure mode.
/// </remarks>
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(SchemaRequest))]
[JsonSerializable(typeof(SchemaIdResponse))]
[JsonSerializable(typeof(SchemaRegistryError))]
internal sealed partial class SchemaRegistryJsonContext : JsonSerializerContext;
