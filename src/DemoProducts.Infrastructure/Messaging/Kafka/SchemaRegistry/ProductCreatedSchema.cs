using System.Reflection;
using System.Text.Json;

namespace DemoProducts.Infrastructure.Messaging.Kafka.SchemaRegistry;

/// <summary>
/// The Avro schema the producing side registers, read from the same <c>.avsc</c> the code generator
/// consumes.
/// </summary>
/// <remarks>
/// Embedded rather than pasted into a string constant so there is exactly one schema in the repository:
/// a constant would be a second copy that no build step compares against the first, and the two would
/// drift on the first field added. The file is shipped as an EmbeddedResource, which also survives the
/// single-file native binary, where there is no directory to read it from.
/// </remarks>
internal static class ProductCreatedSchema
{
    private const string ResourceName = "DemoProducts.Infrastructure.Avro.product-created.avsc";

    private static readonly Lazy<string> Minified = new(Read, LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>The schema as one line, which is the form the registry stores and compares.</summary>
    public static string Json => Minified.Value;

    /// <summary>Confluent's TopicNameStrategy, the default both sides of this sample assume.</summary>
    public static string SubjectFor(string topic) => $"{topic}-value";

    private static string Read()
    {
        using var stream = typeof(ProductCreatedSchema).Assembly.GetManifestResourceStream(ResourceName)
            ?? throw new InvalidOperationException(
                $"The embedded schema '{ResourceName}' is missing. It is declared as an EmbeddedResource in " +
                "DemoProducts.Infrastructure.csproj; a renamed file or a changed LogicalName breaks this.");

        using var reader = new StreamReader(stream);

        // Parsed and rewritten rather than trimmed with string operations: it drops the formatting
        // without touching property order, and it fails here — at boot, naming the file — if the .avsc
        // is not valid JSON, instead of at the registry with a 422.
        using var document = JsonDocument.Parse(reader.ReadToEnd());
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            document.WriteTo(writer);
        }

        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }
}
