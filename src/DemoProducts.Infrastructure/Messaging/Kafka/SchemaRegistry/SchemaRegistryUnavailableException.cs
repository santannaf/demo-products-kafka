namespace DemoProducts.Infrastructure.Messaging.Kafka.SchemaRegistry;

/// <summary>
/// The Schema Registry could not be reached, or refused the schema. Distinct from a Kafka failure on
/// purpose: the broker is fine and the message never reached the wire, so the two are diagnosed
/// differently even though the caller sees one failed publish either way.
/// </summary>
internal sealed class SchemaRegistryUnavailableException : Exception
{
    public SchemaRegistryUnavailableException(string message)
        : base(message)
    {
    }

    public SchemaRegistryUnavailableException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
