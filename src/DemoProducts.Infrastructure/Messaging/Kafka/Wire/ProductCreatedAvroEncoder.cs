using System.Buffers;
using DemoProducts.Domain.Events;

namespace DemoProducts.Infrastructure.Messaging.Kafka.Wire;

/// <summary>
/// Encodes a <see cref="ProductCreatedEvent"/> as the Avro record declared in
/// <c>Avro/Schemas/product-created.avsc</c>, framed with the schema id.
/// </summary>
/// <remarks>
/// The field ORDER below is the contract, not the field names: Avro binary carries no names, so a reader
/// resolves each value purely by position against the writer schema it fetched by id. Reordering the
/// fields here without reordering them in the .avsc silently swaps two values of the same type, which is
/// why <c>ProductCreatedAvroEncoderTests</c> reads the bytes back with the real Avro library rather than
/// comparing against a second hand-written expectation.
/// </remarks>
internal static class ProductCreatedAvroEncoder
{
    public static byte[] Encode(ProductCreatedEvent productCreatedEvent, int schemaId)
    {
        ArgumentNullException.ThrowIfNull(productCreatedEvent);

        var body = new ArrayBufferWriter<byte>(128);

        AvroBinaryWriter.WriteString(body, productCreatedEvent.EventId.ToString());
        AvroBinaryWriter.WriteString(body, productCreatedEvent.ProductId.ToString());
        AvroBinaryWriter.WriteString(body, productCreatedEvent.Name);

        // timestamp-millis is a long of milliseconds since the Unix epoch, UTC. The kind is pinned rather
        // than trusted for the same reason the typed mapper pins it: a DateTime whose Kind is Unspecified
        // would be read as local time and shift the instant on the wire.
        AvroBinaryWriter.WriteLong(
            body,
            new DateTimeOffset(DateTime.SpecifyKind(productCreatedEvent.OccurredAtUtc, DateTimeKind.Utc))
                .ToUnixTimeMilliseconds());

        return AvroBinaryWriter.Frame(schemaId, body.WrittenSpan);
    }
}
