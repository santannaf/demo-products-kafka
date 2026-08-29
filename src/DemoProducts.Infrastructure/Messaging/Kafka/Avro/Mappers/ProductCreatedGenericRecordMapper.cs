using DemoProducts.Domain.Events;
using GenericRecord = Avro.Generic.GenericRecord;

namespace DemoProducts.Infrastructure.Messaging.Kafka.Avro.Mappers;

/// <summary>
/// The untyped counterpart of <see cref="ProductCreatedAvroMapper"/>, used when
/// <c>Kafka:Consumer:EnableAvroReader</c> is <see langword="false"/>: same wire contract, read by field
/// name instead of through the generated class.
/// </summary>
/// <remarks>
/// The field names are string literals here, which is exactly what this option costs. With the typed
/// reader a renamed field is a compiler error; here it is a <see cref="KeyNotFoundException"/> on the
/// first message, and this class is the only place that failure can originate. That is why the schema
/// (<c>Avro/Schemas/product-created.avsc</c>) and these literals must be reviewed together.
/// </remarks>
public static class ProductCreatedGenericRecordMapper
{
    public static ProductCreatedEvent ToEvent(GenericRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);

        return new ProductCreatedEvent(
            Guid.Parse(Field<string>(record, "EventId")),
            Guid.Parse(Field<string>(record, "ProductId")),
            Field<string>(record, "Name"),
            OccurredAtUtc(record));
    }

    /// <summary>
    /// Reads <c>OccurredAtUtc</c>, whose schema is <c>long</c> with logical type <c>timestamp-millis</c>.
    /// </summary>
    /// <remarks>
    /// Both representations are accepted because which one arrives is decided by the reader schema
    /// resolved from the registry, not by this code: Avro's generic reader materialises a logical type
    /// as <see cref="DateTime"/> when it recognises it, and leaves the underlying <see cref="long"/> in
    /// place when it does not — a producer registering the field without the logical type would silently
    /// take the second branch. The typed reader cannot express that ambiguity, which is a fair summary of
    /// the whole trade.
    /// </remarks>
    private static DateTime OccurredAtUtc(GenericRecord record) => record.GetValue(IndexOf(record, "OccurredAtUtc")) switch
    {
        DateTime occurredAt => DateTime.SpecifyKind(occurredAt, DateTimeKind.Utc),
        long milliseconds => DateTimeOffset.FromUnixTimeMilliseconds(milliseconds).UtcDateTime,
        var other => throw new InvalidOperationException(
            $"ProductCreated.OccurredAtUtc arrived as {other?.GetType().Name ?? "null"}; " +
            "expected a DateTime or a long of milliseconds since the Unix epoch."),
    };

    private static T Field<T>(GenericRecord record, string name)
    {
        var value = record.GetValue(IndexOf(record, name));

        return value is T typed
            ? typed
            : throw new InvalidOperationException(
                $"ProductCreated.{name} arrived as {value?.GetType().Name ?? "null"}; expected {typeof(T).Name}.");
    }

    /// <summary>
    /// Resolves a field position once per read. <c>GenericRecord</c>'s string indexer throws
    /// <see cref="KeyNotFoundException"/> with only the field name in it; going through the schema lets
    /// the failure say which record was being read.
    /// </summary>
    private static int IndexOf(GenericRecord record, string name) =>
        record.Schema.TryGetField(name, out var field)
            ? field.Pos
            : throw new InvalidOperationException(
                $"The registered schema '{record.Schema.Fullname}' has no field '{name}'. " +
                "The consumer's field names and Avro/Schemas/product-created.avsc have drifted apart.");
}
