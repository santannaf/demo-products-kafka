using DemoProducts.Domain.Events;

namespace DemoProducts.IntegrationTests.TestSupport;

internal static class TestEvents
{
    /// <summary>
    /// A fixed instant with whole milliseconds in it.
    /// </summary>
    /// <remarks>
    /// The Avro field is <c>timestamp-millis</c>, so anything finer is truncated on the wire and an
    /// equality assertion on the round trip would fail for a reason that has nothing to do with Kafka.
    /// </remarks>
    public static readonly DateTime OccurredAtUtc = new(2026, 8, 30, 14, 7, 3, 456, DateTimeKind.Utc);

    /// <summary>
    /// The default name carries an accent and a character outside the BMP on purpose: it is what proves
    /// the hand-written UTF-8 length prefix in <c>AvroBinaryWriter</c> agrees with what Avro's reader
    /// expects, which a plain ASCII name would pass without testing.
    /// </summary>
    public static ProductCreatedEvent ProductCreated(string name = "Café torrado 日本 🚀") =>
        new(Guid.NewGuid(), Guid.NewGuid(), name, OccurredAtUtc);
}
