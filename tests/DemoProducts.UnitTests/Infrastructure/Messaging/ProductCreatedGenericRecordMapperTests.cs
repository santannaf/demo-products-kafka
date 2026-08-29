using Avro.Generic;
using Avro.IO;
using Avro.Specific;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Generated;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Mappers;
using FluentAssertions;
using Xunit;
using RecordSchema = Avro.RecordSchema;

namespace DemoProducts.UnitTests.Infrastructure.Messaging;

/// <summary>
/// The untyped read path, reachable when <c>Kafka:Consumer:EnableAvroReader</c> is false.
/// </summary>
/// <remarks>
/// The first test goes through Avro's own writer and reader rather than hand-building a
/// <see cref="GenericRecord"/>: the question it answers — what CLR type a <c>timestamp-millis</c> field
/// arrives as — is the library's to answer, and a hand-built record would only echo this test's own
/// assumption back at it.
/// </remarks>
public sealed class ProductCreatedGenericRecordMapperTests
{
    private static readonly RecordSchema Schema = (RecordSchema)ProductCreatedAvro._SCHEMA;

    [Fact]
    public void A_record_read_back_by_Avro_maps_to_the_same_event_the_producer_sent()
    {
        var sent = new ProductCreatedAvro
        {
            EventId = Guid.NewGuid().ToString(),
            ProductId = Guid.NewGuid().ToString(),
            Name = "Café torrado",

            // Whole milliseconds: timestamp-millis has no finer resolution, so anything below is lost on
            // the wire and the round-trip could not be asserted for equality.
            OccurredAtUtc = new DateTime(2026, 8, 29, 11, 2, 11, DateTimeKind.Utc),
        };

        var mapped = ProductCreatedGenericRecordMapper.ToEvent(RoundTrip(sent));

        mapped.EventId.Should().Be(Guid.Parse(sent.EventId));
        mapped.ProductId.Should().Be(Guid.Parse(sent.ProductId));
        mapped.Name.Should().Be(sent.Name);
        mapped.OccurredAtUtc.Should().Be(sent.OccurredAtUtc);
        mapped.OccurredAtUtc.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Fact]
    public void Avro_hands_a_timestamp_millis_field_over_as_a_DateTime()
    {
        var record = RoundTrip(new ProductCreatedAvro
        {
            EventId = Guid.NewGuid().ToString(),
            ProductId = Guid.NewGuid().ToString(),
            Name = "Café torrado",
            OccurredAtUtc = new DateTime(2026, 8, 29, 11, 2, 11, DateTimeKind.Utc),
        });

        // Pins the branch the mapper actually takes. If a future Avro version hands the raw long over
        // instead, this test fails and points at the branch that then becomes the live one.
        record["OccurredAtUtc"].Should().BeOfType<DateTime>();
    }

    [Fact]
    public void A_timestamp_that_arrives_as_raw_milliseconds_is_still_read_as_UTC()
    {
        // The producer that writes this is not the one in this repository: a topic shared with a producer
        // whose schema declares a plain long carries no logical type for Avro to convert, and the mapper
        // is the only thing standing between that and a wrong instant.
        var record = new GenericRecord(Schema);
        record.Add("EventId", Guid.Empty.ToString());
        record.Add("ProductId", Guid.Empty.ToString());
        record.Add("Name", "Café torrado");
        record.Add("OccurredAtUtc", 1_788_001_331_000L);

        var mapped = ProductCreatedGenericRecordMapper.ToEvent(record);

        mapped.OccurredAtUtc.Should().Be(new DateTime(2026, 8, 29, 11, 2, 11, DateTimeKind.Utc));
        mapped.OccurredAtUtc.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Fact]
    public void A_field_of_the_wrong_type_names_the_field_instead_of_failing_on_a_cast()
    {
        var record = new GenericRecord(Schema);
        record.Add("EventId", Guid.Empty.ToString());
        record.Add("ProductId", Guid.Empty.ToString());
        record.Add("Name", "Café torrado");
        record.Add("OccurredAtUtc", "not a timestamp");

        var act = () => ProductCreatedGenericRecordMapper.ToEvent(record);

        act.Should().Throw<InvalidOperationException>().WithMessage("*OccurredAtUtc*String*");
    }

    private static GenericRecord RoundTrip(ProductCreatedAvro productCreatedAvro)
    {
        using var stream = new MemoryStream();

        new SpecificDatumWriter<ProductCreatedAvro>(Schema).Write(productCreatedAvro, new BinaryEncoder(stream));
        stream.Position = 0;

        return new GenericDatumReader<GenericRecord>(Schema, Schema).Read(default!, new BinaryDecoder(stream));
    }
}
