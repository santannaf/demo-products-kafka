using System.Buffers.Binary;
using Avro.Generic;
using Avro.IO;
using DemoProducts.Domain.Events;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Generated;
using DemoProducts.Infrastructure.Messaging.Kafka.Wire;
using FluentAssertions;
using Xunit;
using RecordSchema = Avro.RecordSchema;

namespace DemoProducts.UnitTests.Infrastructure.Messaging;

/// <summary>
/// The hand-written Avro encoder that replaced Confluent's Avro serde on the producing side.
/// </summary>
/// <remarks>
/// Every assertion below reads the bytes back with the real <c>Apache.Avro</c> reader rather than
/// comparing them to a second hand-written expectation. That distinction is the whole value of this
/// file: a byte-for-byte expectation written by the same person who wrote the encoder proves the two
/// agree with each other, not that either agrees with Avro. The reader here is the same one the Consumer
/// uses, so a green test means the Consumer can read what the Api writes.
/// </remarks>
public sealed class ProductCreatedAvroEncoderTests
{
    private static readonly RecordSchema Schema = (RecordSchema)ProductCreatedAvro._SCHEMA;

    private static ProductCreatedEvent AnEvent(string name = "Café torrado") =>
        new(
            Guid.NewGuid(),
            Guid.NewGuid(),
            name,
            new DateTime(2026, 8, 29, 11, 2, 11, DateTimeKind.Utc));

    [Fact]
    public void Avro_reads_back_every_field_the_encoder_wrote()
    {
        var sent = AnEvent();

        var record = ReadBack(ProductCreatedAvroEncoder.Encode(sent, schemaId: 7));

        record["EventId"].Should().Be(sent.EventId.ToString());
        record["ProductId"].Should().Be(sent.ProductId.ToString());
        record["Name"].Should().Be(sent.Name);
        record["OccurredAtUtc"].Should().Be(sent.OccurredAtUtc);
    }

    [Fact]
    public void The_frame_is_Confluents_magic_byte_and_a_big_endian_schema_id()
    {
        var encoded = ProductCreatedAvroEncoder.Encode(AnEvent(), schemaId: 66_051);

        encoded[0].Should().Be(0x00, "Confluent's wire format starts with a zero byte");
        BinaryPrimitives.ReadInt32BigEndian(encoded.AsSpan(1, 4)).Should().Be(66_051);

        // 66051 is 0x00010203: a value whose bytes differ, so a little-endian write cannot pass.
        encoded.AsSpan(1, 4).ToArray().Should().Equal([0x00, 0x01, 0x02, 0x03]);
    }

    [Theory]
    [InlineData("a")]
    [InlineData("Café torrado")]                       // multi-byte UTF-8: length is bytes, not chars
    [InlineData("Ω≈ç√∫˜µ≤≥÷")]                          // every character multi-byte
    [InlineData("café ☕ 你好 🇧🇷")]                       // astral plane, so surrogate pairs
    public void A_name_survives_the_round_trip_whatever_its_encoding_costs(string name)
    {
        var sent = AnEvent(name);

        ReadBack(ProductCreatedAvroEncoder.Encode(sent, schemaId: 1))["Name"].Should().Be(name);
    }

    [Fact]
    public void A_timestamp_before_the_epoch_survives_the_zig_zag_encoding()
    {
        // The negative case the zig-zag exists for. An unsigned varint would write ten bytes here and
        // Avro would read a different instant back.
        var sent = new ProductCreatedEvent(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "Café torrado",
            new DateTime(1969, 7, 20, 20, 17, 40, DateTimeKind.Utc));

        ReadBack(ProductCreatedAvroEncoder.Encode(sent, schemaId: 1))["OccurredAtUtc"]
            .Should().Be(sent.OccurredAtUtc);
    }

    [Fact]
    public void A_DateTime_with_an_unspecified_kind_is_written_as_the_same_instant_in_UTC()
    {
        // Reading it as local time would shift the instant on the wire by the machine's offset, and the
        // test would pass anywhere with TZ=UTC. Constructed Unspecified on purpose.
        var occurredAt = new DateTime(2026, 8, 29, 11, 2, 11, DateTimeKind.Unspecified);
        var sent = new ProductCreatedEvent(Guid.NewGuid(), Guid.NewGuid(), "Café torrado", occurredAt);

        ReadBack(ProductCreatedAvroEncoder.Encode(sent, schemaId: 1))["OccurredAtUtc"]
            .Should().Be(DateTime.SpecifyKind(occurredAt, DateTimeKind.Utc));
    }

    /// <summary>Strips the five-byte frame and decodes the body with Avro's own reader.</summary>
    private static GenericRecord ReadBack(byte[] encoded)
    {
        using var stream = new MemoryStream(encoded, 5, encoded.Length - 5);

        return new GenericDatumReader<GenericRecord>(Schema, Schema).Read(default!, new BinaryDecoder(stream));
    }
}
