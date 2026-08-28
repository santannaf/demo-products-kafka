using DemoProducts.Domain.Events;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Mappers;
using FluentAssertions;
using Xunit;

namespace DemoProducts.UnitTests.Infrastructure.Messaging;

/// <summary>
/// The mapper pins <see cref="DateTimeKind.Utc"/> in both directions because Avro's timestamp-millis
/// converts through <see cref="DateTime.ToUniversalTime"/> — any other kind shifts the instant without
/// any error. These tests are what make that silent failure loud.
/// </summary>
public sealed class ProductCreatedAvroMapperTests
{
    private static readonly DateTime OccurredAtUtc = new(2026, 8, 28, 15, 30, 45, DateTimeKind.Utc);

    [Fact]
    public void RoundTrip_preserves_every_field()
    {
        var original = new ProductCreatedEvent(Guid.NewGuid(), Guid.NewGuid(), "Café torrado", OccurredAtUtc);

        var roundTripped = ProductCreatedAvroMapper.ToEvent(ProductCreatedAvroMapper.ToAvro(original));

        roundTripped.Should().BeEquivalentTo(original);
    }

    [Theory]
    [InlineData(DateTimeKind.Unspecified)]
    [InlineData(DateTimeKind.Local)]
    [InlineData(DateTimeKind.Utc)]
    public void ToAvro_always_marks_the_timestamp_as_Utc(DateTimeKind kind)
    {
        var productCreatedEvent = new ProductCreatedEvent(
            Guid.NewGuid(),
            Guid.NewGuid(),
            "Café torrado",
            DateTime.SpecifyKind(OccurredAtUtc, kind));

        var avro = ProductCreatedAvroMapper.ToAvro(productCreatedEvent);

        avro.OccurredAtUtc.Kind.Should().Be(DateTimeKind.Utc);

        // The wall-clock reading must survive untouched: SpecifyKind relabels, it must not convert.
        avro.OccurredAtUtc.Should().Be(DateTime.SpecifyKind(OccurredAtUtc, DateTimeKind.Utc));
    }

    [Fact]
    public void ToEvent_marks_the_timestamp_as_Utc()
    {
        var avro = ProductCreatedAvroMapper.ToAvro(
            new ProductCreatedEvent(Guid.NewGuid(), Guid.NewGuid(), "Café torrado", OccurredAtUtc));
        avro.OccurredAtUtc = DateTime.SpecifyKind(avro.OccurredAtUtc, DateTimeKind.Unspecified);

        var productCreatedEvent = ProductCreatedAvroMapper.ToEvent(avro);

        productCreatedEvent.OccurredAtUtc.Kind.Should().Be(DateTimeKind.Utc);
    }

    [Fact]
    public void ToAvro_rejects_a_null_event() =>
        FluentActions.Invoking(() => ProductCreatedAvroMapper.ToAvro(null!))
            .Should().Throw<ArgumentNullException>();

    [Fact]
    public void ToEvent_rejects_a_null_record() =>
        FluentActions.Invoking(() => ProductCreatedAvroMapper.ToEvent(null!))
            .Should().Throw<ArgumentNullException>();
}
