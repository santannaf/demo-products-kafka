using System.Buffers.Binary;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using DemoProducts.Application.Abstractions.Messaging;
using DemoProducts.IntegrationTests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Xunit;

namespace DemoProducts.IntegrationTests.Messaging;

/// <summary>
/// The publishing side against a real broker and a real Schema Registry.
/// </summary>
/// <remarks>
/// <c>ProductCreatedAvroEncoderTests</c> in the unit tier already proves the frame's shape and that
/// <c>Apache.Avro</c> reads back every field, using a schema id invented by the test. What only a real
/// registry can prove is the part that id stands in for: that the number the producer frames is the one
/// the registry assigned, and that Confluent's deserializer — which fetches the writer schema BY THAT ID
/// rather than being handed it — accepts what the hand-written encoder wrote. That resolution is the
/// actual seam between the two halves of this repository, and nothing below the broker exercises it.
/// </remarks>
[Collection(KafkaCollection.Name)]
public sealed class ProductCreatedProducerTests(KafkaFixture fixture)
{
    [Fact]
    public async Task The_schema_id_on_the_wire_is_the_one_the_registry_assigned()
    {
        var topic = await fixture.CreateTopicAsync();
        using var producer = TestProducer.Create(KafkaTestOptions.Producer(fixture, topic));

        await producer.Producer.SendAsync(TestEvents.ProductCreated(), TestContext.Current.CancellationToken);

        var value = RawTopic.ReadOne(fixture, topic).Message.Value;
        var registered = await RawTopic.LatestSchemaIdAsync(fixture, $"{topic}-value");

        value[0].Should().Be(0x00, "Confluent's wire format starts with a zero byte");
        BinaryPrimitives.ReadInt32BigEndian(value.AsSpan(1, 4)).Should().Be(
            registered,
            "a consumer resolves the writer schema by this id, so an id the registry never issued is unreadable");
    }

    [Fact]
    public async Task Confluents_deserializer_reads_back_what_the_hand_written_encoder_wrote()
    {
        var topic = await fixture.CreateTopicAsync();
        using var producer = TestProducer.Create(KafkaTestOptions.Producer(fixture, topic));
        var sent = TestEvents.ProductCreated();

        await producer.Producer.SendAsync(sent, TestContext.Current.CancellationToken);

        var value = RawTopic.ReadOne(fixture, topic).Message.Value;

        using var schemaRegistryClient = new CachedSchemaRegistryClient(
            new SchemaRegistryConfig { Url = fixture.SchemaRegistryUrl });

        // The deserializer is given the bytes and nothing else: it reads the id out of the frame and
        // fetches the writer schema from the registry itself, which is exactly what the Consumer does.
        var record = await new AvroDeserializer<GenericRecord>(schemaRegistryClient)
            .DeserializeAsync(value, isNull: false, new SerializationContext(MessageComponentType.Value, topic));

        record["EventId"].Should().Be(sent.EventId.ToString());
        record["ProductId"].Should().Be(sent.ProductId.ToString());
        record["Name"].Should().Be(sent.Name);
        record["OccurredAtUtc"].Should().Be(sent.OccurredAtUtc);
    }

    [Fact]
    public async Task The_message_is_keyed_by_product_id()
    {
        var topic = await fixture.CreateTopicAsync();
        using var producer = TestProducer.Create(KafkaTestOptions.Producer(fixture, topic));
        var sent = TestEvents.ProductCreated();

        await producer.Producer.SendAsync(sent, TestContext.Current.CancellationToken);

        // The key is what makes ConsistentRandom deterministic: every event for one product lands on the
        // same partition, so their order is preserved.
        RawTopic.ReadOne(fixture, topic).Message.Key.Should().Be(sent.ProductId.ToString());
    }

    [Fact]
    public async Task The_schema_is_registered_under_Confluents_topic_name_strategy()
    {
        var topic = await fixture.CreateTopicAsync();
        using var producer = TestProducer.Create(KafkaTestOptions.Producer(fixture, topic));

        await producer.Producer.SendAsync(TestEvents.ProductCreated(), TestContext.Current.CancellationToken);

        // Spelled out rather than taken from ProductCreatedSchema.SubjectFor: the subject shape is the
        // contract with every other client of this topic, and a test that asks the code what the code
        // does would follow it into a rename.
        (await RawTopic.SubjectsAsync(fixture)).Should().Contain($"{topic}-value");
    }

    [Fact]
    public async Task Publishing_fails_when_auto_registration_is_off_and_the_schema_was_never_published()
    {
        var topic = await fixture.CreateTopicAsync();
        using var producer = TestProducer.Create(
            KafkaTestOptions.Producer(fixture, topic, autoRegisterSchemas: false));

        var publishing = async () => await producer.Producer.SendAsync(
            TestEvents.ProductCreated(), TestContext.Current.CancellationToken);

        // This is what AutoRegisterSchemas=false buys: an environment whose schemas are published by a
        // pipeline refuses to invent one at runtime, rather than quietly creating a version.
        await publishing.Should().ThrowAsync<EventPublishFailedException>();
    }

    [Fact]
    public async Task Publishing_with_auto_registration_off_reuses_the_id_of_an_already_published_schema()
    {
        var topic = await fixture.CreateTopicAsync();

        using (var registering = TestProducer.Create(KafkaTestOptions.Producer(fixture, topic)))
        {
            await registering.Producer.SendAsync(TestEvents.ProductCreated(), TestContext.Current.CancellationToken);
        }

        using var lookingUp = TestProducer.Create(
            KafkaTestOptions.Producer(fixture, topic, autoRegisterSchemas: false));

        await lookingUp.Producer.SendAsync(TestEvents.ProductCreated(), TestContext.Current.CancellationToken);

        var registered = await RawTopic.LatestSchemaIdAsync(fixture, $"{topic}-value");
        var published = RawTopic.ReadOne(fixture, topic).Message.Value;

        BinaryPrimitives.ReadInt32BigEndian(published.AsSpan(1, 4)).Should().Be(
            registered,
            "the lookup branch must resolve the same id the register branch did, or the two halves of a " +
            "deployment would frame the same schema differently");

        // Still one version: a lookup that silently registered would have created a second.
        (await RawTopic.SubjectsAsync(fixture)).Should().ContainSingle(subject => subject == $"{topic}-value");
    }

    [Fact]
    public async Task An_unreachable_registry_surfaces_as_EventPublishFailedException()
    {
        var topic = await fixture.CreateTopicAsync();

        // Port 1 is reserved and nothing listens on it, so the client fails to connect rather than
        // waiting out a timeout.
        using var producer = TestProducer.Create(
            KafkaTestOptions.Producer(fixture, topic, schemaRegistryUrl: "http://127.0.0.1:1"));

        var publishing = async () => await producer.Producer.SendAsync(
            TestEvents.ProductCreated(), TestContext.Current.CancellationToken);

        // The translation the AOT rewrite introduced: the producer catches HttpRequestException from its
        // own REST client, where the Confluent serializer used to raise its own type. Without this catch
        // a registry outage surfaces as an unhandled exception and the Api answers 500 instead of 502.
        await publishing.Should().ThrowAsync<EventPublishFailedException>()
            .WithInnerException<EventPublishFailedException, HttpRequestException>();
    }

    [Fact]
    public async Task The_publish_log_line_names_the_offset_the_broker_acknowledged()
    {
        var topic = await fixture.CreateTopicAsync();
        using var producer = TestProducer.Create(KafkaTestOptions.Producer(fixture, topic));
        var sent = TestEvents.ProductCreated();

        await producer.Producer.SendAsync(sent, TestContext.Current.CancellationToken);

        var recorded = ((RecordingLogger)producer.Logger).Entries.Single(entry => entry.EventId == 2002);
        var stored = RawTopic.ReadOne(fixture, topic);

        recorded.Level.Should().Be(LogLevel.Information);
        recorded.Property("Topic").Should().Be(topic);
        recorded.Property("Offset").Should().Be(stored.Offset.Value);
        recorded.Property("DomainEventId").Should().Be(sent.EventId);

        // Event 2002 is written after the await, so it means the broker acknowledged the message under
        // the configured Acks - not that librdkafka queued it. Asserting the offset it reports against
        // the offset the message actually landed on is what holds that promise in place.
        recorded.Message.Should().Contain($"Offset={stored.Offset.Value}");
    }
}
