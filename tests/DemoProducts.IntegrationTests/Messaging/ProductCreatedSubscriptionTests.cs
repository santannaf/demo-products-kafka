using DemoProducts.Domain.Events;
using DemoProducts.IntegrationTests.TestSupport;
using FluentAssertions;
using Xunit;

namespace DemoProducts.IntegrationTests.Messaging;

/// <summary>
/// The consuming side against a real broker: the two Avro readers, and the offset semantics that only a
/// broker can answer for.
/// </summary>
/// <remarks>
/// The unit tier proves the delivery protocol's decisions against a fake subscription. What it cannot
/// prove is that a real Kafka consumer answers those decisions the way the protocol assumes — that a
/// commit means "already handled" to the broker, and that a rewind actually rewinds. Those are the
/// assertions here.
/// </remarks>
[Collection(KafkaCollection.Name)]
public sealed class ProductCreatedSubscriptionTests(KafkaFixture fixture)
{
    [Theory]
    [InlineData(true, "ProductCreatedAvro")]
    [InlineData(false, "GenericRecord")]
    public async Task Both_avro_readers_produce_the_same_event(bool enableAvroReader, string expectedValueType)
    {
        var topic = await fixture.CreateTopicAsync();
        var sent = TestEvents.ProductCreated();
        await PublishAsync(topic, sent);

        using var subscription = TestSubscription.Create(
            KafkaTestOptions.Consumer(fixture, topic, KafkaFixture.NewGroupId(), enableAvroReader));

        var received = Deadline.ReadOne(subscription.Subscription);

        received.Event.Should().Be(sent);

        // Asserted separately because DateTime equality ignores Kind: the record comparison above passes
        // even when the instant came back as Unspecified, and an Unspecified DateTime is reinterpreted as
        // local time by anything downstream that converts it.
        received.Event.OccurredAtUtc.Kind.Should().Be(DateTimeKind.Utc);

        // The guard on the theory. Without it both rows could take the same branch and the test would
        // still be green - which is exactly the failure mode a config flag with one call site has.
        subscription.Logger.Entries.Single(entry => entry.EventId == 3001)
            .Property("ValueType").Should().Be(expectedValueType);
    }

    [Fact]
    public async Task A_committed_offset_is_not_delivered_again_to_the_next_subscription()
    {
        var topic = await fixture.CreateTopicAsync();
        var first = TestEvents.ProductCreated("primeiro");
        var second = TestEvents.ProductCreated("segundo");
        await PublishAsync(topic, first, second);

        // One group across both subscriptions: the point is that the SECOND one resumes where the first
        // committed, which is only meaningful within a group.
        var groupId = KafkaFixture.NewGroupId();

        using (var committing = TestSubscription.Create(KafkaTestOptions.Consumer(fixture, topic, groupId)))
        {
            var received = Deadline.ReadOne(committing.Subscription);
            received.Event.Should().Be(first);

            committing.Subscription.Commit(received);
        }

        using var resuming = TestSubscription.Create(KafkaTestOptions.Consumer(fixture, topic, groupId));

        // Commit(ConsumeResult) stores offset + 1, so "committed" means "handled" rather than "next to
        // read". Getting that wrong by one redelivers every record forever, or skips one per restart.
        Deadline.ReadOne(resuming.Subscription).Event.Should().Be(second);
    }

    [Fact]
    public async Task SeekBack_delivers_the_same_record_again()
    {
        var topic = await fixture.CreateTopicAsync();
        var sent = TestEvents.ProductCreated();
        await PublishAsync(topic, sent);

        using var subscription = TestSubscription.Create(
            KafkaTestOptions.Consumer(fixture, topic, KafkaFixture.NewGroupId()));

        var received = Deadline.ReadOne(subscription.Subscription);
        subscription.Subscription.SeekBack(received);

        var again = Deadline.ReadOne(subscription.Subscription);

        again.PositionDescription.Should().Be(received.PositionDescription);
        again.Event.Should().Be(sent);
    }

    [Fact]
    public async Task Not_committing_is_not_enough_to_get_a_record_again()
    {
        var topic = await fixture.CreateTopicAsync();
        var first = TestEvents.ProductCreated("primeiro");
        var second = TestEvents.ProductCreated("segundo");
        await PublishAsync(topic, first, second);

        using var subscription = TestSubscription.Create(
            KafkaTestOptions.Consumer(fixture, topic, KafkaFixture.NewGroupId()));

        var received = Deadline.ReadOne(subscription.Subscription);
        received.Event.Should().Be(first);

        // Deliberately neither committed nor rewound.
        Deadline.ReadOne(subscription.Subscription).Event.Should().Be(
            second,
            "the read position has already moved past a record by the time the handler sees it, so a " +
            "failed handler that only skips the commit loses the record for the rest of the session - " +
            "which is the whole reason AtLeastOnceDelivery calls SeekBack, and the reason deleting that " +
            "call breaks nothing loudly");
    }

    private async Task PublishAsync(string topic, params ProductCreatedEvent[] events)
    {
        using var producer = TestProducer.Create(KafkaTestOptions.Producer(fixture, topic));

        foreach (var productCreatedEvent in events)
        {
            await producer.Producer.SendAsync(productCreatedEvent, TestContext.Current.CancellationToken);
        }
    }
}
