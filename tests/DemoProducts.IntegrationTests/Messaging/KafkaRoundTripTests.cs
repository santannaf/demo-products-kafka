using DemoProducts.Application.Events.ProductCreated;
using DemoProducts.Infrastructure.Messaging.Delivery;
using DemoProducts.IntegrationTests.TestSupport;
using FluentAssertions;
using Xunit;

namespace DemoProducts.IntegrationTests.Messaging;

/// <summary>
/// One event, all the way across: the producer adapter, a real broker and Schema Registry, the
/// subscription adapter, the delivery protocol, and the application's own handler.
/// </summary>
/// <remarks>
/// Every piece here has its own test elsewhere in this project. This one exists for the failure those
/// cannot have: two halves that each satisfy their own test and disagree with each other. It is also the
/// only place the correlation between event 2002 and event 1001 is asserted, which is the whole reason
/// both lines carry <c>DomainEventId</c>.
/// </remarks>
[Collection(KafkaCollection.Name)]
public sealed class KafkaRoundTripTests(KafkaFixture fixture)
{
    [Fact]
    public async Task An_event_published_by_the_api_side_is_consumed_handled_and_committed()
    {
        var topic = await fixture.CreateTopicAsync();
        var groupId = KafkaFixture.NewGroupId();
        var sent = TestEvents.ProductCreated();

        using var producer = TestProducer.Create(KafkaTestOptions.Producer(fixture, topic));
        await producer.Producer.SendAsync(sent, TestContext.Current.CancellationToken);

        var handlerLogger = new RecordingLogger<ProductCreatedEventHandler>();
        var handler = new ProductCreatedEventHandler(handlerLogger);

        using (var subscription = TestSubscription.Create(KafkaTestOptions.Consumer(fixture, topic, groupId)))
        {
            var delivery = new AtLeastOnceDelivery(
                new RecordingLogger<AtLeastOnceDelivery>(), maxAttemptsPerRecord: 2);

            // Linked to the test's own token so a cancelled test does not leave the loop running.
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(
                TestContext.Current.CancellationToken);

            // Bridged with GetAwaiter().GetResult() because the protocol is synchronous, exactly as
            // ProductCreatedListener does it. The listener itself is not under test here: it adds a host
            // and a DI scope per message, and neither can fail in a way this assertion would notice.
            var loop = Task.Run(() => delivery.Run(
                subscription.Subscription,
                (productCreatedEvent, token) => handler.HandleAsync(productCreatedEvent, token).GetAwaiter().GetResult(),
                cancellation.Token),
                TestContext.Current.CancellationToken);

            try
            {
                Deadline.Until(
                    () => handlerLogger.Entries.Any(entry => entry.EventId == 1001),
                    () => $"the handler wrote {handlerLogger.Entries.Count} log entries, none of them event 1001");
            }
            finally
            {
                await cancellation.CancelAsync();
                await loop;
            }
        }

        var consumed = handlerLogger.Entries.Single(entry => entry.EventId == 1001);
        consumed.Property("ProductId").Should().Be(sent.ProductId);
        consumed.Property("Name").Should().Be(sent.Name);
        consumed.Property("OccurredAt").Should().Be(sent.OccurredAtUtc);

        var published = ((RecordingLogger)producer.Logger).Entries.Single(entry => entry.EventId == 2002);
        consumed.Property("DomainEventId").Should().Be(
            published.Property("DomainEventId"),
            "the two lines share that placeholder so one event id correlates a publish with its consume");

        // The protocol committed after the handler succeeded, so the same group finds nothing left. Two
        // seconds because the positive case arrives in milliseconds - an absence is evidence here, and a
        // longer window would only make a passing test slower.
        using var resuming = TestSubscription.Create(KafkaTestOptions.Consumer(fixture, topic, groupId));
        Deadline.ReadOrNothing(resuming.Subscription, TimeSpan.FromSeconds(2)).Should().BeNull();
    }
}
