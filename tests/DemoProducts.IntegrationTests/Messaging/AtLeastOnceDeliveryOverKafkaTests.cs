using DemoProducts.Domain.Events;
using DemoProducts.Infrastructure.Messaging.Delivery;
using DemoProducts.IntegrationTests.TestSupport;
using FluentAssertions;
using Microsoft.Extensions.Logging;
using Xunit;

namespace DemoProducts.IntegrationTests.Messaging;

/// <summary>
/// The delivery protocol driving a real Kafka subscription rather than the fake it is unit-tested with.
/// </summary>
/// <remarks>
/// <c>AtLeastOnceDeliveryTests</c> in the unit tier proves the protocol calls <c>SeekBack</c>,
/// <c>Commit</c> and <c>PauseBeforeRetry</c> in the right order. It cannot prove that doing so has the
/// intended effect on a broker — a fake rewinds because the fake was written to. These tests close that
/// gap: a retried record really arrives again, and a record that exhausts the attempt cap really is
/// stepped over.
/// </remarks>
[Collection(KafkaCollection.Name)]
public sealed class AtLeastOnceDeliveryOverKafkaTests(KafkaFixture fixture)
{
    [Fact]
    public async Task A_record_whose_handler_failed_is_handed_over_again_and_then_committed()
    {
        var topic = await fixture.CreateTopicAsync();
        var first = TestEvents.ProductCreated("primeiro");
        var second = TestEvents.ProductCreated("segundo");
        await PublishAsync(topic, first, second);

        // Fails the first delivery of "primeiro" only, so the run covers a retry that recovers and the
        // move on to the next record in one sequence.
        var handler = new ScriptedHandler((productCreatedEvent, attempt) =>
            productCreatedEvent != first || attempt > 1);

        var logger = await RunUntilAsync(topic, handler, () => handler.Seen.Count >= 3);

        handler.Seen.Should().Equal(first, first, second);
        logger.Entries.Should().ContainSingle(entry => entry.EventId == 3003, "one handler failure was scripted");
        logger.Entries.Should().NotContain(entry => entry.EventId == 3006, "the retry succeeded, so nothing was given up on");
    }

    [Fact]
    public async Task A_record_that_never_succeeds_is_dropped_at_the_attempt_cap_and_the_loop_moves_on()
    {
        var topic = await fixture.CreateTopicAsync();
        var poison = TestEvents.ProductCreated("veneno");
        var next = TestEvents.ProductCreated("seguinte");
        await PublishAsync(topic, poison, next);

        var handler = new ScriptedHandler((productCreatedEvent, _) => productCreatedEvent != poison);

        var logger = await RunUntilAsync(topic, handler, () => handler.Seen.Contains(next));

        // MaxAttemptsPerRecord is 2, and it is a ceiling on attempts rather than on retries.
        handler.Seen.Should().Equal(poison, poison, next);

        var givingUp = logger.Entries.Single(entry => entry.EventId == 3006);
        givingUp.Level.Should().Be(LogLevel.Error);
        givingUp.Property("Attempts").Should().Be(2);

        // At-least-once holds only up to the cap: past it the record is committed over and lost, and
        // with no dead-letter topic here this line is the only trace it leaves. That makes the line part
        // of the contract rather than incidental output.
        givingUp.Message.Should().Contain("committed past and dropped");
    }

    private async Task PublishAsync(string topic, params ProductCreatedEvent[] events)
    {
        using var producer = TestProducer.Create(KafkaTestOptions.Producer(fixture, topic));

        foreach (var productCreatedEvent in events)
        {
            await producer.Producer.SendAsync(productCreatedEvent, TestContext.Current.CancellationToken);
        }
    }

    /// <summary>
    /// Runs the protocol on a background thread until <paramref name="done"/> holds, then stops it and
    /// returns what it logged.
    /// </summary>
    /// <remarks>
    /// The loop is stopped by cancelling rather than by letting it run out, because <c>Run</c> only
    /// returns on cancellation — and it is awaited before the assertions so the log is not read while
    /// another thread is still appending to it.
    /// </remarks>
    private async Task<RecordingLogger> RunUntilAsync(string topic, ScriptedHandler handler, Func<bool> done)
    {
        using var subscription = TestSubscription.Create(
            KafkaTestOptions.Consumer(fixture, topic, KafkaFixture.NewGroupId()));

        var logger = new RecordingLogger<AtLeastOnceDelivery>();
        var delivery = new AtLeastOnceDelivery(logger, maxAttemptsPerRecord: 2);

        // Linked to the test's own token so a cancelled test does not leave the loop running.
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken);

        var loop = Task.Run(
            () => delivery.Run(subscription.Subscription, handler.Handle, cancellation.Token),
            TestContext.Current.CancellationToken);

        try
        {
            Deadline.Until(done, () => $"the handler saw [{string.Join(", ", handler.Seen.Select(e => e.Name))}]");
        }
        finally
        {
            await cancellation.CancelAsync();
            await loop;
        }

        return logger;
    }

    /// <summary>
    /// A handler that fails or succeeds by rule, recording every delivery it was handed.
    /// </summary>
    /// <param name="succeeds">Given the event and which attempt this is for that event, 1-based.</param>
    private sealed class ScriptedHandler(Func<ProductCreatedEvent, int, bool> succeeds)
    {
        private readonly List<ProductCreatedEvent> _seen = [];
        private readonly Lock _gate = new();

        /// <summary>The deliveries in order. Read from the test thread while the loop appends.</summary>
        public IReadOnlyList<ProductCreatedEvent> Seen
        {
            get
            {
                lock (_gate)
                {
                    return [.. _seen];
                }
            }
        }

        public void Handle(ProductCreatedEvent productCreatedEvent, CancellationToken cancellationToken)
        {
            int attempt;

            lock (_gate)
            {
                _seen.Add(productCreatedEvent);
                attempt = _seen.Count(seen => seen == productCreatedEvent);
            }

            if (!succeeds(productCreatedEvent, attempt))
            {
                throw new InvalidOperationException($"Scripted failure for '{productCreatedEvent.Name}'.");
            }
        }
    }
}
