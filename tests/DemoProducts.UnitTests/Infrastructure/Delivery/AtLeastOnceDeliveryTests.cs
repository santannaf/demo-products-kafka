using DemoProducts.Domain.Events;
using DemoProducts.Infrastructure.Messaging.Delivery;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace DemoProducts.UnitTests.Infrastructure.Delivery;

/// <summary>
/// The offset protocol. Every assertion here was impossible before the subscription seam existed: the
/// rules lived in private methods of a BackgroundService and could only be exercised against a broker.
/// </summary>
public sealed class AtLeastOnceDeliveryTests
{
    private static AtLeastOnceDelivery Delivery() =>
        new(NullLogger<AtLeastOnceDelivery>.Instance);

    private static void Succeeds(ProductCreatedEvent productCreatedEvent, CancellationToken cancellationToken)
    {
    }

    private static void Fails(ProductCreatedEvent productCreatedEvent, CancellationToken cancellationToken) =>
        throw new InvalidOperationException("handler blew up");

    [Fact]
    public void A_handled_message_is_committed()
    {
        var message = FakeProductCreatedSubscription.Message();
        using var subscription = new FakeProductCreatedSubscription(message);

        Delivery().Run(subscription, Succeeds, subscription.Token);

        subscription.Committed.Should().ContainSingle().Which.Should().BeSameAs(message);
        subscription.SoughtBack.Should().BeEmpty();
    }

    [Fact]
    public void A_failed_handler_never_commits()
    {
        using var subscription = new FakeProductCreatedSubscription(FakeProductCreatedSubscription.Message());

        Delivery().Run(subscription, Fails, subscription.Token);

        subscription.Committed.Should().BeEmpty();
    }

    [Fact]
    public void A_failed_handler_rewinds_so_the_message_is_not_skipped()
    {
        // The invariant with no loud failure mode: drop the SeekBack and the message is silently skipped
        // for the rest of the session, with every test still green and every build still clean.
        var message = FakeProductCreatedSubscription.Message();
        using var subscription = new FakeProductCreatedSubscription(message);

        Delivery().Run(subscription, Fails, subscription.Token);

        subscription.SoughtBack.Should().ContainSingle().Which.Should().BeSameAs(message);
    }

    [Fact]
    public void A_failed_handler_pauses_after_rewinding_and_not_before()
    {
        using var subscription = new FakeProductCreatedSubscription(FakeProductCreatedSubscription.Message());

        Delivery().Run(subscription, Fails, subscription.Token);

        // Pausing before the rewind would leave the read position past the failed message for the whole
        // delay, so the order is part of the contract.
        subscription.Calls.Should().ContainInOrder("TryRead", "SeekBack", "PauseBeforeRetry");
        subscription.Pauses.Should().Be(1);
    }

    [Fact]
    public void A_handled_message_is_not_paused_over()
    {
        using var subscription = new FakeProductCreatedSubscription(FakeProductCreatedSubscription.Message());

        Delivery().Run(subscription, Succeeds, subscription.Token);

        subscription.Pauses.Should().Be(0);
    }

    [Fact]
    public void Every_message_in_a_batch_is_committed_in_order()
    {
        var first = FakeProductCreatedSubscription.Message("Primeiro", offset: 1);
        var second = FakeProductCreatedSubscription.Message("Segundo", offset: 2);
        var third = FakeProductCreatedSubscription.Message("Terceiro", offset: 3);
        using var subscription = new FakeProductCreatedSubscription(first, second, third);

        Delivery().Run(subscription, Succeeds, subscription.Token);

        subscription.Committed.Should().Equal(first, second, third);
    }

    [Fact]
    public void A_failure_in_the_middle_of_a_batch_rewinds_only_that_message()
    {
        var first = FakeProductCreatedSubscription.Message("Primeiro", offset: 1);
        var poison = FakeProductCreatedSubscription.Message("Veneno", offset: 2);
        var third = FakeProductCreatedSubscription.Message("Terceiro", offset: 3);
        using var subscription = new FakeProductCreatedSubscription(first, poison, third);

        Delivery().Run(
            subscription,
            (productCreatedEvent, _) =>
            {
                if (productCreatedEvent.Name == "Veneno")
                {
                    throw new InvalidOperationException("handler blew up");
                }
            },
            subscription.Token);

        subscription.Committed.Should().Equal(first, third);
        subscription.SoughtBack.Should().ContainSingle().Which.Should().BeSameAs(poison);
    }

    [Fact]
    public void Nothing_delivered_means_read_again()
    {
        // A poll timeout, a tombstone, or a read failure the subscription already reported.
        using var subscription = new FakeProductCreatedSubscription(null, null, FakeProductCreatedSubscription.Message());

        Delivery().Run(subscription, Succeeds, subscription.Token);

        subscription.Reads.Should().Be(4);
        subscription.Committed.Should().ContainSingle();
        subscription.SoughtBack.Should().BeEmpty();
    }

    [Fact]
    public void Run_returns_normally_when_the_token_is_cancelled()
    {
        using var subscription = new FakeProductCreatedSubscription(FakeProductCreatedSubscription.Message());

        FluentActions.Invoking(() => Delivery().Run(subscription, Succeeds, subscription.Token))
            .Should().NotThrow();
    }

    [Fact]
    public void Run_does_nothing_when_the_token_is_already_cancelled()
    {
        using var subscription = new FakeProductCreatedSubscription(FakeProductCreatedSubscription.Message());
        using var alreadyCancelled = new CancellationTokenSource();
        alreadyCancelled.Cancel();

        Delivery().Run(subscription, Succeeds, alreadyCancelled.Token);

        subscription.Calls.Should().BeEmpty();
    }

    [Fact]
    public void A_handler_cancelled_by_shutdown_does_not_rewind()
    {
        // Shutdown is not a delivery failure: rewinding here would look like a poison message.
        using var subscription = new FakeProductCreatedSubscription(FakeProductCreatedSubscription.Message());

        Delivery().Run(
            subscription,
            (_, _) => throw new OperationCanceledException(),
            subscription.Token);

        subscription.SoughtBack.Should().BeEmpty();
        subscription.Committed.Should().BeEmpty();
        subscription.Pauses.Should().Be(0);
    }

    [Fact]
    public void Run_rejects_a_null_subscription() =>
        FluentActions.Invoking(() => Delivery().Run(null!, Succeeds, CancellationToken.None))
            .Should().Throw<ArgumentNullException>();

    [Fact]
    public void Run_rejects_a_null_handler()
    {
        using var subscription = new FakeProductCreatedSubscription();

        FluentActions.Invoking(() => Delivery().Run(subscription, null!, CancellationToken.None))
            .Should().Throw<ArgumentNullException>();
    }
}
