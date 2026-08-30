using DemoProducts.Infrastructure.Messaging.Delivery;

namespace DemoProducts.IntegrationTests.TestSupport;

/// <summary>
/// Waiting done to a deadline, never with a sleep.
/// </summary>
/// <remarks>
/// A fixed <c>Task.Delay</c> is flaky when the agent is slow and slow when it is fast, and its failure
/// says nothing. Every wait here reports what it last observed, so a red build names the state it timed
/// out in.
/// </remarks>
internal static class Deadline
{
    /// <summary>
    /// Generous on purpose: it is a ceiling on failure, not a duration a passing test spends. A record
    /// already on the topic arrives in milliseconds; this only bounds how long a genuinely broken run
    /// takes to say so.
    /// </summary>
    public static readonly TimeSpan Default = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Reads until a record arrives or the deadline passes. A null from <c>TryRead</c> is a poll timeout
    /// rather than an answer, so it is read again.
    /// </summary>
    public static ReceivedProductCreated ReadOne(IProductCreatedSubscription subscription, TimeSpan? within = null)
    {
        ArgumentNullException.ThrowIfNull(subscription);

        var timeout = within ?? Default;

        return ReadWithin(subscription, timeout)
            ?? throw new TimeoutException(
                $"No ProductCreated arrived within {timeout.TotalSeconds:0.#}s.");
    }

    /// <summary>
    /// Reads for <paramref name="within"/> and returns whatever arrived, or <see langword="null"/>.
    /// </summary>
    /// <remarks>
    /// The whole window is spent whenever the answer is "nothing", so callers proving an absence should
    /// pass a short one. An absence can only ever be evidence, not proof: this says nothing arrived in
    /// that window, and is used where the positive case arrives in milliseconds.
    /// </remarks>
    public static ReceivedProductCreated? ReadOrNothing(IProductCreatedSubscription subscription, TimeSpan within)
    {
        ArgumentNullException.ThrowIfNull(subscription);

        return ReadWithin(subscription, within);
    }

    /// <summary>
    /// Polls <paramref name="condition"/> until it holds or the deadline passes, reporting
    /// <paramref name="describeLastState"/> on failure.
    /// </summary>
    public static void Until(
        Func<bool> condition,
        Func<string> describeLastState,
        TimeSpan? within = null)
    {
        ArgumentNullException.ThrowIfNull(condition);
        ArgumentNullException.ThrowIfNull(describeLastState);

        var timeout = within ?? Default;
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            Thread.Sleep(25);
        }

        throw new TimeoutException(
            $"The condition did not hold within {timeout.TotalSeconds:0.#}s. Last observed: {describeLastState()}");
    }

    private static ReceivedProductCreated? ReadWithin(IProductCreatedSubscription subscription, TimeSpan timeout)
    {
        using var cancellation = new CancellationTokenSource(timeout);

        try
        {
            while (true)
            {
                var received = subscription.TryRead(cancellation.Token);

                if (received is not null)
                {
                    return received;
                }
            }
        }
        catch (OperationCanceledException)
        {
            return null;
        }
    }
}
