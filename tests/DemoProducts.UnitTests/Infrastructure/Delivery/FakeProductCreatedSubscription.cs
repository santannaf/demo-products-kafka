using DemoProducts.Domain.Events;
using DemoProducts.Infrastructure.Messaging.Delivery;

namespace DemoProducts.UnitTests.Infrastructure.Delivery;

/// <summary>
/// The second adapter at the subscription seam — the one that makes it a real seam rather than a
/// hypothetical one. Records what the protocol did instead of talking to a broker.
/// </summary>
/// <remarks>
/// The protocol loops until its token is cancelled, so this fake cancels once its script is exhausted
/// and then behaves like a cancelled subscription. That is what makes a test terminate.
/// </remarks>
internal sealed class FakeProductCreatedSubscription : IProductCreatedSubscription, IDisposable
{
    private readonly Queue<ReceivedProductCreated?> script;
    private readonly CancellationTokenSource cancellation = new();

    public FakeProductCreatedSubscription(params ReceivedProductCreated?[] script)
    {
        this.script = new Queue<ReceivedProductCreated?>(script);
    }

    public CancellationToken Token => cancellation.Token;

    /// <summary>Every call the protocol made, in order.</summary>
    public List<string> Calls { get; } = [];

    public List<ReceivedProductCreated> Committed { get; } = [];

    public List<ReceivedProductCreated> SoughtBack { get; } = [];

    public int Reads { get; private set; }

    public int Pauses { get; private set; }

    public static ReceivedProductCreated Message(string name = "Café torrado", int offset = 0) =>
        new(
            new ProductCreatedEvent(Guid.NewGuid(), Guid.NewGuid(), name, DateTime.UtcNow),
            Position: offset,
            PositionDescription: $"product-created [[0]] @{offset}");

    public ReceivedProductCreated? TryRead(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Reads++;
        Calls.Add(nameof(TryRead));

        if (script.Count == 0)
        {
            // Nothing left to deliver: behave like a subscription whose host is shutting down.
            cancellation.Cancel();
            cancellationToken.ThrowIfCancellationRequested();
        }

        return script.Dequeue();
    }

    public void Commit(ReceivedProductCreated received)
    {
        Calls.Add(nameof(Commit));
        Committed.Add(received);
    }

    public void SeekBack(ReceivedProductCreated received)
    {
        Calls.Add(nameof(SeekBack));
        SoughtBack.Add(received);
    }

    public void PauseBeforeRetry(CancellationToken cancellationToken)
    {
        Calls.Add(nameof(PauseBeforeRetry));
        Pauses++;
    }

    public void Dispose() => cancellation.Dispose();
}
