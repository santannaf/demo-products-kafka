namespace DemoProducts.Infrastructure.Messaging.Delivery;

/// <summary>
/// The broker operations <see cref="AtLeastOnceDelivery"/> needs, and nothing more. The seam that lets
/// the delivery protocol be exercised without a broker.
/// </summary>
/// <remarks>
/// Subscribing and leaving the consumer group are NOT on this interface: an implementation subscribes
/// when it is constructed and leaves when it is disposed, so the protocol never learns that a topic name
/// or a consumer group exists.
/// </remarks>
internal interface IProductCreatedSubscription
{
    /// <summary>
    /// Blocks until a message arrives, the token is cancelled, or the read fails. Returns
    /// <see langword="null"/> when nothing was delivered — a poll timeout, a tombstone, or a read
    /// failure the implementation has already reported. A null is not an error: the protocol reads again.
    /// </summary>
    /// <exception cref="OperationCanceledException">The token was cancelled.</exception>
    ReceivedProductCreated? TryRead(CancellationToken cancellationToken);

    /// <summary>
    /// Marks <paramref name="received"/> as handled, so it is not delivered again after a restart.
    /// Called only after the handler succeeded.
    /// </summary>
    void Commit(ReceivedProductCreated received);

    /// <summary>
    /// Rewinds so <paramref name="received"/> is delivered again. Not committing is not enough on its
    /// own: a subscription's read position has already moved past the message by the time the handler
    /// runs, so without this the failed message would be skipped for the rest of the session.
    /// </summary>
    void SeekBack(ReceivedProductCreated received);

    /// <summary>
    /// Waits before the next read after a failure, bounding a permanently failing message to one attempt
    /// per delay instead of a hot loop. Returns early when the token is cancelled.
    /// </summary>
    void PauseBeforeRetry(CancellationToken cancellationToken);
}
