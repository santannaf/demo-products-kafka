using DemoProducts.Domain.Events;

namespace DemoProducts.Infrastructure.Messaging.Delivery;

/// <summary>
/// One delivery attempt of a <see cref="ProductCreatedEvent"/>, handed to the delivery protocol by an
/// <see cref="IProductCreatedSubscription"/>.
/// </summary>
/// <param name="Event">The event to hand to the application.</param>
/// <param name="Position">
/// Opaque to the protocol, meaningful only to the subscription that produced it: the protocol passes it
/// back to <see cref="IProductCreatedSubscription.Commit"/> or
/// <see cref="IProductCreatedSubscription.SeekBack"/> without ever inspecting it. Typing it as
/// <see cref="object"/> is what keeps broker types out of the protocol.
/// </param>
/// <param name="PositionDescription">
/// A human-readable form of <paramref name="Position"/> for the log line the protocol writes when a
/// handler fails. Only the subscription can format it.
/// </param>
internal sealed record ReceivedProductCreated(
    ProductCreatedEvent Event,
    object Position,
    string PositionDescription);
