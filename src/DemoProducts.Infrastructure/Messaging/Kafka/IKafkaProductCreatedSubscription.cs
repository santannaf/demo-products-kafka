using DemoProducts.Infrastructure.Messaging.Delivery;

namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// A subscription plus the lifetime that owns the consumer group membership.
/// </summary>
/// <remarks>
/// The disposal lives here rather than on <see cref="IProductCreatedSubscription"/> deliberately: joining
/// and leaving a consumer group is a Kafka concern, and the delivery protocol is written so it cannot
/// learn that consumer groups exist. What the protocol needs is on the seam; what the host needs to close
/// is on this one.
/// </remarks>
internal interface IKafkaProductCreatedSubscription : IProductCreatedSubscription, IDisposable;
