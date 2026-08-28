namespace DemoProducts.Application.Abstractions.Messaging;

/// <summary>
/// The failure contract of <see cref="ISendProductCreatedEventProvider"/>. The Kafka implementation wraps
/// broker and Schema Registry errors in this, so the Api can answer 502 without referencing Confluent.*.
/// </summary>
public sealed class EventPublishFailedException : Exception
{
    public EventPublishFailedException(string message)
        : base(message)
    {
    }

    public EventPublishFailedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
