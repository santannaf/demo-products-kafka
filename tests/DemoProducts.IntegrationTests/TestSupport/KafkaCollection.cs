using Xunit;

namespace DemoProducts.IntegrationTests.TestSupport;

/// <summary>
/// One collection for the whole assembly, so the broker and the registry start once.
/// </summary>
/// <remarks>
/// The trade is explicit: xUnit parallelises across collections, not within one, so every class here
/// runs serially. A container start costs tens of seconds and a fresh topic costs milliseconds, so
/// paying the start once and serialising is the cheaper side by a wide margin.
/// </remarks>
[CollectionDefinition(Name)]
public sealed class KafkaCollection : ICollectionFixture<KafkaFixture>
{
    public const string Name = "kafka";
}
