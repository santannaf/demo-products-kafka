using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Testcontainers.Kafka;
using Xunit;

namespace DemoProducts.IntegrationTests.TestSupport;

/// <summary>
/// One broker and one Schema Registry for the whole assembly, on a network of their own.
/// </summary>
/// <remarks>
/// <para>
/// <b>What this suite does not prove.</b> Every test here runs on the CLR with reflection intact, so
/// nothing in this project attests Native-AOT compatibility. That is what the <c>native</c> job in
/// <c>.github/workflows/ci.yml</c> is for — it publishes both binaries and runs a real request through
/// them. A green run here says the protocol and the wire format are right, not that the trimmed binary
/// can execute them.
/// </para>
/// <para>
/// The images are the same tags <c>docker-compose.yml</c> pins, because that file's comment records the
/// tag as load-bearing. The consensus protocol deliberately is not: compose runs ZooKeeper to
/// demonstrate the classic topology, and this fixture takes the module's KRaft default because it is one
/// container instead of two and nothing in this repository speaks to ZooKeeper.
/// </para>
/// <para>
/// Host ports are assigned by Docker rather than published on fixed numbers, so the suite runs with the
/// developer's own <c>docker compose up</c> already holding 9092 and 18081.
/// </para>
/// </remarks>
public sealed class KafkaFixture : IAsyncLifetime
{
    /// <summary>
    /// The address Schema Registry uses to reach the broker. It has to be a listener on the container
    /// network: the mapped host port that <see cref="KafkaContainer.GetBootstrapAddress"/> returns is
    /// reachable from the test process and from nowhere inside Docker.
    /// </summary>
    private const string InternalListener = "kafka:19092";

    private const string ImageTag = "7.7.1";

    private readonly INetwork _network = new NetworkBuilder().Build();

    private readonly KafkaContainer _kafka;
    private readonly IContainer _schemaRegistry;

    public KafkaFixture()
    {
        _kafka = new KafkaBuilder($"confluentinc/cp-kafka:{ImageTag}")
            .WithNetwork(_network)
            // WithListener also registers the host part as a network alias, which is what makes
            // "kafka" resolve from the Schema Registry container.
            .WithListener(InternalListener)
            .Build();

        _schemaRegistry = new ContainerBuilder($"confluentinc/cp-schema-registry:{ImageTag}")
            .WithNetwork(_network)
            .WithPortBinding(8081, assignRandomHostPort: true)
            .WithEnvironment("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", $"PLAINTEXT://{InternalListener}")
            .WithEnvironment("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            // Readiness from the registry answering its own API, never a sleep: it starts a Kafka
            // consumer of its own over the _schemas topic and is not usable until that has caught up.
            .WithWaitStrategy(Wait.ForUnixContainer()
                .UntilHttpRequestIsSucceeded(request => request.ForPort(8081).ForPath("/subjects")))
            .Build();
    }

    /// <summary>The broker address as librdkafka wants it, with the scheme stripped.</summary>
    public string BootstrapServers => _kafka.GetBootstrapAddress().Replace("PLAINTEXT://", string.Empty, StringComparison.Ordinal);

    public string SchemaRegistryUrl => $"http://{_schemaRegistry.Hostname}:{_schemaRegistry.GetMappedPublicPort(8081)}";

    public async ValueTask InitializeAsync()
    {
        await _network.CreateAsync().ConfigureAwait(false);
        await _kafka.StartAsync().ConfigureAwait(false);
        await _schemaRegistry.StartAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a topic nothing else in the run will touch, and returns its name.
    /// </summary>
    /// <remarks>
    /// Kafka has no <c>TRUNCATE</c>, so a fresh topic per test is what a state reset looks like here: it
    /// also gives the test a fresh Schema Registry subject, since the subject is derived from the topic
    /// name. Created explicitly rather than left to <c>auto.create.topics.enable</c> — a consumer that
    /// subscribes before the first produce gets no partition assignment and simply reads nothing, which
    /// is the race <c>ci.yml</c> records having lost the first time that job ran for real.
    /// </remarks>
    public async Task<string> CreateTopicAsync()
    {
        var topic = $"product-created-{Guid.NewGuid():N}";

        using var admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = BootstrapServers,
        }).Build();

        await admin.CreateTopicsAsync([
            new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 },
        ]).ConfigureAwait(false);

        return topic;
    }

    /// <summary>
    /// A consumer group no other test has committed against, so <c>AutoOffsetReset=Earliest</c> makes
    /// the order between publishing and subscribing irrelevant.
    /// </summary>
    public static string NewGroupId() => $"integration-{Guid.NewGuid():N}";

    public async ValueTask DisposeAsync()
    {
        await _schemaRegistry.DisposeAsync().ConfigureAwait(false);
        await _kafka.DisposeAsync().ConfigureAwait(false);
        await _network.DisposeAsync().ConfigureAwait(false);
    }
}
