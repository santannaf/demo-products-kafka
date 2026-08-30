using System.Text.Json;
using Confluent.Kafka;

namespace DemoProducts.IntegrationTests.TestSupport;

/// <summary>
/// The topic and the registry read without going through anything this repository wrote, so an
/// assertion about the wire says something about the wire.
/// </summary>
internal static class RawTopic
{
    /// <summary>Reads one message as the bytes the broker actually stored.</summary>
    public static ConsumeResult<string, byte[]> ReadOne(
        KafkaFixture fixture,
        string topic,
        TimeSpan? within = null)
    {
        ArgumentNullException.ThrowIfNull(fixture);

        var timeout = within ?? Deadline.Default;

        using var consumer = new ConsumerBuilder<string, byte[]>(new ConsumerConfig
        {
            BootstrapServers = fixture.BootstrapServers,
            GroupId = KafkaFixture.NewGroupId(),
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        }).Build();

        consumer.Subscribe(topic);

        try
        {
            var result = consumer.Consume(timeout)
                ?? throw new TimeoutException($"Nothing was published to '{topic}' within {timeout.TotalSeconds:0.#}s.");

            return result;
        }
        finally
        {
            consumer.Close();
        }
    }

    /// <summary>The id the registry assigned to the latest version of <paramref name="subject"/>.</summary>
    public static async Task<int> LatestSchemaIdAsync(KafkaFixture fixture, string subject)
    {
        ArgumentNullException.ThrowIfNull(fixture);

        using var httpClient = new HttpClient();

        var body = await httpClient
            .GetStringAsync(new Uri($"{fixture.SchemaRegistryUrl}/subjects/{Uri.EscapeDataString(subject)}/versions/latest"))
            .ConfigureAwait(false);

        return JsonDocument.Parse(body).RootElement.GetProperty("id").GetInt32();
    }

    /// <summary>Every subject the registry currently knows about.</summary>
    public static async Task<IReadOnlyList<string>> SubjectsAsync(KafkaFixture fixture)
    {
        ArgumentNullException.ThrowIfNull(fixture);

        using var httpClient = new HttpClient();

        var body = await httpClient
            .GetStringAsync(new Uri($"{fixture.SchemaRegistryUrl}/subjects"))
            .ConfigureAwait(false);

        return [.. JsonDocument.Parse(body).RootElement.EnumerateArray().Select(element => element.GetString()!)];
    }
}
