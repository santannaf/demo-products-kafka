namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// What the consuming side reads from the "Kafka" section of appsettings.json — and nothing else. The
/// producer's keys are absent by design.
/// </summary>
/// <remarks>
/// <c>AutoRegisterSchemas</c> is missing from <see cref="SchemaRegistrySettings"/> on purpose: it
/// governs schema publication, which only the producing side does.
/// </remarks>
public sealed class KafkaConsumerOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = string.Empty;

    public string ClientId { get; set; } = string.Empty;

    public ConsumerSettings Consumer { get; set; } = new();

    public SchemaRegistrySettings SchemaRegistry { get; set; } = new();

    public TopicSettings Topics { get; set; } = new();

    public sealed class ConsumerSettings
    {
        public string GroupId { get; set; } = string.Empty;

        public string AutoOffsetReset { get; set; } = "Latest";

        public bool EnableAutoCommit { get; set; }

        public int SessionTimeoutMs { get; set; } = 45_000;

        /// <summary>
        /// The ceiling on time between two polls before the broker considers this consumer gone and
        /// rebalances its partitions away. Handling one record must stay well inside it.
        /// </summary>
        public int MaxPollIntervalMs { get; set; } = 300_000;

        /// <summary>
        /// The most records one poll may return, bounding how much work sits between two polls and
        /// therefore how close handling can get to <see cref="MaxPollIntervalMs"/>.
        /// </summary>
        public int MaxPollRecords { get; set; } = 1_000;

        /// <summary>
        /// How many bytes the broker waits to accumulate before answering a fetch, unless
        /// <see cref="FetchWaitMaxMs"/> expires first. Higher trades latency for fewer, larger round
        /// trips.
        /// </summary>
        public int FetchMinBytes { get; set; } = 200_000;

        /// <summary>
        /// How long the broker may hold a fetch waiting for <see cref="FetchMinBytes"/>. This is the
        /// latency floor on a quiet topic: with nothing arriving, every record waits this long.
        /// </summary>
        public int FetchWaitMaxMs { get; set; } = 400;

        /// <summary>
        /// How long the listener pauses after a failed handler before re-consuming the same offset, so a
        /// permanently failing message does not become a hot loop.
        /// </summary>
        public int RetryDelayMs { get; set; } = 5_000;

        /// <summary>
        /// How many times one record is handed to the handler before the listener gives up on it,
        /// commits past it and moves on.
        /// </summary>
        /// <remarks>
        /// This is the setting that bounds delivery, and it costs something real: a record that fails
        /// this many times is <b>dropped</b>. There is no dead-letter topic here, so the only trace it
        /// leaves is the error log line naming its partition and offset. Raising the value trades
        /// throughput for durability; there is no value that gives both without a dead-letter topic.
        /// </remarks>
        public int MaxAttemptsPerRecord { get; set; } = 2;

        /// <summary>
        /// <see langword="true"/> reads messages as the generated <c>ProductCreatedAvro</c>;
        /// <see langword="false"/> reads them as an Avro <c>GenericRecord</c> and maps by field name.
        /// </summary>
        /// <remarks>
        /// Both paths produce the same <c>ProductCreatedEvent</c> — the difference is where a schema
        /// mismatch is caught. The typed reader fails inside Avro with the schema in hand; the generic
        /// one fails in the mapper, on a field lookup. The generic path also avoids
        /// <c>Avro.ObjectCreator</c>, which resolves record types by name and is the reason ADR 0001
        /// keeps the Consumer off Native AOT.
        /// </remarks>
        public bool EnableAvroReader { get; set; }

        /// <summary>
        /// Whether the offset commit is fire-and-forget. Only <see langword="false"/> is supported here.
        /// </summary>
        /// <remarks>
        /// An asynchronous commit returns before the broker has stored the offset, so a crash in that
        /// window redelivers a record the handler already completed. That is legal at-least-once and
        /// perfectly reasonable — but this listener's contract is "committed means handled", and a
        /// setting that quietly widens the redelivery window would break it without any signal. Rejected
        /// at boot instead of accepted and ignored.
        /// </remarks>
        public bool AsyncAck { get; set; }

        /// <summary>
        /// Whether records are handed to the handler in batches. Only <see langword="false"/> is
        /// supported here.
        /// </summary>
        /// <remarks>
        /// <see cref="MaxAttemptsPerRecord"/> is counted per record, and a batch has no per-record
        /// position to count against: one poison record in a batch of a thousand would either drop the
        /// whole batch or replay the 999 that already succeeded. Supporting batches means deciding that
        /// first, so the key is rejected rather than silently ignored.
        /// </remarks>
        public bool EnableBatchListener { get; set; }
    }

    public sealed class SchemaRegistrySettings
    {
        public string Url { get; set; } = string.Empty;
    }

    public sealed class TopicSettings
    {
        public string ProductCreated { get; set; } = string.Empty;
    }
}
