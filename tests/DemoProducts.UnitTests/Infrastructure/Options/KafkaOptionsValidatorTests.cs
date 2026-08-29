using DemoProducts.Infrastructure.Messaging.Kafka;
using FluentAssertions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Xunit;

namespace DemoProducts.UnitTests.Infrastructure.Options;

/// <summary>
/// The validators run under ValidateOnStart, so every failure here is a boot failure in production.
/// What is asserted is the message: a boot log that names the key is the whole point of hand-writing
/// these instead of using data annotations.
/// </summary>
public sealed class KafkaOptionsValidatorTests
{
    private static KafkaProducerOptions ValidProducerOptions() => new()
    {
        BootstrapServers = "localhost:9092",
        ClientId = "demo-products-kafka-api",
        Producer = new KafkaProducerOptions.ProducerSettings { Acks = "All", MessageTimeoutMs = 30_000 },
        SchemaRegistry = new KafkaProducerOptions.SchemaRegistrySettings { Url = "http://localhost:8081" },
        Topics = new KafkaProducerOptions.TopicSettings { ProductCreated = "product-created" },
    };

    private static KafkaConsumerOptions ValidConsumerOptions() => new()
    {
        BootstrapServers = "localhost:9092",
        ClientId = "demo-products-kafka-consumer",
        Consumer = new KafkaConsumerOptions.ConsumerSettings
        {
            GroupId = "demo-products-kafka-consumer",
            AutoOffsetReset = "Earliest",
            EnableAutoCommit = false,
            SessionTimeoutMs = 45_000,
            MaxPollIntervalMs = 300_000,
            RetryDelayMs = 5_000,
        },
        SchemaRegistry = new KafkaConsumerOptions.SchemaRegistrySettings { Url = "http://localhost:8081" },
        Topics = new KafkaConsumerOptions.TopicSettings { ProductCreated = "product-created" },
    };

    private static ValidateOptionsResult ValidateProducer(KafkaProducerOptions options) =>
        new KafkaProducerOptionsValidator().Validate(name: null, options);

    private static ValidateOptionsResult ValidateConsumer(KafkaConsumerOptions options) =>
        new KafkaConsumerOptionsValidator().Validate(name: null, options);

    [Fact]
    public void Producer_options_from_appsettings_are_valid() =>
        ValidateProducer(ValidProducerOptions()).Succeeded.Should().BeTrue();

    [Fact]
    public void Consumer_options_from_appsettings_are_valid() =>
        ValidateConsumer(ValidConsumerOptions()).Succeeded.Should().BeTrue();

    [Fact]
    public void An_unsupported_compression_type_is_rejected_and_lists_the_legal_values()
    {
        var options = ValidProducerOptions();
        options.Producer.CompressionType = "brotli";

        var result = ValidateProducer(options);

        // The legal values are in the message because the caller cannot see Confluent's enum from a
        // configuration file, and "unsupported value" alone would send them to the source.
        result.Failures.Should().ContainMatch("*Kafka:Producer:CompressionType*brotli*Snappy*");
    }

    [Fact]
    public void An_unsupported_partitioner_is_rejected_naming_the_key()
    {
        // The name a reader arriving from the Java client would reach for. There is no librdkafka
        // equivalent, so the boot has to say so rather than fall back to a default.
        var options = ValidProducerOptions();
        options.Producer.Partitioner = "UniformStickyPartitioner";

        var result = ValidateProducer(options);

        result.Failures.Should().ContainMatch("*Kafka:Producer:Partitioner*ConsistentRandom*");
    }

    [Fact]
    public void A_producer_retry_count_below_one_is_rejected_naming_the_key()
    {
        var options = ValidProducerOptions();
        options.Producer.MaxRetries = 0;

        ValidateProducer(options).Failures.Should().ContainMatch("*Kafka:Producer:MaxRetries*");
    }

    [Fact]
    public void An_attempt_cap_below_one_is_rejected_naming_the_key()
    {
        // Zero would mean "hand the record to nobody and commit past it", which reads as a typo rather
        // than an intention. The boot says so instead of silently dropping the topic.
        var options = ValidConsumerOptions();
        options.Consumer.MaxAttemptsPerRecord = 0;

        ValidateConsumer(options).Failures.Should().ContainMatch("*Kafka:Consumer:MaxAttemptsPerRecord*");
    }

    [Fact]
    public void An_asynchronous_ack_is_rejected_naming_the_key()
    {
        var options = ValidConsumerOptions();
        options.Consumer.AsyncAck = true;

        ValidateConsumer(options).Failures.Should().ContainMatch("*Kafka:Consumer:AsyncAck*");
    }

    [Fact]
    public void A_batch_listener_is_rejected_naming_the_key()
    {
        var options = ValidConsumerOptions();
        options.Consumer.EnableBatchListener = true;

        ValidateConsumer(options).Failures.Should().ContainMatch("*Kafka:Consumer:EnableBatchListener*");
    }

    [Fact]
    public void A_fetch_that_may_outlast_the_poll_deadline_is_rejected()
    {
        // Both values are individually legal: a 5 minute poll deadline and a 5 minute fetch wait. Together
        // they starve the poll loop into a rebalance while the consumer is perfectly healthy, which is the
        // kind of pairing no single-field rule can see.
        var options = ValidConsumerOptions();
        options.Consumer.MaxPollIntervalMs = 300_000;
        options.Consumer.FetchWaitMaxMs = 300_000;

        var result = ValidateConsumer(options);

        result.Failures.Should().ContainMatch("*Kafka:Consumer:FetchWaitMaxMs*Kafka:Consumer:MaxPollIntervalMs*");
    }

    [Fact]
    public void The_settings_that_bound_a_poll_are_all_rejected_when_not_positive()
    {
        var options = ValidConsumerOptions();
        options.Consumer.MaxPollRecords = 0;
        options.Consumer.FetchMinBytes = 0;
        options.Consumer.FetchWaitMaxMs = 0;

        var result = ValidateConsumer(options);

        // Reported together rather than one boot at a time: the validator collects, and a restart per
        // typo is what asserting only the first one would buy.
        result.Failures.Should().ContainMatch("*Kafka:Consumer:MaxPollRecords*");
        result.Failures.Should().ContainMatch("*Kafka:Consumer:FetchMinBytes*");
        result.Failures.Should().ContainMatch("*Kafka:Consumer:FetchWaitMaxMs*");
    }

    [Fact]
    public void The_Api_boots_from_a_configuration_with_no_consumer_section()
    {
        // The point of the split. Before it, the Api refused to start without a group id it never reads.
        var options = Bind<KafkaProducerOptions>(new Dictionary<string, string?>
        {
            ["Kafka:BootstrapServers"] = "localhost:9092",
            ["Kafka:ClientId"] = "demo-products-kafka-api",
            ["Kafka:Producer:Acks"] = "All",
            ["Kafka:Producer:MessageTimeoutMs"] = "30000",
            ["Kafka:SchemaRegistry:Url"] = "http://localhost:8081",
            ["Kafka:Topics:ProductCreated"] = "product-created",
        });

        ValidateProducer(options).Succeeded.Should().BeTrue();
    }

    [Fact]
    public void The_Consumer_boots_from_a_configuration_with_no_producer_section()
    {
        var options = Bind<KafkaConsumerOptions>(new Dictionary<string, string?>
        {
            ["Kafka:BootstrapServers"] = "localhost:9092",
            ["Kafka:ClientId"] = "demo-products-kafka-consumer",
            ["Kafka:Consumer:GroupId"] = "demo-products-kafka-consumer",
            ["Kafka:Consumer:AutoOffsetReset"] = "Earliest",
            ["Kafka:Consumer:EnableAutoCommit"] = "false",
            ["Kafka:Consumer:SessionTimeoutMs"] = "45000",
            ["Kafka:Consumer:MaxPollIntervalMs"] = "300000",
            ["Kafka:Consumer:RetryDelayMs"] = "5000",
            ["Kafka:SchemaRegistry:Url"] = "http://localhost:8081",
            ["Kafka:Topics:ProductCreated"] = "product-created",
        });

        ValidateConsumer(options).Succeeded.Should().BeTrue();
    }

    [Fact]
    public void The_key_names_did_not_change_with_the_split()
    {
        // Splitting the options changed which host reads which key, not the keys themselves — anything
        // else would silently break Kafka__* environment overrides.
        var options = Bind<KafkaProducerOptions>(new Dictionary<string, string?>
        {
            ["Kafka:Producer:Acks"] = "Leader",
            ["Kafka:Producer:EnableIdempotence"] = "false",
            ["Kafka:Topics:ProductCreated"] = "product-created",
        });

        options.Producer.Acks.Should().Be("Leader");
        options.Producer.EnableIdempotence.Should().BeFalse();
        options.Topics.ProductCreated.Should().Be("product-created");
    }

    private static TOptions Bind<TOptions>(Dictionary<string, string?> values)
        where TOptions : class, new()
    {
        var options = new TOptions();

        new ConfigurationBuilder()
            .AddInMemoryCollection(values)
            .Build()
            .GetSection("Kafka")
            .Bind(options);

        return options;
    }

    [Fact]
    public void Producer_validator_names_a_missing_bootstrap_servers()
    {
        var options = ValidProducerOptions();
        options.BootstrapServers = "";

        ValidateProducer(options).Failures.Should().Contain("Kafka:BootstrapServers is required.");
    }

    [Fact]
    public void Producer_validator_rejects_an_unsupported_acks_and_lists_the_allowed_values()
    {
        var options = ValidProducerOptions();
        options.Producer.Acks = "Some";

        ValidateProducer(options).Failures.Should()
            .ContainSingle(failure => failure.StartsWith("Kafka:Producer:Acks has an unsupported value 'Some'."));
    }

    [Fact]
    public void Producer_validator_rejects_a_non_positive_message_timeout()
    {
        var options = ValidProducerOptions();
        options.Producer.MessageTimeoutMs = 0;

        ValidateProducer(options).Failures
            .Should().Contain("Kafka:Producer:MessageTimeoutMs must be greater than zero.");
    }

    [Fact]
    public void Consumer_validator_names_a_missing_group_id()
    {
        var options = ValidConsumerOptions();
        options.Consumer.GroupId = "";

        ValidateConsumer(options).Failures.Should().Contain("Kafka:Consumer:GroupId is required.");
    }

    [Fact]
    public void Consumer_validator_rejects_auto_commit()
    {
        // Auto-commit would silently break "commit only after the handler succeeds", so it is refused
        // at boot rather than assumed.
        var options = ValidConsumerOptions();
        options.Consumer.EnableAutoCommit = true;

        ValidateConsumer(options).Failures.Should().Contain(
            "Kafka:Consumer:EnableAutoCommit must be false: the listener commits only after the handler succeeds.");
    }

    [Fact]
    public void Consumer_validator_rejects_a_non_positive_retry_delay()
    {
        var options = ValidConsumerOptions();
        options.Consumer.RetryDelayMs = 0;

        ValidateConsumer(options).Failures
            .Should().Contain("Kafka:Consumer:RetryDelayMs must be greater than zero.");
    }

    [Fact]
    public void A_validator_reports_every_failure_at_once()
    {
        var options = ValidConsumerOptions();
        options.BootstrapServers = "";
        options.Consumer.GroupId = "";
        options.Consumer.MaxPollIntervalMs = -1;

        ValidateConsumer(options).Failures.Should().HaveCount(3);
    }
}
