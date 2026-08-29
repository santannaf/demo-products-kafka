using System.Diagnostics.CodeAnalysis;
using Confluent.SchemaRegistry;
using DemoProducts.Application.Abstractions.Messaging;
using DemoProducts.Infrastructure.Messaging.Delivery;
using DemoProducts.Infrastructure.Messaging.Kafka.SchemaRegistry;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// Producer and consumer are two separate entry points on purpose. The Api publishes as a Native-AOT
/// binary, and the Avro DESERIALIZE path resolves record types by name through Avro.ObjectCreator,
/// which is not statically analysable. Keeping the registrations apart means ILC's reachability
/// analysis, starting at the Api's entry point, never roots that path.
/// </summary>
public static class DependencyInjection
{
    /// <remarks>
    /// <para>
    /// The three <c>NativeMethods</c> roots below are the remedy for a fatal reflection point.
    /// <c>Librdkafka.SetDelegates(Type)</c> binds every librdkafka entry point by enumerating
    /// <c>GetRuntimeMethods()</c> over one of these classes and calling <c>.Single(m =&gt; m.Name ==
    /// ...)</c> per delegate. ILC roots the TYPES — they reach <c>SetDelegates</c> as <c>typeof</c>
    /// literals — but nothing references their P/Invoke members, so full trimming removes them and the
    /// first <c>.Single(...)</c> throws <c>Sequence contains no matching element</c> while constructing
    /// the producer. That is the IL2067 the publish reports against <c>SetDelegates</c>. Confluent.Kafka
    /// carries no trim annotations of its own — not even in the net10.0 asset added in 2.15.0 — so the
    /// root has to come from here. All three candidates are rooted because <c>LoadLinuxDelegates</c>
    /// tries them in turn to find the one whose <c>DllImport</c> name matches the librdkafka build
    /// present in the image.
    /// </para>
    /// <para>
    /// There is no root for <c>ProductCreatedAvro</c> any more, and its absence is the point: the
    /// producing side no longer touches the generated record, Confluent's Avro serde or Avro's schema
    /// parser. See <see cref="Wire.ProductCreatedAvroEncoder"/>.
    /// </para>
    /// </remarks>
    [DynamicDependency(
        DynamicallyAccessedMemberTypes.All,
        "Confluent.Kafka.Impl.NativeMethods.NativeMethods",
        "Confluent.Kafka")]
    [DynamicDependency(
        DynamicallyAccessedMemberTypes.All,
        "Confluent.Kafka.Impl.NativeMethods.NativeMethods_Centos8",
        "Confluent.Kafka")]
    [DynamicDependency(
        DynamicallyAccessedMemberTypes.All,
        "Confluent.Kafka.Impl.NativeMethods.NativeMethods_Alpine",
        "Confluent.Kafka")]
    public static IServiceCollection AddKafkaProducer(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        // Bound with the concrete type rather than through a generic helper: the configuration binding
        // source generator cannot see through a type parameter (SYSLIB1104) and would fall back to
        // reflection, which a trimmed binary cannot do. That is also why the two Add methods below
        // repeat the shape instead of sharing one.
        services
            .AddOptions<KafkaProducerOptions>()
            .Bind(configuration.GetSection(KafkaProducerOptions.SectionName))
            .ValidateOnStart();

        services.TryAddSingleton<IValidateOptions<KafkaProducerOptions>, KafkaProducerOptionsValidator>();

        // A bare HttpClient rather than IHttpClientFactory: one long-lived client against one host that
        // never changes needs neither the handler rotation nor the Microsoft.Extensions.Http dependency.
        services.TryAddSingleton(serviceProvider => new SchemaRegistryRestClient(
            new HttpClient
            {
                // The trailing slash matters: without it the last path segment of a base address is
                // replaced rather than appended to.
                BaseAddress = new Uri(
                    serviceProvider.GetRequiredService<IOptions<KafkaProducerOptions>>()
                        .Value.SchemaRegistry.Url.TrimEnd('/') + '/'),
            }));

        services.TryAddSingleton<ProductCreatedSchemaId>();

        // The container creates it, so the container disposes it — that is what flushes in-flight
        // messages at shutdown.
        services.AddSingleton<ISendProductCreatedEventProvider, KafkaProductCreatedProducer>();

        return services;
    }

    /// <remarks>
    /// The same three librdkafka roots as <see cref="AddKafkaProducer"/>, for the same reason: the
    /// P/Invoke binding is shared by both clients, and a consumer built from a trimmed binary fails in
    /// <c>Librdkafka.SetDelegates</c> exactly as the producer did. They are repeated rather than pulled
    /// into a shared helper because <c>[DynamicDependency]</c> is honoured on the method that carries it,
    /// and each entry point has to be rooted from its own reachable method.
    /// </remarks>
    [DynamicDependency(
        DynamicallyAccessedMemberTypes.All,
        "Confluent.Kafka.Impl.NativeMethods.NativeMethods",
        "Confluent.Kafka")]
    [DynamicDependency(
        DynamicallyAccessedMemberTypes.All,
        "Confluent.Kafka.Impl.NativeMethods.NativeMethods_Centos8",
        "Confluent.Kafka")]
    [DynamicDependency(
        DynamicallyAccessedMemberTypes.All,
        "Confluent.Kafka.Impl.NativeMethods.NativeMethods_Alpine",
        "Confluent.Kafka")]
    public static IServiceCollection AddKafkaConsumer(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        services
            .AddOptions<KafkaConsumerOptions>()
            .Bind(configuration.GetSection(KafkaConsumerOptions.SectionName))
            .ValidateOnStart();

        services.TryAddSingleton<IValidateOptions<KafkaConsumerOptions>, KafkaConsumerOptionsValidator>();

        services.TryAddSingleton<ISchemaRegistryClient>(serviceProvider =>
            new CachedSchemaRegistryClient(new SchemaRegistryConfig
            {
                Url = serviceProvider
                    .GetRequiredService<IOptions<KafkaConsumerOptions>>().Value.SchemaRegistry.Url,
            }));

        // The listener is an adapter, not host code: it lives beside the producer adapter so no Avro or
        // Confluent type has to be named from a host project.
        //
        // Built by hand rather than by constructor injection so the attempt cap reaches the protocol as a
        // number: AtLeastOnceDelivery taking IOptions<KafkaConsumerOptions> would put a Kafka type on the
        // one class written specifically not to have any.
        services.AddSingleton(serviceProvider => new AtLeastOnceDelivery(
            serviceProvider.GetRequiredService<ILogger<AtLeastOnceDelivery>>(),
            serviceProvider.GetRequiredService<IOptions<KafkaConsumerOptions>>().Value.Consumer.MaxAttemptsPerRecord));
        services.AddHostedService<ProductCreatedListener>();

        return services;
    }
}
