using System.Diagnostics.CodeAnalysis;
using Confluent.SchemaRegistry;
using DemoProducts.Application.Abstractions.Messaging;
using DemoProducts.Infrastructure.Messaging.Kafka.Avro.Generated;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
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
    /// Confluent's SpecificSerializerImpl&lt;T&gt; reads the <c>_SCHEMA</c> static field of the record
    /// type reflectively, so trimming could remove it. Rooting it here makes the trim warning FALSE
    /// rather than hidden — the sanctioned alternative to a suppression.
    /// </remarks>
    [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(ProductCreatedAvro))]
    public static IServiceCollection AddKafkaProducer(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        AddKafkaOptions(services, configuration);

        services.AddSingleton<KafkaConnection>();
        services.AddSingleton<ISendProductCreatedEventProvider, KafkaProductCreatedProducer>();

        return services;
    }

    public static IServiceCollection AddKafkaConsumer(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configuration);

        AddKafkaOptions(services, configuration);

        services.AddSingleton<ISchemaRegistryClient>(serviceProvider =>
            new CachedSchemaRegistryClient(new SchemaRegistryConfig
            {
                Url = serviceProvider.GetRequiredService<IOptions<KafkaOptions>>().Value.SchemaRegistry.Url,
            }));

        return services;
    }

    private static void AddKafkaOptions(IServiceCollection services, IConfiguration configuration)
    {
        // Bound with the concrete type rather than through a generic helper: the configuration binding
        // source generator cannot see through a type parameter (SYSLIB1104) and would fall back to
        // reflection, which a trimmed binary cannot do.
        services
            .AddOptions<KafkaOptions>()
            .Bind(configuration.GetSection(KafkaOptions.SectionName))
            .ValidateOnStart();

        services.TryAddSingleton<IValidateOptions<KafkaOptions>, KafkaOptionsValidator>();
    }
}
