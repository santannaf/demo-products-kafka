using DemoProducts.Application.Events.ProductCreated;
using DemoProducts.Application.UseCases.CreateProduct;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace DemoProducts.Application;

public static class DependencyInjection
{
    public static IServiceCollection AddApplication(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton(TimeProvider.System);
        services.AddScoped<CreateProductUseCase>();
        services.AddScoped<ProductCreatedEventHandler>();

        return services;
    }
}
