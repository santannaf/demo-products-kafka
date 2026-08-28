using DemoProducts.Application.Abstractions.Messaging;
using DemoProducts.Domain.Events;
using DemoProducts.Domain.Products;

namespace DemoProducts.Application.UseCases.CreateProduct;

public sealed class CreateProductUseCase(
    ISendProductCreatedEventProvider sendProductCreatedEventProvider,
    TimeProvider timeProvider)
{
    public async Task<CreateProductResponse> ExecuteAsync(
        CreateProductRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);

        var product = Product.Create(request.Name);
        var productCreatedEvent = ProductCreatedEvent.From(product, timeProvider);

        // Awaited on purpose: the caller is told the event was acknowledged by the broker, or gets the
        // failure. Fire-and-forget here would return 201 for an event that was never published.
        await sendProductCreatedEventProvider
            .SendAsync(productCreatedEvent, cancellationToken)
            .ConfigureAwait(false);

        return new CreateProductResponse(
            productCreatedEvent.ProductId,
            productCreatedEvent.Name,
            productCreatedEvent.OccurredAtUtc);
    }
}
