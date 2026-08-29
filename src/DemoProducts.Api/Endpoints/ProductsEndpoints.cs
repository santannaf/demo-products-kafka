using DemoProducts.Application.UseCases.CreateProduct;
using Microsoft.AspNetCore.Http.HttpResults;

namespace DemoProducts.Api.Endpoints;

internal static class ProductsEndpoints
{
    public static void MapProductsEndpoints(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        endpoints.MapPost("/products", CreateProductAsync)
            .WithName("CreateProduct");
    }

    // No validation here on purpose: Product.Create owns the name rules, and GlobalExceptionHandler
    // turns InvalidProductNameException into the field-scoped 400. A copy of the rules in this method
    // would make the domain's copy unreachable and let the two drift apart.
    private static async Task<Created<CreateProductResponse>> CreateProductAsync(
        CreateProductRequest request,
        CreateProductUseCase createProductUseCase,
        CancellationToken cancellationToken)
    {
        var response = await createProductUseCase
            .ExecuteAsync(request, cancellationToken)
            .ConfigureAwait(false);

        // No Location header: this sample has no persistence, so there is no GET /products/{id} to point
        // at, and a Location that answers 404 is worse than none.
        return TypedResults.Created((string?)null, response);
    }
}
