using DemoProducts.Application.UseCases.CreateProduct;
using DemoProducts.Domain.Products;
using Microsoft.AspNetCore.Http.HttpResults;

namespace DemoProducts.Api.Endpoints;

internal static class ProductsEndpoints
{
    public static IEndpointRouteBuilder MapProductsEndpoints(this IEndpointRouteBuilder endpoints)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        endpoints.MapPost("/products", CreateProductAsync)
            .WithName("CreateProduct");

        return endpoints;
    }

    private static async Task<Results<Created<CreateProductResponse>, ValidationProblem>> CreateProductAsync(
        CreateProductRequest request,
        CreateProductUseCase createProductUseCase,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.Name))
        {
            return TypedResults.ValidationProblem(new Dictionary<string, string[]>
            {
                ["name"] = ["Name is required."],
            });
        }

        if (request.Name.Trim().Length > Product.MaxNameLength)
        {
            return TypedResults.ValidationProblem(new Dictionary<string, string[]>
            {
                ["name"] = [$"Name must be at most {Product.MaxNameLength} characters."],
            });
        }

        var response = await createProductUseCase
            .ExecuteAsync(request, cancellationToken)
            .ConfigureAwait(false);

        // No Location header: this sample has no persistence, so there is no GET /products/{id} to point
        // at, and a Location that answers 404 is worse than none.
        return TypedResults.Created((string?)null, response);
    }
}
