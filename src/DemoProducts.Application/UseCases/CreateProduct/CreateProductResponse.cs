namespace DemoProducts.Application.UseCases.CreateProduct;

public sealed record CreateProductResponse(Guid ProductId, string Name, DateTime OccurredAtUtc);
