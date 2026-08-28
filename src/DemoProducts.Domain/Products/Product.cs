using DemoProducts.Domain.Exceptions;

namespace DemoProducts.Domain.Products;

/// <summary>
/// A product. This sample has no persistence: the instance lives only long enough to build the
/// ProductCreated event that is published to Kafka.
/// </summary>
public sealed class Product
{
    public const int MaxNameLength = 200;

    private Product(Guid id, string name)
    {
        Id = id;
        Name = name;
    }

    public Guid Id { get; }

    public string Name { get; }

    public static Product Create(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new InvalidProductNameException("Product name is required.");
        }

        var trimmedName = name.Trim();

        if (trimmedName.Length > MaxNameLength)
        {
            throw new InvalidProductNameException(
                $"Product name must be at most {MaxNameLength} characters.");
        }

        return new Product(Guid.NewGuid(), trimmedName);
    }
}
