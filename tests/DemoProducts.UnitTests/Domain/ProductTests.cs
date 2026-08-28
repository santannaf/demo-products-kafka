using DemoProducts.Domain.Exceptions;
using DemoProducts.Domain.Products;
using FluentAssertions;
using Xunit;

namespace DemoProducts.UnitTests.Domain;

/// <summary>
/// Product.Create is the only gate on a product name. These tests are reachable for the first time:
/// the endpoint used to re-check the same rules, so nothing ever reached the guards below over HTTP.
/// </summary>
public sealed class ProductTests
{
    [Fact]
    public void Create_trims_the_name()
    {
        var product = Product.Create("  Café torrado  ");

        product.Name.Should().Be("Café torrado");
    }

    [Fact]
    public void Create_assigns_a_distinct_id_per_product()
    {
        var first = Product.Create("Café torrado");
        var second = Product.Create("Café torrado");

        first.Id.Should().NotBeEmpty();
        first.Id.Should().NotBe(second.Id);
    }

    [Fact]
    public void Create_accepts_a_name_at_exactly_the_maximum_length()
    {
        var name = new string('a', Product.MaxNameLength);

        Product.Create(name).Name.Should().Be(name);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\t\n")]
    public void Create_rejects_a_blank_name(string name) =>
        FluentActions.Invoking(() => Product.Create(name))
            .Should().Throw<InvalidProductNameException>()
            .Which.Field.Should().Be(nameof(Product.Name));

    [Fact]
    public void Create_rejects_a_null_name() =>
        FluentActions.Invoking(() => Product.Create(null!))
            .Should().Throw<InvalidProductNameException>();

    [Fact]
    public void Create_measures_the_maximum_length_after_trimming()
    {
        // Padding a maximum-length name must not push it over the limit.
        var name = $"  {new string('a', Product.MaxNameLength)}  ";

        FluentActions.Invoking(() => Product.Create(name)).Should().NotThrow();
    }

    [Fact]
    public void Create_rejects_a_name_over_the_maximum_length() =>
        FluentActions.Invoking(() => Product.Create(new string('a', Product.MaxNameLength + 1)))
            .Should().Throw<InvalidProductNameException>()
            .Which.Field.Should().Be(nameof(Product.Name));
}
