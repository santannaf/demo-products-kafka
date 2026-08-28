namespace DemoProducts.Domain.Exceptions;

/// <summary>
/// Carries <see cref="Field"/> so a caller can attribute the failure to the offending field without
/// re-deriving which rule broke — that is what lets the rule live in exactly one place.
/// </summary>
public sealed class InvalidProductNameException : Exception
{
    public InvalidProductNameException(string field, string message)
        : base(message)
    {
        Field = field;
    }

    public InvalidProductNameException(string field, string message, Exception innerException)
        : base(message, innerException)
    {
        Field = field;
    }

    public string Field { get; }
}
