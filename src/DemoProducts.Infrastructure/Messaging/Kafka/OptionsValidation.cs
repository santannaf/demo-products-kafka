namespace DemoProducts.Infrastructure.Messaging.Kafka;

/// <summary>
/// The guards shared by the producer and consumer options validators. Every failure names the
/// configuration key so a bad value is diagnosable from the boot log alone.
/// </summary>
internal static class OptionsValidation
{
    public static void RequireValue(List<string> failures, string value, string key)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            failures.Add($"{key} is required.");
        }
    }

    public static void RequirePositive(List<string> failures, int value, string key)
    {
        if (value <= 0)
        {
            failures.Add($"{key} must be greater than zero.");
        }
    }

    public static void RequireEnum<TEnum>(List<string> failures, string value, string key)
        where TEnum : struct, Enum
    {
        if (!Enum.TryParse<TEnum>(value, ignoreCase: true, out _))
        {
            failures.Add($"{key} has an unsupported value '{value}'. Allowed: {string.Join(", ", Enum.GetNames<TEnum>())}.");
        }
    }
}
