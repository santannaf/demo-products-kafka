namespace DemoProducts.Infrastructure.Logging;

/// <summary>
/// The values behind the "Serilog" section of appsettings.json. Only values live in configuration;
/// the sink and enricher wiring is written in code (see <see cref="SerilogConfiguration"/>).
/// </summary>
public sealed class SerilogOptions
{
    public const string SectionName = "Serilog";

    public MinimumLevelOptions MinimumLevel { get; set; } = new();

    public string OutputTemplate { get; set; } =
        "{Timestamp:yyyy-MM-dd HH:mm:ss} [{ThreadId}] {ShortLevel} {ShortContext} - {Message:lj}{NewLine}{Exception}";

    public sealed class MinimumLevelOptions
    {
        public string Default { get; set; } = "Information";

        public Dictionary<string, string> Override { get; set; } = [];
    }
}
