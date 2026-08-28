using System.Globalization;
using Microsoft.Extensions.Configuration;
using Serilog;
using Serilog.Events;

namespace DemoProducts.Infrastructure.Logging;

/// <summary>
/// Serilog wiring shared by both hosts.
/// </summary>
/// <remarks>
/// Deliberately NOT <c>ReadFrom.Configuration</c>: that resolves sinks and enrichers by assembly
/// scanning, which a trimmed / Native-AOT binary cannot do — and it fails by writing nothing rather
/// than by throwing. Levels and the output template still come from appsettings.json, so nothing is
/// hardcoded; only the wiring moved from a DSL into C#.
/// </remarks>
public static class SerilogConfiguration
{
    public static LoggerConfiguration Configure(
        LoggerConfiguration loggerConfiguration,
        IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(loggerConfiguration);
        ArgumentNullException.ThrowIfNull(configuration);

        var options = configuration
            .GetSection(SerilogOptions.SectionName)
            .Get<SerilogOptions>() ?? new SerilogOptions();

        loggerConfiguration
            .MinimumLevel.Is(ParseLevel(options.MinimumLevel.Default))
            .Enrich.FromLogContext()
            .Enrich.WithThreadId()
            .Enrich.With<ShortLevelEnricher>()
            .Enrich.With<ShortContextEnricher>()
            .WriteTo.Console(
                outputTemplate: options.OutputTemplate,
                formatProvider: CultureInfo.InvariantCulture);

        foreach (var (source, level) in options.MinimumLevel.Override)
        {
            loggerConfiguration.MinimumLevel.Override(source, ParseLevel(level));
        }

        return loggerConfiguration;
    }

    private static LogEventLevel ParseLevel(string? value) =>
        Enum.TryParse<LogEventLevel>(value, ignoreCase: true, out var level)
            ? level
            : LogEventLevel.Information;
}
