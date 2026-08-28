using Serilog.Core;
using Serilog.Events;

namespace DemoProducts.Infrastructure.Logging;

internal sealed class ShortLevelEnricher : ILogEventEnricher
{
    public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
    {
        ArgumentNullException.ThrowIfNull(logEvent);
        ArgumentNullException.ThrowIfNull(propertyFactory);

        var shortLevel = logEvent.Level switch
        {
            LogEventLevel.Information => "INFO ",
            LogEventLevel.Warning => "WARN ",
            LogEventLevel.Error => "ERROR",
            LogEventLevel.Debug => "DEBUG",
            LogEventLevel.Verbose => "TRACE",
            LogEventLevel.Fatal => "FATAL",
            _ => "UNKWN",
        };

        logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("ShortLevel", shortLevel));
    }
}
