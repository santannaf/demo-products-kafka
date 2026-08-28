using Serilog.Core;
using Serilog.Events;

namespace DemoProducts.Infrastructure.Logging;

internal sealed class ShortContextEnricher : ILogEventEnricher
{
    private const int MaxLength = 36;

    public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
    {
        ArgumentNullException.ThrowIfNull(logEvent);
        ArgumentNullException.ThrowIfNull(propertyFactory);

        if (!logEvent.Properties.TryGetValue("SourceContext", out var sourceContext))
        {
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("ShortContext", string.Empty));
            return;
        }

        var raw = sourceContext.ToString().Trim('"');
        var truncated = raw.Length > MaxLength ? raw[^MaxLength..] : raw;

        logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("ShortContext", truncated));
    }
}
