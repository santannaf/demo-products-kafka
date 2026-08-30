using Microsoft.Extensions.Logging;

namespace DemoProducts.IntegrationTests.TestSupport;

/// <summary>One entry a component wrote, kept in the two forms a test can meaningfully assert on.</summary>
/// <param name="Message">The rendered line, which is what an operator reads.</param>
/// <param name="Properties">
/// The structured properties, which is what a log query facets on. Both are kept because they are two
/// renderings of one event and nothing else stops them telling different stories.
/// </param>
internal sealed record RecordedLog(
    LogLevel Level,
    int EventId,
    string Message,
    IReadOnlyDictionary<string, object?> Properties)
{
    public object? Property(string name) => Properties.TryGetValue(name, out var value) ? value : null;
}

/// <summary>
/// Captures what a component logged so a test can assert on the handful of events that are contract.
/// </summary>
/// <remarks>
/// Thread-safe because the delivery loop runs on a background thread while the test asserts from the
/// test thread. Assert only on events with a documented <c>EventId</c>: making every line an assertion
/// turns a wording improvement into a red build, which teaches people to stop improving the wording.
/// </remarks>
internal class RecordingLogger : ILogger
{
    private readonly List<RecordedLog> _entries = [];
    private readonly Lock _gate = new();

    public IReadOnlyList<RecordedLog> Entries
    {
        get
        {
            lock (_gate)
            {
                return [.. _entries];
            }
        }
    }

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        ArgumentNullException.ThrowIfNull(formatter);

        var properties = state is IReadOnlyList<KeyValuePair<string, object?>> values
            ? values.ToDictionary(pair => pair.Key, pair => pair.Value, StringComparer.Ordinal)
            : [];

        var entry = new RecordedLog(logLevel, eventId.Id, formatter(state, exception), properties);

        lock (_gate)
        {
            _entries.Add(entry);
        }
    }
}

/// <summary>
/// The generic face of <see cref="RecordingLogger"/>, for the components that take
/// <see cref="ILogger{TCategoryName}"/> rather than <see cref="ILogger"/>.
/// </summary>
internal sealed class RecordingLogger<T> : RecordingLogger, ILogger<T>;
