using DemoProducts.Application.Abstractions.Messaging;
using DemoProducts.Domain.Exceptions;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Mvc;

namespace DemoProducts.Api.Middlewares;

internal sealed partial class GlobalExceptionHandler(
    IProblemDetailsService problemDetailsService,
    ILogger<GlobalExceptionHandler> logger) : IExceptionHandler
{
    public async ValueTask<bool> TryHandleAsync(
        HttpContext httpContext,
        Exception exception,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        ArgumentNullException.ThrowIfNull(exception);

        var (statusCode, title) = exception switch
        {
            InvalidProductNameException => (StatusCodes.Status400BadRequest, "Invalid product."),

            // The upstream broker or Schema Registry failed, not the caller.
            EventPublishFailedException => (StatusCodes.Status502BadGateway, "Failed to publish the ProductCreated event."),

            _ => (StatusCodes.Status500InternalServerError, "Unexpected error."),
        };

        if (statusCode == StatusCodes.Status500InternalServerError)
        {
            LogUnhandled(logger, exception);
        }
        else
        {
            LogMapped(logger, statusCode, exception);
        }

        httpContext.Response.StatusCode = statusCode;

        return await problemDetailsService.TryWriteAsync(new ProblemDetailsContext
        {
            HttpContext = httpContext,
            ProblemDetails = new ProblemDetails
            {
                Status = statusCode,
                Title = title,

                // An unmapped exception message may carry internals, so it is logged and not returned.
                Detail = statusCode == StatusCodes.Status500InternalServerError ? null : exception.Message,
            },
        }).ConfigureAwait(false);
    }

    [LoggerMessage(EventId = 2001, Level = LogLevel.Error, Message = "Unhandled exception.")]
    private static partial void LogUnhandled(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 2002, Level = LogLevel.Warning, Message = "Request failed with status {StatusCode}.")]
    private static partial void LogMapped(ILogger logger, int statusCode, Exception exception);
}
