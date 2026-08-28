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

        var problemDetails = Describe(exception);
        var statusCode = problemDetails.Status ?? StatusCodes.Status500InternalServerError;

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
            ProblemDetails = problemDetails,
        }).ConfigureAwait(false);
    }

    private static ProblemDetails Describe(Exception exception) => exception switch
    {
        // The domain names the offending field, so the field-scoped body needs no second copy of the
        // rule in the endpoint. Dictionary keys are camel-cased by ApiJsonSerializerContext.
        InvalidProductNameException invalidProductName => new HttpValidationProblemDetails(
            new Dictionary<string, string[]>
            {
                [invalidProductName.Field] = [invalidProductName.Message],
            })
        {
            Status = StatusCodes.Status400BadRequest,
            Title = "Invalid product.",
        },

        // The upstream broker or Schema Registry failed, not the caller.
        EventPublishFailedException => new ProblemDetails
        {
            Status = StatusCodes.Status502BadGateway,
            Title = "Failed to publish the ProductCreated event.",
            Detail = exception.Message,
        },

        // An unmapped exception message may carry internals, so it is logged and not returned.
        _ => new ProblemDetails
        {
            Status = StatusCodes.Status500InternalServerError,
            Title = "Unexpected error.",
        },
    };

    [LoggerMessage(EventId = 2001, Level = LogLevel.Error, Message = "Unhandled exception.")]
    private static partial void LogUnhandled(ILogger logger, Exception exception);

    [LoggerMessage(EventId = 2002, Level = LogLevel.Warning, Message = "Request failed with status {StatusCode}.")]
    private static partial void LogMapped(ILogger logger, int statusCode, Exception exception);
}
