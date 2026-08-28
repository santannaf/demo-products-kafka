using DemoProducts.Api.Endpoints;
using DemoProducts.Api.Middlewares;
using DemoProducts.Api.Serialization;
using DemoProducts.Application;
using DemoProducts.Infrastructure.Logging;
using DemoProducts.Infrastructure.Messaging.Kafka;
using Serilog;

var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions
{
    Args = args,

    // A single-file native binary otherwise resolves appsettings.json against the current working
    // directory rather than against the binary.
    ContentRootPath = AppContext.BaseDirectory,
});

builder.Logging.ClearProviders();
builder.Services.AddSerilog(loggerConfiguration =>
    SerilogConfiguration.Configure(loggerConfiguration, builder.Configuration));

var urls = builder.Configuration["Api:Urls"];
if (!string.IsNullOrWhiteSpace(urls))
{
    builder.WebHost.UseUrls(urls.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
}

builder.Services.ConfigureHttpJsonOptions(options =>
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, ApiJsonSerializerContext.Default));

builder.Services.AddProblemDetails();
builder.Services.AddExceptionHandler<GlobalExceptionHandler>();

builder.Services
    .AddApplication()
    .AddKafkaProducer(builder.Configuration);

var app = builder.Build();

app.UseExceptionHandler();
app.MapProductsEndpoints();

await app.RunAsync().ConfigureAwait(false);
