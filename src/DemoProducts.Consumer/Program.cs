using DemoProducts.Application;
using DemoProducts.Infrastructure.Logging;
using DemoProducts.Infrastructure.Messaging.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;

var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
{
    Args = args,
    ContentRootPath = AppContext.BaseDirectory,
});

builder.Logging.ClearProviders();
builder.Services.AddSerilog(loggerConfiguration =>
    SerilogConfiguration.Configure(loggerConfiguration, builder.Configuration));

builder.Services
    .AddApplication()
    .AddKafkaConsumer(builder.Configuration);

var host = builder.Build();

await host.RunAsync().ConfigureAwait(false);
