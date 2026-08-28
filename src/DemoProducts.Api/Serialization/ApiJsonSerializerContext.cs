using System.Text.Json.Serialization;
using DemoProducts.Application.UseCases.CreateProduct;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace DemoProducts.Api.Serialization;

/// <summary>
/// Every type that crosses the HTTP boundary, error payloads included. With reflection-based JSON off,
/// a type missing from this list throws at the moment it is first serialized — and a happy-path smoke
/// never touches <see cref="ProblemDetails"/> or <see cref="HttpValidationProblemDetails"/>, so the
/// first 400 or 502 is where the omission would surface.
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    WriteIndented = false)]
[JsonSerializable(typeof(CreateProductRequest))]
[JsonSerializable(typeof(CreateProductResponse))]
[JsonSerializable(typeof(ProblemDetails))]
[JsonSerializable(typeof(HttpValidationProblemDetails))]
internal sealed partial class ApiJsonSerializerContext : JsonSerializerContext;
