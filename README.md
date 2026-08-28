# demo-products-kafka

A small, didactic **C# 14 / .NET 10** sample: `POST /products` builds a `ProductCreated` event, publishes
it to Kafka as **Avro through Schema Registry**, and a `BackgroundService` consumes it, maps the Avro
contract back to the application event, calls a handler and logs it with **Serilog** — committing the
offset only after the handler succeeds.

```text
POST /products -> Application -> ProductCreatedEvent -> Kafka producer -> Avro -> Schema Registry
              -> Kafka topic -> KafkaProductCreatedSubscription -> AtLeastOnceDelivery
              -> ProductCreatedEventHandler -> Serilog
```

## Layout

```text
src/DemoProducts.Domain           Product, ProductCreatedEvent, domain exception. Depends on nothing.
src/DemoProducts.Application      Use case, handler, and the outbound port. No Kafka/Avro/Schema Registry.
src/DemoProducts.Infrastructure   Kafka, Avro, Schema Registry, Serilog wiring. Implements the port,
                                  and hosts the delivery protocol and both of its adapters.
src/DemoProducts.Api              Minimal API, POST /products. Native AOT.
src/DemoProducts.Consumer         Worker host. Program.cs and nothing else.
tests/DemoProducts.UnitTests      xUnit v3. Domain rules, options validators, the Avro round-trip,
                                  and the delivery protocol against a fake subscription.
```

`CONTEXT.md` carries the domain glossary — what a Product, a ProductCreated event, a subscription seam
and the rewind rule mean here.

Dependencies point inward: `Api`/`Consumer` → `Application`, `Infrastructure`; `Infrastructure` →
`Application`; `Application` → `Domain`; `Domain` → nothing. `Domain` and `Application` reference **no**
`Confluent.*` and **no** Avro package — the seam is
`Application/Abstractions/Messaging/ISendProductCreatedEventProvider`.

## Run it

Three terminals.

```bash
# 1. Kafka + Schema Registry (classic Zookeeper topology, Confluent Platform 7.7.x)
docker compose up -d

# ... or the Zookeeper-less KRaft variant, same published ports:
# docker compose -f docker-compose.kraft.yml up -d
```

```bash
# 2. The consumer
dotnet run --project src/DemoProducts.Consumer
```

```bash
# 3. The api
dotnet run --project src/DemoProducts.Api
```

## Smoke test (manual, on purpose)

```bash
curl -X POST http://localhost:5080/products \
  -H 'Content-Type: application/json' \
  -d '{"name":"Notebook"}'
```

Expected: `201 Created` with `{"productId":"...","name":"Notebook","occurredAtUtc":"..."}`, and terminal 2
printing

```text
2026-08-28 20:07:00 [7] INFO  Events.ProductCreatedEventHandler - ProductCreated consumed. EventId=… ProductId=… Name=Notebook OccurredAt=…
```

Confirm the schema really went through Schema Registry (nothing on the wire is JSON):

```bash
curl http://localhost:8081/subjects
curl http://localhost:8081/subjects/product-created-value/versions/latest
```

`requests.http` has the same calls, including the `400` validation case.

> The smoke is documented rather than scripted: automating it would need a backgrounded server plus a
> `kill`, which this repository's delivery policy forbids. `dotnet build` is the automated gate.

## Tests

```bash
dotnet test
```

`global.json` opts into the Microsoft.Testing.Platform runner — .NET 10's `dotnet test` no longer drives
VSTest, and xUnit v3 test projects carry their own runner. No broker, no container, no Docker: the suite
is hermetic and runs in about a second.

What is covered, and why each earns its place:

- **`Product.Create`** — the name rules. Reachable for the first time: the endpoint used to re-check the
  same two rules, so nothing ever reached the domain guards over HTTP.
- **`KafkaProducerOptionsValidator` / `KafkaConsumerOptionsValidator`** — including that each host binds
  from a configuration with no trace of the other's section, and that the key names survived the split.
- **`ProductCreatedAvroMapper`** — the `DateTimeKind.Utc` round-trip, in both directions.
- **`AtLeastOnceDelivery`** — the offset protocol, against a fake subscription: a handled message is
  committed, a failed one is never committed, is rewound, and is paused over *after* the rewind. Deleting
  the `SeekBack` call turns three of these red; before the seam existed it turned nothing red.

## Build

```bash
dotnet restore DemoProducts.slnx
dotnet build DemoProducts.slnx -c Release
```

`Directory.Build.props` sets `IsAotCompatible=true` and `TreatWarningsAsErrors=true` on every `src/**`
project, so this build **is** the source-level trim/AOT analyser gate. It currently passes with 0 warnings.

## Native AOT

`DemoProducts.Api` carries `<PublishAot>true</PublishAot>`. `DemoProducts.Consumer` deliberately does not
— the Avro *deserialize* path resolves record types by name via `Activator`, which ILC cannot analyse.

**The publish gate passes.** No extra flag is needed on the command line:

```bash
dotnet publish src/DemoProducts.Api/DemoProducts.Api.csproj \
  -c Release -r linux-x64 --self-contained true -o ./publish/api
# -> 139 ILC trim/AOT warnings, 0 errors
# -> publish/api/DemoProducts.Api : ELF 64-bit pie executable, x86-64, stripped, ~22 MB
#    and no managed DemoProducts.Api.dll beside it - a genuine native image
```

### About those 139 warnings

`Directory.Build.props` sets `TreatWarningsAsErrors=true`, which NativeAOT propagates into
`IlcTreatWarningsAsErrors`. Left at that, the publish **fails**: ILC reports 139 trim/AOT diagnostics as
errors and stops at "Generating native code". They come from **132 × `Newtonsoft.Json`** (a transitive
dependency of `Apache.Avro 1.12.1`, used to parse the schema JSON), **5 × `Confluent.SchemaRegistry`** and
**2 × `Confluent.Kafka`**. **None originate in `DemoProducts.*`**, and none are fixable from this
repository — no package pin resolves them at these versions.

So `src/DemoProducts.Api/DemoProducts.Api.csproj` sets:

```xml
<IlcTreatWarningsAsErrors>false</IlcTreatWarningsAsErrors>
```

This is option A of [`docs/adr/0001-native-aot.md`](docs/adr/0001-native-aot.md), which carries the
reachability argument for each of the three families and grades how far each one is actually proven. Read
it before trusting the binary in anything that matters. What the flag does **not** do:

- it does not touch the C# compiler warnings — `dotnet build -c Release` is still **0 warnings, 0 errors**;
- it does not hide anything: there is no `<NoWarn>` for an IL code, no `[UnconditionalSuppressMessage]`
  and no `#pragma warning disable IL...` anywhere in `src/**`. All 139 still print, each naming its method;
- it does not apply solution-wide — only the Api is AOT-published, so only the Api carries it.

The number to watch is **0 diagnostics from `DemoProducts.*`**. A warning originating in this repository's
own code means the ADR's premise broke and needs amending, not tolerating.

### Verifying it yourself

```bash
# the publish gate, bare metal (clang is NOT required - see below)
dotnet publish src/DemoProducts.Api/DemoProducts.Api.csproj -c Release -r linux-x64 --self-contained true -o ./publish/api

# or in a pinned, reproducible image
docker build -f Dockerfile.api -t demo-products-api .

# the boot smoke: one invalid key, so the process validates, fails and exits on its own
cd publish/api && env Kafka__BootstrapServers= ./DemoProducts.Api
# -> ERROR ... Hosting failed to start
# -> OptionsValidationException: Kafka:BootstrapServers is required.
```

That boot smoke is worth running: in the *native binary* it proves Serilog's code-based configuration
survives trimming (template, `ThreadId`, `ShortLevel` and `ShortContext` enrichers all render), the
source-generated configuration binder bound the `Kafka` tree, the DI graph builds, and
`KafkaProducerOptionsValidator` runs. `docker run --rm -e Kafka__BootstrapServers= demo-products-api` shows the
same from inside the image.

It does **not** exercise the produce path — `_SCHEMA` initialises lazily on the first serialize. To close
that last gap, run the *Smoke test* section above but start the Api as `./publish/api/DemoProducts.Api`
instead of `dotnet run`: a `201` plus the `ProductCreated consumed.` line proves all three warning
families at once, on the native image.

Two things the planning notes got wrong, corrected by actually running the gate:

- **`clang` is not needed here.** It is absent on this machine (only `gcc`), but ILC linked the image
  successfully anyway, and the `linux-x64` ILCompiler packs were already in the local NuGet cache.
  `Dockerfile.api` installs it for reproducibility, not because the host requires it.
- **The failure was at analysis, not at the linker.** With the diagnostics promoted to errors ILC never
  reached the link step at all — so the fix was a severity decision, not a missing toolchain.

## Configuration

Everything is in `appsettings.json` — no broker URL, topic name, group id or port is hardcoded:

```text
Api:Urls                        (Api only)
Kafka:BootstrapServers          Kafka:ClientId                  (both)
Kafka:Producer                  Acks, EnableIdempotence, MessageTimeoutMs           (Api only)
Kafka:Consumer                  GroupId, AutoOffsetReset, EnableAutoCommit,         (Consumer only)
                                SessionTimeoutMs, MaxPollIntervalMs, RetryDelayMs
Kafka:SchemaRegistry:Url                                                            (both)
Kafka:SchemaRegistry:AutoRegisterSchemas                                            (Api only)
Kafka:Topics:ProductCreated                                                         (both)
Serilog                         MinimumLevel.Default, MinimumLevel.Override, OutputTemplate
```

Each host binds and validates **only the keys it reads**: `KafkaProducerOptions` in the Api,
`KafkaConsumerOptions` in the Consumer, both from the same `Kafka` section. The key names are the same
ones as before the split, so `Kafka__*` environment overrides are unaffected.

The matching validator runs at boot under `ValidateOnStart()` and fails the host with the offending key
named. It also **rejects `Kafka:Consumer:EnableAutoCommit = true`**: the key stays configurable as the
brief asks, but the sample's commit-after-success contract is enforced rather than assumed.

Two notes on the shape of the configuration:

- **Neither host carries the other's keys.** The Api boots without `Kafka:Consumer:GroupId` and the
  Consumer without `Kafka:Producer:Acks`; previously both refused to start without configuration they
  never read.
- **`EnableAutoOffsetStore` is not a configuration key.** It is pinned to `false` in
  `KafkaProductCreatedSubscription`, because committing only after success requires the offset store to
  stay manual; exposing it would let configuration silently break the contract.

Serilog reads its *values* from `appsettings.json` but is *wired in code*
(`Infrastructure/Logging/SerilogConfiguration.cs`). `ReadFrom.Configuration` resolves sinks and enrichers
by assembly scanning, which a trimmed binary cannot do — and it fails by writing nothing rather than by
throwing. `Serilog.Settings.Configuration` is therefore not referenced.

## The Avro contract

`src/DemoProducts.Infrastructure/Messaging/Kafka/Avro/`

- `Schemas/product-created.avsc` — **the canonical schema.** Flat, four primitive fields.
- `Generated/ProductCreatedAvro.cs` — the `ISpecificRecord` class, committed so the build never needs the
  tool. Its `_SCHEMA` literal holds the same JSON as the `.avsc`.
- `Mappers/ProductCreatedAvroMapper.cs` — `ProductCreatedEvent ⇄ ProductCreatedAvro`, both directions.

The schema JSON therefore lives in two places. Re-sync them with the opt-in target rather than by hand:

```bash
dotnet tool restore
dotnet build -p:GenerateAvro=true
```

The target is off by default (`Avro.targets`, guarded by `'$(GenerateAvro)' == 'true'`), so an ordinary
build never needs `avrogen` and never reaches the network. It passes `--skip-directories` so the file
lands next to the committed one; if your `avrogen` build lacks that flag, drop it and move the generated
file out of the namespace-shaped subfolders it creates.

`OccurredAtUtc` is `timestamp-millis`. Apache.Avro converts it through `DateTime.ToUniversalTime()`, so a
`DateTime` whose `Kind` is `Unspecified` or `Local` would silently shift the instant on the wire. The
mapper pins `DateTimeKind.Utc` in both directions.

## Known limitations

- **The unit suite stops at the seams.** `tests/DemoProducts.UnitTests` covers the domain rules, both
  options validators, the Avro round-trip and the delivery protocol, but nothing exercises a real broker:
  `KafkaProductCreatedSubscription` and `KafkaProductCreatedProducer` — the two adapters — have no
  integration test. Testcontainers against Kafka plus Schema Registry is the next layer.
- **`CreateProductUseCase` is untested.** Three lines of orchestration whose only interesting behaviour —
  that the response is not returned until the broker acknowledges — would be asserted through a mock of
  the port it already depends on. Judged not worth the test.
- **The AOT publish carries 139 third-party ILC warnings** (see above). The published native binary has
  been booted, but no *produce* smoke has been run against it, so the `Confluent.Kafka` P/Invoke family —
  the one the ADR grades as not closed by static argument — stays argued rather than proven.
- **No persistence.** The product exists only long enough to build the event, which is why `201 Created`
  carries no `Location` header — there is no `GET /products/{id}` to point at.
- **No dead-letter topic.** A message whose handler fails permanently is re-consumed every
  `Kafka:Consumer:RetryDelayMs` and blocks its partition. That is the correct consequence of "commit only
  after success" and the deliberate limit of this sample.
- **`SchemaRegistry:AutoRegisterSchemas = true`** is convenient locally and normally discouraged outside
  development. Do not copy this default into a real service.
