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
# 1. Kafka + Schema Registry + Control Center (classic Zookeeper topology, Confluent Platform 7.7.x)
docker compose up -d

# ... or the Zookeeper-less KRaft variant, same published ports:
# docker compose -f docker-compose.kraft.yml up -d
```

| Service         | URL                      |
|-----------------|--------------------------|
| Kafka broker    | `localhost:9092`         |
| Schema Registry | `http://localhost:18081` |
| Control Center  | `http://localhost:9021`  |

Schema Registry is published on `18081`, not the conventional `8081`, because that port is taken on the
machine this sample is maintained on; inside the compose network it is still `8081`. Control Center is
last to answer — it builds its own internal topics before serving `9021`. It runs in `management` mode: topic browser, message inspection (Avro decoded through Schema
Registry), consumer groups and the schema view. It runs in `all` mode; the throughput/latency charts stay
empty regardless, because neither `cp-kafka` nor `apache/kafka` ships the Confluent Metrics Reporter —
that needs `cp-server`, which is more broker than this sample warrants. An earlier revision used
`management` mode for that reason, which was the wrong trade: an empty chart costs less than a feature
that is switched off.

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
curl http://localhost:18081/subjects
curl http://localhost:18081/subjects/product-created-value/versions/latest
```

Or the same thing with a UI: <http://localhost:9021> → cluster → **Topics** → `product-created` →
**Messages** for the decoded payloads, **Schema** for the registered Avro definition.

`requests.http` has the same calls, including the `400` validation case.

> **CI runs this smoke on every push**, against the native binaries and a real broker
> (`.github/workflows/ci.yml`). Run it by hand when changing the messaging adapters: it is the only gate
> that executes the binary, and it is what found both trimming defects in ADR 0001's amendment while the
> build and publish gates were green.

## Tests

```bash
dotnet test
```

Two settings are needed, and missing either one is quiet rather than loud. `global.json` picks the runner
on the `dotnet test` side (`"test": { "runner": "Microsoft.Testing.Platform" }`), and
`<UseMicrosoftTestingPlatformRunner>true</UseMicrosoftTestingPlatformRunner>` in the test csproj picks the
matching entry point on the test app's side. With only the first, the executable keeps xUnit's own console
runner, rejects the arguments `dotnet test` hands it, and the command reports **"Zero tests ran"** with no
hint that 41 tests exist — while running the executable directly still passes. If `dotnet test` ever prints
`total: 0`, check that property before believing the suite is empty.

No broker, no container, no Docker: the suite is hermetic and runs in about a second.

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

## CI

`.github/workflows/ci.yml`, three gates in the order they are worth running:

| Job | What it protects | Why it is not enough on its own |
| --- | --- | --- |
| **Build and test** | source-level trim/AOT analysers over `src/**`, warnings as errors, plus the 64 tests | the analysers are per-assembly; a dependency's annotations are only checked at publish |
| **Native publish + warning baseline** | that both binaries publish, silently, and that the suppressed diagnostics have not moved from **4** (Api) and **60** (Consumer), nor grown to include `DemoProducts.*` | a clean publish says the code is analysable, not that the binary runs |
| **Produce smoke** | a real `POST /products` on the **native** binaries against a real broker and Schema Registry, asserting `201` and the matching `ProductCreated consumed` line | — |

The baseline job is what pays for the `NoWarn` in ADR 0001's second amendment: it re-publishes with
`-p:ShowThirdPartyTrimWarnings=true` and fails if the count moves **in either direction**. Upward means
the suppression grew; downward means a dependency started annotating and the ADR is stale.

It deletes `obj/Release/net10.0/linux-x64` before each publish, and that `rm -rf` is load-bearing: ILC is
incremental, so publishing the same project twice compiles once and the second run prints nothing. Without
it the baseline reads `0` and passes vacuously — which is exactly how the step failed the first time it
was run.

## Native AOT

`DemoProducts.Api` carries `<PublishAot>true</PublishAot>`. `DemoProducts.Consumer` deliberately does not
— the Avro *deserialize* path resolves record types by name via `Activator`, which ILC cannot analyse.
That reason now applies to one of two read paths: it is the typed reader
(`Kafka:Consumer:EnableAvroReader = true`) that goes through `Avro.ObjectCreator`, and the generic reader
this sample ships by default does not. Whether that is enough to publish the Consumer natively has **not**
been measured — a clean publish is a claim only a publish can make, and nobody has run one.

**The publish gate passes.** No extra flag is needed on the command line:

```bash
dotnet publish src/DemoProducts.Api/DemoProducts.Api.csproj \
  -c Release -r linux-x64 --self-contained true -o ./publish/api
# -> 4 ILC trim/AOT warnings, 0 errors
# -> publish/api/DemoProducts.Api : ELF 64-bit pie executable, x86-64
#    and no managed DemoProducts.Api.dll beside it - a genuine native image
```

### About those 4 warnings

`Directory.Build.props` sets `TreatWarningsAsErrors=true`, which NativeAOT propagates into
`IlcTreatWarningsAsErrors`. Left at that, the publish **fails**: ILC reports the trim/AOT diagnostics as
errors and stops at "Generating native code".

There were **139**, then **43**, and now **4**. Two changes did it, and they were different in kind:

| | Warnings | What changed |
| --- | --- | --- |
| Start | 139 | — |
| Package pin | 43 | `Confluent.*` 2.14.2 → 2.15.0, `Apache.Avro` 1.12.1 → 1.12.2, which ship `net10.0` assets |
| Newtonsoft off the Api's graph | **4** | the producing side stopped using Confluent's Avro serde and Schema Registry client |

**34 of the 43 were Newtonsoft.Json**, and none of them came from logging — Serilog does not use
Newtonsoft. They came from two direct dependencies: `Confluent.SchemaRegistry`, whose `.nuspec` declares
`Newtonsoft.Json` in every target framework group and whose REST client serialises every request and
response with it, and `Apache.Avro`, which parses schema JSON with it. Newtonsoft binds by reflection over
property names, which is exactly what trimming removes.

So the fix was to stop reaching that code from the Api. The producing side now owns two small pieces
instead:

- `Messaging/Kafka/SchemaRegistry/SchemaRegistryRestClient.cs` — the two calls the producer makes,
  register and look up, over `System.Text.Json` with a source-generated context.
- `Messaging/Kafka/Wire/AvroBinaryWriter.cs` and `ProductCreatedAvroEncoder.cs` — the Avro binary
  encoding for this one flat record: zig-zag varints, UTF-8 strings, and Confluent's five-byte frame.

The **Consumer still uses Confluent's serde and client**. It runs on the CLR where reflection is intact,
and it needs schema-by-id resolution and caching that the producing direction does not. Deleting a
dependency the Api could not trim is not a reason to hand-write one the Consumer uses safely.

The encoder is pinned by tests that read its bytes back with the **real `Apache.Avro` reader** — the same
one the Consumer uses — rather than against a second hand-written expectation. A byte-for-byte constant
written by the encoder's author proves only that the two agree with each other.

What remains is 4, all `Confluent.Kafka`, none of them Newtonsoft:

| Warning | Where | Status |
| --- | --- | --- |
| `IL2067` | `Librdkafka.SetDelegates` | rooted by `[DynamicDependency]`; the diagnostic stays because the reflection is inside the package |
| `IL2091` | `Marshal.PtrToStructure<T>` | the P/Invoke marshalling of librdkafka structs |
| `IL2026`, `IL2075` | `OAuthBearer.Aws.AwsAutoWireDispatcher` | AWS OAuth wiring this application never configures |

**4 was the floor, and reaching 0 took a suppression.** The publish is quiet by default because each
entry-point project appends those codes to `$(NoWarn)` just before ILC runs — scoped to a target so the
C# compiler still reports them, and reversible with `-p:ShowThirdPartyTrimWarnings=true`, which prints
all 4 (and all 60 for the Consumer). ADR 0001's second amendment carries what that costs. What follows is
why 4 was the floor in the first place, and it is unchanged: A trim diagnostic is reported at the call
site, and all four sites are inside `Confluent.Kafka`: the annotation that would silence them has to come
from the package. `[DynamicDependency]` roots what the reflection looks for — it fixes the behaviour, and
`SetDelegates` proves it leaves the warning standing. Removing reachability is what took 43 to 4, but
these four hang off `ProducerBuilder.Build()` and `ProduceAsync`. And they will not clear on their own:
`Confluent.Kafka 2.15.0`'s `net10.0` asset carries no trim annotations at all — not one
`[DynamicallyAccessedMembers]`, `[RequiresUnreferencedCode]` or `[RequiresDynamicCode]` in the whole
assembly. Retargeting a TFM is not annotating for trimming.

Two of them, on `AwsAutoWireDispatcher`, are new: they did not exist at `Confluent.Kafka 2.14.2`, which
the upgrade traded for 96 others. Pinning back would drop them and also drop `MaxPollRecords`, which does
not exist in that version — a real throughput control for a diagnostic about a code path this service
cannot execute, since it configures no `SaslMechanism`. ADR 0001 records why they are documented rather
than suppressed.

So `src/DemoProducts.Api/DemoProducts.Api.csproj` still sets:

```xml
<IlcTreatWarningsAsErrors>false</IlcTreatWarningsAsErrors>
```

This is option A of [`docs/adr/0001-native-aot.md`](docs/adr/0001-native-aot.md). What the flag does
**not** do:

- it does not touch the C# compiler warnings — `dotnet build -c Release` is still **0 warnings, 0 errors**;
- it is not what makes the publish quiet — that is a separate, deliberate `NoWarn`, recorded in ADR
  0001's second amendment. There are still no `[UnconditionalSuppressMessage]` attributes and no
  `#pragma warning disable IL...` anywhere in `src/**`;
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

It does **not** exercise the produce path — `_SCHEMA` initialises lazily on the first serialize. Run the
*Smoke test* section above but start the Api as `./publish/api/DemoProducts.Api` instead of `dotnet run`:
a `201` plus the `ProductCreated consumed.` line proves all three warning families at once, on the native
image.

**That smoke has now been run, and it failed twice before it passed.** Both failures were real trimming
defects that the boot smoke and both static gates missed, and both are fixed:

| Symptom on the native binary (CLR: `201`) | Cause | Fix |
| --- | --- | --- |
| `500` — `InvalidOperationException: Sequence contains no matching element` in `Librdkafka.SetDelegates` | it binds every librdkafka entry point via `GetRuntimeMethods().Single(...)`; ILC keeps the `NativeMethods` types but trims their P/Invoke members | three `[DynamicDependency]` roots in `Messaging/Kafka/DependencyInjection.cs` |
| `502` — `Empty schema; error code: 42201` | `Confluent.SchemaRegistry` serialises its REST DTOs with Newtonsoft.Json; trimmed of their properties they serialise to `{}`. Captured through a logging proxy: the registration POST body was **2 bytes**, `{}` | `<TrimmerRootAssembly Include="Confluent.SchemaRegistry" />` in the Api csproj |

Both roots were confirmed load-bearing by ablation: removing either one brings its failure straight back,
on `Confluent.* 2.15.0` too. Neither is a suppression — they root the reflection so it finds what it looks
for, and ILC verifies the type names (a typo would raise `IL2036`).

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
Kafka:Producer                  Acks, EnableIdempotence, MessageTimeoutMs,          (read by the Api)
                                MaxRetries, CompressionType, Partitioner
Kafka:Consumer                  GroupId, AutoOffsetReset, EnableAutoCommit,         (read by the Consumer)
                                SessionTimeoutMs, MaxPollIntervalMs, MaxPollRecords,
                                FetchMinBytes, FetchWaitMaxMs, RetryDelayMs,
                                MaxAttemptsPerRecord, EnableAvroReader, AsyncAck,
                                EnableBatchListener
Kafka:SchemaRegistry:Url                                                            (both)
Kafka:SchemaRegistry:AutoRegisterSchemas                                            (Api only)
Kafka:Topics:ProductCreated                                                         (both)
Serilog                         MinimumLevel.Default, MinimumLevel.Override, OutputTemplate
```

**Both `appsettings.json` files carry the whole `Kafka` shape**, producer and consumer together, so one
file shows every knob the way a Spring `application.yml` does. Each host still binds and validates **only
the keys it reads**: `KafkaProducerOptions` in the Api, `KafkaConsumerOptions` in the Consumer, both from
the same `Kafka` section. Every key is overridable with `Kafka__Section__Key` as an environment variable.

> That symmetry has one cost worth knowing: a host does not validate the half it ignores. A typo in
> `Kafka:Consumer:*` inside the **Api's** file is silent, because the Api never binds that section — and
> the reverse for `Kafka:Producer:*` in the Consumer's. The alternative, each host binding both, is what
> ADR 0001 removed on purpose: it made the Api refuse to start without a `GroupId` it never reads.

The matching validator runs at boot under `ValidateOnStart()` and fails the host with the offending key
named. It also **rejects `Kafka:Consumer:EnableAutoCommit = true`**: the key stays configurable as the
brief asks, but the sample's commit-after-success contract is enforced rather than assumed.

The poll and fetch settings are the ones that decide throughput and whether the group rebalances under
load:

- **`MaxPollIntervalMs`** (300000) is the deadline. Go longer than this between two polls and the broker
  declares this consumer dead and moves its partitions elsewhere.
- **`MaxPollRecords`** (1000) bounds how many records one poll returns, and therefore how much work sits
  between two polls. It is the knob that keeps handling time inside the deadline above.
- **`FetchMinBytes`** (200000) and **`FetchWaitMaxMs`** (400) are the trade on the broker side: it holds a
  fetch until it has 200 KB or 400 ms have passed, whichever comes first. Raising `FetchMinBytes` means
  fewer, larger round trips; `FetchWaitMaxMs` is the latency floor on a quiet topic. The boot rejects a
  `FetchWaitMaxMs` at or above `MaxPollIntervalMs`, because a fetch the broker may hold past the poll
  deadline starves the loop into a rebalance while the consumer is perfectly healthy — and neither value
  is wrong on its own.

Four more buy something and cost something, so they are worth reading before changing:

- **`Kafka:Consumer:MaxAttemptsPerRecord` bounds at-least-once delivery.** A record handed to the handler
  this many times without succeeding is committed past and **dropped** — the alternative, retrying it
  forever, stops the partition for every message queued behind it. Neither is safe; this is the one that
  keeps the consumer moving. With no dead-letter topic here, the `Giving up on offset …` error line is
  that record's only remaining trace. Raise it to trade throughput for durability.
- **`Kafka:Consumer:EnableAvroReader` chooses where a schema mismatch is caught.** `true` deserialises
  into the generated `ProductCreatedAvro`, so a wrong field fails inside Avro with the schema in hand.
  `false` deserialises into an Avro `GenericRecord` and maps by field name in
  `ProductCreatedGenericRecordMapper`, where the field names are string literals — a renamed field goes
  from a compiler error to a runtime one. Both paths produce the same `ProductCreatedEvent`, and both are
  exercised end to end.
- **`Kafka:Consumer:AsyncAck` and `Kafka:Consumer:EnableBatchListener` only accept `false`,** and the boot
  says so rather than ignoring a `true`. An asynchronous commit returns before the broker stored the
  offset, which widens the redelivery window past this listener's "committed means handled" contract; a
  batch listener has no per-record position for `MaxAttemptsPerRecord` to count against, so one poison
  record would either drop its whole batch or replay the ones that already succeeded. Both are solvable —
  neither is solved here, and a silently ignored key is worse than a refused one.
- **`Kafka:Producer:Partitioner` has no `UniformStickyPartitioner`.** That is a Java-client class with no
  librdkafka equivalent; the legal values are Confluent's own — Random, Consistent, ConsistentRandom,
  Murmur2, Murmur2Random — and the boot rejects anything else, naming them. `ConsistentRandom` is
  librdkafka's default and what this sample ships. It changes nothing here either way: every message
  carries the product id as its key, so only the keyed branch is ever taken.

Two notes on the shape of the configuration:

- **Neither host carries the other's keys.** The Api boots without `Kafka:Consumer:GroupId` and the
  Consumer without `Kafka:Producer:Acks`; previously both refused to start without configuration they
  never read.
- **`EnableAutoOffsetStore` is not a configuration key.** It is pinned to `false` in
  `KafkaProductCreatedSubscription<TValue>`, because committing only after success requires the offset
  store to stay manual; exposing it would let configuration silently break the contract.

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
- **The AOT publish carries 4 third-party ILC warnings** (see above), all `Confluent.Kafka` and none
  fixable from here. The produce smoke has been run against the native binary and passes; it found two
  real trimming defects on the way, both fixed and both recorded in ADR 0001's amendment.
- **No persistence.** The product exists only long enough to build the event, which is why `201 Created`
  carries no `Location` header — there is no `GET /products/{id}` to point at.
- **No dead-letter topic.** A message whose handler keeps failing is retried every
  `Kafka:Consumer:RetryDelayMs` until `Kafka:Consumer:MaxAttemptsPerRecord` is spent, then committed past
  and **dropped**, with the `Giving up on offset …` error line as its only trace. The cap is what stops a
  poison record from blocking its partition forever; a dead-letter topic is what would let it do that
  without losing the record, and there is none here.
- **The Api's Avro encoder handles one flat record of primitives.** `AvroBinaryWriter` understands `long`
  and `string`, which is all `product-created.avsc` contains. Nested records, unions, enums, arrays or
  `fixed` are not implemented, and adding one means going back to `Apache.Avro` on the producing side
  rather than growing that file.
- **`SchemaRegistry:AutoRegisterSchemas = true`** is convenient locally and normally discouraged outside
  development. Do not copy this default into a real service.
