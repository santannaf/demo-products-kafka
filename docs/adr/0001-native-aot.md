# ADR 0001 — The Api is Native AOT, the Consumer is not

**Status:** accepted, **amended** — see *Amendment: the produce smoke was run, and it failed*
**Context:** `demo-products-kafka`, .NET 10 / C# 14

> The amendment at the end of this file is not a footnote. Two of the reachability arguments below were
> **wrong**, and the produce smoke this ADR listed as "still owed" is what proved it. Read the amendment
> before relying on any per-family grade in the middle of this document.

## Context

The goal requires `<PublishAot>true</PublishAot>` on the **Api**. Both hosts share one Avro contract
(`ProductCreatedAvro : ISpecificRecord`) and one Infrastructure assembly, so the natural question is
whether the Consumer should be published the same way.

Apache.Avro's two directions do not behave the same under trimming:

| Path | Reflection used | AOT verdict |
| --- | --- | --- |
| **Serialize** — `SpecificSerializerImpl<T>` | reads the `_SCHEMA` **static field of a concrete, known type** | rootable with `[DynamicDependency]` → **AOT-safe** |
| **Deserialize** — `Avro.SpecificDefaultReader` → `Avro.ObjectCreator` | resolves record types **by name** via `Activator` | not statically analysable → **not AOT-safe** |

`Apache.Avro 1.12.1` also ships only `netstandard2.0` / `netstandard2.1` assets and depends on
`Newtonsoft.Json`; `Confluent.*` 2.14.2 tops out at `net8.0`. That is exactly the shape that raises ILC
warnings under .NET 10.

## Decision

1. **`DemoProducts.Api` publishes as a native binary.** `PublishAot=true`, `TrimMode=full`,
   `TrimmerSingleWarn=false`, `JsonSerializerIsReflectionEnabledByDefault=false`, and —
   see *option A, adopted* below — `IlcTreatWarningsAsErrors=false`.
2. **`DemoProducts.Consumer` is an ordinary framework-dependent worker.** No `PublishAot`.
   `IsAotCompatible=true` still applies from `Directory.Build.props`, so the source-level analysers run
   over its code; only the whole-program ILC publish is skipped.
3. **Infrastructure exposes two DI entry points**, `AddKafkaProducer(...)` and `AddKafkaConsumer(...)`.
   A single combined `AddKafka()` would put `AvroDeserializer` — and therefore `Avro.ObjectCreator` — on
   the reachability graph that starts at the Api's entry point, for no benefit. Splitting them keeps the
   deserializer out of the Api's trim graph entirely.
4. **The `_SCHEMA` reflection is rooted, not suppressed:**

   ```csharp
   [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(ProductCreatedAvro))]
   public static IServiceCollection AddKafkaProducer(...)
   ```

   This makes the trim warning *false* rather than *hidden*. There are zero
   `[UnconditionalSuppressMessage]`, `#pragma warning disable IL...` or `<NoWarn>` entries for an IL code
   anywhere in `src/**`; adding one requires amending this ADR. `IlcTreatWarningsAsErrors=false` is none
   of those three — it changes the *severity* of diagnostics that are still emitted and still counted,
   and it is argued for on its own terms below rather than folded in here.
5. **The Avro record is flat** — four primitive fields, no nested records, enums or `fixed`. Nested types
   are precisely what drives `ObjectCreator`'s by-name lookup.

## The gates — all four were run

| Gate | Command | Scope | Result |
| --- | --- | --- | --- |
| **Build** | `dotnet build DemoProducts.slnx -c Release` | source-level, per assembly | **PASS** — 0 warnings, 0 errors |
| **Publish** | `dotnet publish src/DemoProducts.Api/... -c Release -r linux-x64 --self-contained` | whole-program ILC | **PASS** — 139 third-party warnings, 0 from `DemoProducts.*`, 0 errors |
| **Image** | `docker build -f Dockerfile.api -t demo-products-api .` | the same publish, pinned toolchain | **PASS** — same 139 warnings, image builds and boots |
| **Boot** | the published native binary with an invalid `Kafka:BootstrapServers` | native runtime | **PASS** — see *What the boot smoke proves* |

| **Produce** | `POST /products` against a real broker, on the native binary | the produce path end to end | **PASS — only after two fixes.** Failed twice first; see the amendment |

The produce gate was added after this ADR was first written. It found two real defects that the other
four gates all missed.

Two assumptions made while planning turned out to be wrong, and are corrected here rather than left in
place:

1. **`clang` is not required on this machine.** `clang` is indeed absent (only `gcc`), but ILC linked the
   native image successfully with the toolchain already installed. The `linux-x64` ILCompiler and
   NativeAOT runtime packs (`10.0.11`) were already in the local NuGet cache, so the publish ran fully
   offline. `Dockerfile.api` remains useful for reproducibility, but it is not the only way to run this
   gate.
2. **The publish gate was not failing at the linker — it was failing at analysis.** With ILC diagnostics
   promoted to errors, ILC never reached the link step at all. The distinction matters: the fix was a
   severity decision, not a missing toolchain.

### The measured result

`Directory.Build.props` sets `TreatWarningsAsErrors=true`, which the NativeAOT targets propagate to
`IlcTreatWarningsAsErrors`. Left at that, ILC reports **139 trim/AOT diagnostics as errors** and the
publish stops at "Generating native code":

| Origin | Count | Nature |
| --- | --- | --- |
| `Newtonsoft.Json` | 132 | reflective contract resolver, `TypeDescriptor.GetConverter`, `MakeGenericType`, `dynamic` binders |
| `Confluent.SchemaRegistry` | 5 | incl. `SpecificSerializerImpl<T>.ExtractSchemaData` → `Activator.CreateInstance` (IL2067) and `Utils.Transform*Async` → `MakeGenericType` (IL3050) |
| `Confluent.Kafka` | 2 | `Librdkafka.SetDelegates` → `GetRuntimeMethods`, `Marshal.PtrToStructure<T>` |
| **`DemoProducts.*`** | **0** | — |

By IL code: 41 × IL3050, 27 × IL2070, 18 × IL2075, 17 × IL2026, 8 × IL2067, 7 × IL2080, 7 × IL2046,
4 × IL2055, 3 × IL2060, 3 × IL2057, 2 × IL2072, 1 × IL2091, 1 × IL2077.

`Apache.Avro 1.12.1` pulls `Newtonsoft.Json` in and uses it to parse the schema JSON behind
`Avro.Schema.Parse`, which the `_SCHEMA` static initialiser reaches.

> **Amended.** This paragraph originally read "`Newtonsoft.Json` is not a direct dependency". It is one:
> `Confluent.SchemaRegistry`'s own `.nuspec` declares `Newtonsoft.Json` in every target framework group,
> and its REST client serialises every request and response DTO with it. That omission is exactly what
> made the reachability argument below wrong.

> **Amended.** The next paragraph originally read "**None of these are fixable from this repository** …
> no package pin resolves them at these versions". A pin resolved 96 of the 139: moving `Confluent.*` to
> 2.15.0 and `Apache.Avro` to 1.12.2 — versions that ship `net10.0` assets — drops the count to **43**
> (25 `Confluent.SchemaRegistry`, 10 `Newtonsoft.Json`, 4 `Confluent.Kafka`, 4 `Avro`, 0 `DemoProducts.*`).
> The claim was made without checking the feed. The rest of the paragraph still holds for those 43.

## The decision on the conflict: option A, adopted

The goal requires **both** `PublishAot=true` on the Api **and** `Confluent.SchemaRegistry.Serdes.Avro`.
At these versions the two are in genuine tension. Three options were on the table:

- **A — demote the ILC diagnostics to warnings** for `DemoProducts.Api`, argue reachability per family,
  and make a smoke on the published binary the real proof. The only option that ships a native binary.
- **B — drop `PublishAot` from the Api**, as already decided for the Consumer, and treat Avro + Schema
  Registry as incompatible with AOT at these versions.
- **C — replace the serialization stack**, or wait for `Apache.Avro` / `Confluent.*` assets targeting
  `net10.0` with trim annotations.

**Option A is adopted.** B contradicts an explicit, mandatory requirement of the goal, and C is not
available at these versions. A is the only option that satisfies the requirement, and the earlier state —
a gate that could not pass while `Dockerfile.api` and the README told the reader it did — was worse than
either: it left a documented command that fails with no explanation.

The demotion is a single property on a single project, `src/DemoProducts.Api/DemoProducts.Api.csproj`:

```xml
<IlcTreatWarningsAsErrors>false</IlcTreatWarningsAsErrors>
```

It sits in the csproj rather than on a publish command line so that the bare-metal publish, the README
command, `Dockerfile.api` and CI all get the same result from one source of truth — a flag that has to be
remembered at four call sites is a flag that will be forgotten at one of them.

What it does **not** do, deliberately:

- It does not touch the C# compiler warnings. `TreatWarningsAsErrors=true` still holds for every
  `src/**` project, and the build gate is still 0 warnings / 0 errors.
- It does not hide anything. There is still **no** `<NoWarn>` for an IL code, **no**
  `[UnconditionalSuppressMessage]` and **no** `#pragma warning disable IL...` anywhere in `src/**`. All
  139 diagnostics are still emitted, still name their method (that is what `TrimmerSingleWarn=false`
  buys), and are still countable in the publish log.
- It does not widen to the solution. Only the Api is AOT-published, so only the Api has the property.

### Reachability, per warning family

Option A owes an argument for each family. Here it is, honestly graded.

**`Newtonsoft.Json` — 132 warnings. Argued sound. — REFUTED, see the amendment.**
Reached only through `Avro.Schema.Parse`, called from the `ProductCreatedAvro._SCHEMA` static
initialiser. The only JSON this application ever hands to Newtonsoft is the schema literal compiled into
`ProductCreatedAvro.cs`: one flat record, four primitive fields, no nested records, unions, enums or
`fixed`. The flagged code — `DefaultContractResolver`, `ReflectionUtils`, `JObject`'s
`ICustomTypeDescriptor`, the `Microsoft.CSharp.RuntimeBinder` `dynamic` bridge, `FSharpUtils` — belongs
to `JsonConvert.(De)SerializeObject` over arbitrary CLR types, which nothing here calls. The
application's own HTTP JSON never goes near it: that is `System.Text.Json` with a source-generated
context and `JsonSerializerIsReflectionEnabledByDefault=false`.
*Failure mode if the argument is wrong:* the first touch of `_SCHEMA` throws, i.e. on the first
`POST /products`. Loud and immediate, never silent corruption.

**`Confluent.SchemaRegistry` — 5 warnings. Two sub-families, both argued sound.**
`SpecificSerializerImpl<T>.ExtractSchemaData` → `Activator.CreateInstance` (IL2067) is *exactly* the
reflection that `[DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(ProductCreatedAvro))]` on
`AddKafkaProducer` roots; `T` is one closed type in this application, so the warning is false here.
`Utils.TransformEnumerableAsync` / `TransformDictionaryAsync` → `MakeGenericType` (IL3050) +
`GetInterfaces` (IL2075) run the schema-*references* and field-rule transform. `product-created.avsc` is
a single self-contained record with neither references nor rules, so that path is never entered.

**`Confluent.Kafka` — 2 warnings. The residual risk. Not closed by static argument. — CONFIRMED as a real defect, now rooted; see the amendment.**
`Librdkafka.SetDelegates` → `GetRuntimeMethods` (IL2070) and `Marshal.PtrToStructure<T>` (IL3050) are the
P/Invoke binding to `librdkafka.so`. The `NativeMethods` type reaches `SetDelegates` as a `typeof(...)`
literal, so ILC roots the *type* — but `GetRuntimeMethods()` enumerating a type whose unused methods were
trimmed can still come back short, and this argument does not rule that out. **This is the one family a
reader should not take on faith.** It fails at the first broker interaction, which the produce smoke
below settles in one request.

### What the boot smoke proves

Running the published native binary with one deliberately invalid key makes it fail at
`ValidateOnStart` and exit, so it needs no broker and no background process:

```bash
cd publish/api && env Kafka__BootstrapServers= ./DemoProducts.Api
# -> 2026-08-28 18:17:31 [1] ERROR oft.Extensions.Hosting.Internal.Host - Hosting failed to start
# -> Microsoft.Extensions.Options.OptionsValidationException: Kafka:BootstrapServers is required.
# -> exit 134
```

That single line is more informative than it looks. In the **native** image it proves:

- the binary starts — no ILC-induced startup failure;
- **Serilog's code-based configuration survives trimming.** The template, the `[1]` `ThreadId` enricher,
  the `ERROR` `ShortLevel` enricher and the 36-character `ShortContext`
  (`oft.Extensions.Hosting.Internal.Host`) all rendered. This is the direct payoff of configuring
  Serilog in code instead of `ReadFrom.Configuration`, whose assembly scanning would have written
  *nothing* here and thrown no error;
- the **source-generated configuration binder** bound the whole `Kafka` tree, environment-variable
  override included;
- the DI graph builds, `AddKafkaProducer` and its `[DynamicDependency]` included;
- `KafkaOptionsValidator` runs and names the offending key.

It does **not** prove the produce path. `_SCHEMA` initialises lazily on the first serialize, and
`KafkaConnection` is constructed on the first request, so neither the `Newtonsoft.Json` nor the
`Confluent.Kafka` family is exercised by a boot that never serves a request.

### The one smoke that was owed — now run

```bash
docker compose up -d
dotnet run --project src/DemoProducts.Consumer          # terminal 1
./publish/api/DemoProducts.Api                          # terminal 2 — the NATIVE binary, not dotnet run
curl -X POST http://localhost:5080/products -H 'Content-Type: application/json' -d '{"name":"Notebook"}'
```

A `201` plus the `ProductCreated consumed.` line in terminal 1 closes all three families at once: it
forces `_SCHEMA` through `Newtonsoft.Json`, `SpecificSerializerImpl<T>` through the rooted
`Activator.CreateInstance`, and `Librdkafka.SetDelegates` through a real broker handshake. Until someone
runs it against the native binary, the `Confluent.Kafka` family stays argued rather than proven.

## Consequences

- **The mandatory AOT requirement is attested by three gates: build, publish and boot.** The produce
  smoke against the native binary is the fourth and has not been run. Do not read a green `dotnet build`
  as an AOT verdict.
- `TrimmerSingleWarn=false` stays set; it is what makes the 139 diagnostics name their methods instead of
  collapsing into one `IL2104` per assembly. Demoting them to warnings without it would have produced
  three unreadable lines instead of an auditable list.
- **The 139 warnings are a standing debt, not a resolved issue.** They should shrink to zero on their own
  when `Apache.Avro` and `Confluent.*` ship `net10.0` assets with trim annotations; revisit this ADR then
  and delete `IlcTreatWarningsAsErrors` rather than carrying it forward out of habit.
- Any *new* ILC warning is now silent, since it no longer fails the publish. The check is the count: 139
  from three known third-party assemblies and **0 from `DemoProducts.*`**. A diagnostic originating in
  `DemoProducts.*` means this ADR's premise no longer holds and must be amended, not tolerated.
- The Consumer stays off AOT entirely, which is what makes the producer/consumer DI split worth keeping.

---

## Amendment: the produce smoke was run, and it failed

The smoke above was run against the native binary, a real broker and a real Schema Registry. It returned
`500`, then — once that was fixed — `502`. Both were genuine trimming defects. The **build**, **publish**,
**image** and **boot** gates were all green while both were present, which is the point worth carrying
away: *none of the four static gates can see them.*

### Defect 1 — `Confluent.Kafka`: the residual risk was real

```
System.InvalidOperationException: Sequence contains no matching element
   at System.Linq.Enumerable.Single[TSource](IEnumerable`1, Func`2)
   at Confluent.Kafka.Impl.Librdkafka.SetDelegates(Type)
   at Confluent.Kafka.Impl.Librdkafka.LoadLinuxDelegates(String)
   at Confluent.Kafka.Producer`2..ctor(ProducerBuilder`2)
```

`SetDelegates` binds every librdkafka entry point with
`nativeMethodsClass.GetRuntimeMethods().Single(m => m.Name == ...)`. ILC roots the `NativeMethods` *types*
— they reach `SetDelegates` as `typeof` literals, which is what the original grade relied on — but nothing
statically references their P/Invoke *members*, so `TrimMode=full` removes them and the first `Single(...)`
throws. The grade above said the `typeof` argument "does not rule that out". It was right to hedge.

**Fix:** three `[DynamicDependency]` roots on `AddKafkaProducer`, one per candidate class
(`NativeMethods`, `NativeMethods_Centos8`, `NativeMethods_Alpine` — `LoadLinuxDelegates` tries each in turn
to match the librdkafka build in the image). ILC resolves the type names and would raise `IL2036` on a typo.

### Defect 2 — `Newtonsoft.Json`: the reachability argument was wrong

```
Confluent.SchemaRegistry.SchemaRegistryException: Empty schema; error code: 42201
   at Confluent.SchemaRegistry.RestService.RegisterSchemaWithResponseAsync(...)
   at Confluent.SchemaRegistry.Serdes.SpecificSerializerImpl`1.Serialize(...)
```

The argument was: "the only JSON this application ever hands to Newtonsoft is the schema literal compiled
into `ProductCreatedAvro.cs`". False. `Confluent.SchemaRegistry` depends on `Newtonsoft.Json` **directly**
and serialises every REST DTO with it. Trimmed of their properties, those DTOs serialise to nothing.

Captured by putting a logging proxy between the binary and the registry — the registration body was
**2 bytes**:

```
>>> POST /subjects/product-created-value/versions?normalize=False
>>> body (2 bytes): b'{}'
<<< 42201 Empty schema
```

and after the fix, 446 bytes carrying the full Avro schema.

**Fix:** `<TrimmerRootAssembly Include="Confluent.SchemaRegistry" />` on the Api project. Deliberately
blunt: the DTO set is internal to the package and shifts between versions, so enumerating types would be a
list that silently rots.

### Both roots are load-bearing, and neither is a suppression

Verified by ablation on `Confluent.* 2.15.0`: remove the `[DynamicDependency]` roots and the `500` returns;
remove the `TrimmerRootAssembly` and the `502` returns. There are still zero `<NoWarn>` entries for an IL
code, zero `[UnconditionalSuppressMessage]` and zero `#pragma warning disable IL...` in `src/**`. A root
makes the reflection *find what it looks for*; a suppression only stops the compiler mentioning it.

### What this changes about the gates

The four original gates are necessary and jointly insufficient. **A green publish says the code is
analysable, not that the binary works.** Only an end-to-end request through the real dependencies exercises
the reflection that trimming broke — and here it took the real Schema Registry, not a stub, because the
symptom was a well-formed HTTP call with an empty body.

### Result after this amendment

| | Before | After |
| --- | --- | --- |
| ILC warnings | 139 | **43** (0 from `DemoProducts.*`) |
| `POST /products` on the native binary | `500`, then `502` | **`201`**, consumer logs `ProductCreated consumed.` |
| Schema registered by the native binary | never | yes — proven against an emptied registry |

The expectation recorded in *Consequences* — that the warnings "should shrink to zero on their own when
`Apache.Avro` and `Confluent.*` ship `net10.0` assets with trim annotations" — is **half right and worth
correcting**. 2.15.0 does ship `net10.0` assets, and the count did fall by 96. But that assembly contains
**no trim annotations whatsoever**: not one `[DynamicallyAccessedMembers]`, `[RequiresUnreferencedCode]` or
`[RequiresDynamicCode]`. Retargeting a TFM is not annotating for trimming, and the remaining 43 will not
clear themselves. Re-check the feed each time this is revisited rather than assuming either direction.
