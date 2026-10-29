# REST and Pixel operation contracts

`prerna.contract` describes wire contracts for documentation and SDK generation. Monolith consumes the same classes through its
existing SEMOSS dependency. Monolith exports a single OpenAPI artifact; semoss-ui consumes it to generate TypeScript.
The format also supports a future Python generator.

Two examples are implemented: Monolith's `HealthResource.readiness` and SEMOSS's
[`StopPixelExecutionReactor`](../../src/prerna/reactor/StopPixelExecutionReactor.java).
Existing reactors continue to use `AbstractReactor`; there is no catalog migration.

## REST methods

Keep JAX-RS in charge of HTTP routing and bindings. For a small response object,
declare its fields directly in `@Returns`. No response class is needed:

```java
@GET
@Path("/ready")
@Produces("application/json")
@Description("Reports whether backend startup has completed.")
@Returns(
    statuses = {200, 503},
    description = "HTTP 200 when startup is complete; HTTP 503 while the backend is not ready.",
    fields = {
        @Returns.Field(name = "status", type = String.class,
                       values = {"READY", "NOT_READY"}, description = "Current readiness state."),
        @Returns.Field(name = "startupComplete", type = boolean.class,
                       description = "Whether startup completed successfully.")
    }
)
public Response readiness() {
    boolean complete = DBLoader.isStartupComplete();
    Map<String, Object> status = new HashMap<>();
    status.put("status", complete ? "READY" : "NOT_READY");
    status.put("startupComplete", complete);
    return WebUtility.getResponseNoCache(status, complete ? 200 : 503);
}
```

The implementation keeps returning its existing map. Each inline field declares
its exact wire name, Java wire type, and description. Fields are required and
non-null by default; use `required = false` for an absent field or `nullable = true`
for an explicit null. `values` constrains strings to an enumerated set, and
`format` adds a wire-format hint. Field types may also reference records, arrays,
enums, or named `TypeRef` declarations for complex values.

`statuses` is shorthand for responses sharing a body and description. Use either
`status` or `statuses`. Repeat `@Returns` when response shapes or descriptions
differ. The reader still returns a separate response entry for every status.
The actual readiness endpoint returns
`READY/true` with HTTP 200 or `NOT_READY/false` with HTTP 503. An SDK generator
must retain the statuses, not turn every declared response into a success result.

For larger or reused payloads, `@Returns(value = SomeResponse.class, status = 200)`
still accepts a contract record or an existing wire-compatible DTO. Use a type or
inline fields in each declaration; combining them is rejected. Inline objects use
the same immutable, referenced type graph as records, with operation-scoped IDs.

`@Parameter` can describe an input directly on a Java parameter, or on the method
when the implementation obtains it from `HttpServletRequest`. Use `binding` for
supplemental inputs (`QUERY`, `PATH`, `HEADER`, `COOKIE`, `FORM`, `MATRIX`, or `BODY`). For
example, a JSON-valued form field can use
`@Parameter(name = "changes", type = Change[].class, binding = Parameter.Binding.FORM,
encoding = Parameter.Encoding.JSON)`.

Native JAX-RS names and locations win. Conflicting metadata is rejected. Explicit
types describe logical values before the declared encoding; a Java `String` may
therefore carry a JSON-encoded record. `@Context` arguments are not client inputs.
`@DefaultValue` is retained. Native query/form requiredness without explicit
metadata is unknown, not assumed optional. Path parameters must be required and
non-null. `@BeanParam` needs explicit flattened metadata and is reported as a gap.

The reader's path is relative to the owning JAX-RS application. `/health` comes
from application/servlet registration and is outside this reader's scope.

## Reactors using actual input/output records

[`AbstractReactor2<I, O>`](../../src/prerna/reactor/AbstractReactor2.java) binds Pixel
arguments to the actual input record and invokes `protected O execute(I input)`.
Annotate the components where their Java types are defined. The execution return
type supplies the output shape; no duplicate type/class/schema declaration is needed.
Keep small operation-specific records inside the reactor:

```java
@Reactor(name = "Echo", description = "Echoes an identifier.")
@Invocation(mode = Invocation.Mode.BOTH)
public class EchoReactor extends AbstractReactor2<EchoReactor.Input, String> {
    public record Input(
        @Key(description = "Identifier to echo.") String id
    ) {}

    @Override
    @Output(name = "message", description = "The echoed identifier.")
    protected String execute(Input input) {
        return input.id();
    }
}
```

Import annotations from `prerna.contract`. `@Reactor` supplies the callable name,
description and optional operation aliases. Names/aliases must match the existing
reactor registry; metadata does not register or rename operations. `@Key` supplies a component's description,
optional wire-name override (`name`), and aliases; its Java type defines its shape.
Keys are required and non-null by default. `@Output` names and describes the actual
return value. Its name is exported as the schema `title`, not a new payload property.
`StopPixelExecutionReactor` uses this pattern and still produces a string with the
existing cancellation messages, permission checks and `OPERATION` flag.

For complex data, return a record and actually construct it in execution:

```java
public record Item(@Description("Item identifier.") String id) {}
public record Page(
    @Description("Matching items.") List<Item> items,
    @Description("Total matches.") long total
) {}
```

A reactor returning `Page` extends `AbstractReactor2<Input, Page>` and implements
`protected Page execute(Input input)`. Lists, string-keyed maps, enums and nested
records can appear in actual input/output types. No separate documentation-only
record is required for these typed reactors.

### Enforcement

- Java checks the input record and return type against the generic base. Returning
  a map from `execute(Input)` declared to return `Page` is a compilation error.
- `ContractReader` requires a concrete typed execution declaration, `@Reactor`,
  descriptions, `@Key` on each input component, `@Output` and `@Invocation`. It
  rejects conflicting metadata, unresolved types and invalid invocation declarations
  before export, and when constructing an adopted reactor. This metadata validation
  is an export/startup check, **not an annotation processor in `javac`**.
- The final public `NounMetadata execute()` entry point binds and validates input
  values, invokes typed execution, converts the result to its wire representation,
  and validates output shape/nullability. Optional primitive inputs must be boxed.
  Numeric conversions reject fractional integers and overflow; strings are not
  implicitly coerced from numbers. Business correctness and description accuracy
  still need operation tests.
- Contract reads are cached per concrete class and do not initialize, instantiate
  or execute the reactor. Input record constructors run only during actual invocation.
- The output bridge retains scalar Pixel noun types. Structured/nullable values use
  the existing `JSON_OBJECT` serializer to preserve explicit null fields. Operation
  flags can be customized with `outputOperationTypes()` without bypassing typed output.

### Argument and field semantics

- `@Invocation` explicitly selects `NAMED`, `POSITIONAL`, or `BOTH`. For typed reactors,
  omitted positional `order` derives from record component order. An explicit order
  must list every key once. Reordering the record can change positional compatibility.
- Arrays/collections are list values. A key becomes variadic only through an explicit
  `variadic` declaration, and must be last in positional order.
- Named keys and aliases address one canonical value. Duplicate spellings in a call,
  unsupported argument modes and excess positional values are rejected. Mixed calls
  fill unbound keys from the positional row in declared order.
- `@Optional` means a key/field may be absent; its missing Java value is null.
  `@Nullable` separately permits an explicit null. Nullable does not imply optional.
  Optional non-null record fields whose value is null are omitted on output; nullable
  fields are emitted as JSON null. A plain Java reference cannot distinguish absent
  from explicit null when both are permitted.
- Input records must be public. The default binder supports JSON-compatible scalar,
  record, array, List/Set and Map values. Custom collection classes, DTO constructors,
  polymorphic input interfaces and internal frame/task/query objects need a dedicated
  binding adapter; the schema reader's ability to describe a type does not imply that
  the default runtime binder can construct it. Unsupported binding fails explicitly.
- `@Format` remains a descriptive wire hint, not an additional runtime constraint.
  Recursive schemas are supported; actual cyclic/deep values are rejected.

### Existing reactors and specialized bases

Legacy reactors keep their existing inheritance, execution, noun-store handling and
metadata hooks. They can still use class-level `@Description`, `@Parameter`, `@Returns`
and `@Invocation` with `ContractReader`. Existing `@ReactorMetadata` providers also
remain readable for compatibility. These are documentation contracts, not typed binding.
Do not combine them with a typed reactor's declarations.

Adopt the record execution boundary per reactor. Frame/task/query pipelines that
exchange internal objects need compatible runtime adapters before switching to this
base; this change does not pretend their wire result is their internal Java object.
The reader and OpenAPI model remain independent of the runtime inheritance hierarchy.
Apply `@Ignore`, `@Internal`, `@Deprecated` and `@Deprecation` on the reactor as before.

## Wire types

Use existing DTOs only when their fields match the serialized shape. Otherwise
write a contract record beside the owning domain; do not expose implementation
objects merely because reflection can inspect their fields.

```java
public record Result(
    @WireName("item_id") String id,
    @Optional String label,
    @Nullable String explanation,
    @OneOf({String.class, Long.class}) Object value,
    Map<String, @Dynamic(reason = "Provider-defined metadata") Object> metadata,
    @Format("date-time") String createdAt
) {}

public final class ResultPages extends TypeRef<Map<String, List<Result>>> {}
```

Shared top-level public types belong in their own files; operation-specific records may be nested in their reactor. `TypeRef<T>` supplies a class literal
for a generic root, for example `@Returns(ResultPages.class)` or
`.returns(ResultPages.class, "Results grouped by page.")`; it is never constructed.

| Declaration | Meaning |
| --- | --- |
| An ordinary record/DTO property | Required, non-null wire property |
| `@Optional` | Property may be absent |
| `@Nullable` | Value may explicitly be null, including at collection element positions |
| `@Optional @Nullable` | Property may be absent or null |
| `@OneOf` | Union of alternative shapes; schema views use `anyOf` |
| `@WireName` | Exact wire name, also usable on enum constants |
| `@Format` | Wire format hint; does not serialize Java dates automatically |
| `@Dynamic(reason = "...")` | Intentionally variable data with an explanation |
| `@Unverified(reason = "...")` | Known uncertainty, returned as a diagnostic |

Java reference types alone imply neither absence nor null. Gson's omission rules
must be reflected explicitly in the contract. Gson `@SerializedName` is honored;
a conflicting `@WireName` is rejected. Static, synthetic, and transient DTO fields
are excluded. Getters and custom serializers are not executed or inferred.

String-keyed maps, arrays, generic collections, inherited DTO fields, enums, and
recursive record graphs are supported. Unknown Java types, unresolved generic
variables, raw containers, or unsupported map keys remain explicit gaps. Annotate
dynamic values at the appropriate leaf so verified surrounding fields stay useful.
`void` describes no response body; it is different from JSON null.

## Inclusion, audience, and deprecation

All annotations have runtime retention and appear in Java documentation.

- `@Ignore(reason = "...")` excludes future SDK operations while retaining the
  reason. Apply it to REST methods/resource classes or reactor classes.
- `@Internal(reason = "...")` keeps an operation available with a visible caution
  for platform developers. Administrative permissions do not imply internal use.
- Java `@Deprecated` carries `since` and `forRemoval`; add
  `@Deprecation(reason = "...", replacement = "...")` for readable details.
  The supplemental annotation requires Java `@Deprecated`.
- Resource class policies, superclass policies, and method policies accumulate.
  A method cannot undo an owner's exclusion. Internal and deprecated can coexist.
- Unmarked operations remain included. Policies do not modify authorization,
  route/Pixel registration, or serialization. Future catalog builders must apply a
  reactor policy to its registered aliases and retain context-specific variants.

For example, an appropriate platform operation can carry
`@Internal(reason = "For platform lifecycle management; use with caution.")`.
An inbound provider callback can carry
`@Ignore(reason = "Inbound provider callback; clients should not invoke it.")`.
Neither policy is assigned to readiness or cancellation merely because those
operations concern platform state. The test fixtures demonstrate combining ignore,
internal, and deprecation independently.

Blank policy reasons, duplicate names/aliases, contradictory type annotations,
inconsistent invocation order, conflicting HTTP bindings, and duplicate response
status/media-type pairs are rejected through `ContractException` diagnostics.

## Reading a contract

```java
OperationContract pixel = ContractReader.read(StopPixelExecutionReactor.class);
OperationContract rest = ContractReader.read(
    HealthResource.class.getMethod("readiness"));
TypeContract type = ContractReader.readType(ResultPages.class);
```

For an inherited REST method, use `ContractReader.read(concreteResource, method)`.
The reader inspects only supplied elements. It does not scan registrations,
execute endpoints, construct reactors, or call legacy metadata methods.

Results use immutable records, stable ordering, and referenced type definitions.
Input order is normalized by name; positional order is retained separately.
Treat type reference IDs as opaque identities; inline object IDs do not name Java
classes. Fields and response statuses are normalized in stable order.
Missing operation descriptions, unknown types, unspecified response statuses, and unspecified requiredness produce
warnings. Contradictions throw `ContractException`; callers must not generate a
client from an invalid contract. `ContractSchemas` produces fresh JSON Schema
views for the new base's help/MCP metadata; it is not a catalog exporter.

TypeScript and future Python generators consume the backend OpenAPI document.
The Java wire graph is an internal backend representation. They own language-specific identifier escaping, class/interface names,
imports, and numeric representations. Preserve integer/decimal formats, null versus
absence, HTTP status/media distinctions, and Pixel argument conventions. A partial
contract must remain partial; it must not become an invented precise client type.

## Backend OpenAPI export

`OpenApiExporter` converts selected `OperationContract` values into OpenAPI 3.1.
It shares the wire-schema renderer with `ContractSchemas`, using stable local
`components.schemas` references instead of exposing the internal Java graph.
Monolith's `ExportOpenApi` owns the initial readiness/cancellation selection and
combines REST and Pixel metadata without constructing or executing operations.

After building/installing the matching SEMOSS artifact, run in Monolith:

```sh
mvn -Pcontracts -Dlicense.skip=true process-classes
```

The ignored output is `target/contracts/openapi.json`. Do not commit or publish
it, or copy it into semoss-ui. The frontend generator reads it directly with
`pnpm --filter @semoss/sdk generate --openapi /absolute/path/to/openapi.json`.
Only generated TypeScript and sync provenance/file ownership are checked in.
No Java or backend checkout is needed in the frontend generation environment.

REST routes, parameters and responses use standard OpenAPI fields. The application
path supplies the resource-group tag, while the selection provides a stable
`operationId`. Schemas preserve integer/decimal types, enums, wire names, required
fields, nullable values, maps, unions and recursive references. The export rejects
incomplete contracts and unsupported request bindings instead of inventing shapes.

`x-semoss-pixels` has `schemaVersion: 1` and a `reactors` object keyed by registered
name. Each reactor carries a description, JSON Schema `inputSchema` and
`outputSchema`, and `invocation` with `mode`, `order`, `variadic` (name or null), and
`canOmitParameters`. Inputs describe canonical names; property `x-semoss-aliases`
lists accepted aliases. SDK adapters normalize these aliases and serialize the
verified named/positional invocation. The output schema describes the value inside
the Pixel result, not the transport envelope. No synthetic REST route is created
for a reactor.

Operation extensions `x-semoss-ignore` and `x-semoss-internal` retain arrays of
policy reasons. Java deprecation becomes `deprecated: true` plus
`x-semoss-deprecation` (`since`, `forRemoval`, `reason`, `replacement`). Schema
annotations `x-contract-status` and `x-contract-reason` retain dynamic/unverified
knowledge, and `x-no-content` identifies an absent Pixel output. Incomplete or
unverified contracts fail before export. These extensions are interpreted by the
SEMOSS SDK adapter; general OpenAPI tools can consume the REST surface.

The initial frontend adapter supports JSON GET operations with scalar path/query
or repeated query parameters. Other verbs, bodies, response headers, remote
references and other unsupported schema features require an adapter before SDK
generation. Backend selection remains explicit; there is no full catalog scan.

## Verification

From SEMOSS, under JDK 21:

```bash
mvn -Dtest=ContractReaderUnitTests,OpenApiExporterUnitTests,AbstractReactor2UnitTests test
```

The tests cover metadata without operation initialization, policies, contradictory
declarations, generic and recursive schemas, optional/null values, stable results,
derived runtime keys, aliases, typed record binding, compiler rejection of incorrect result types, and the actual Pixel output serializer. If unrelated
test sources fail compilation, run the focused tests after compiling production
sources, without modifying the Maven build configuration:

```bash
mvn -Dlicense.skip=true compile dependency:build-classpath \
    -Dmdep.outputFile=target/contract-classpath.txt -DincludeScope=test
contract_classpath="target/classes:$(cat target/contract-classpath.txt)"
mkdir -p target/test-classes
javac --release 21 -proc:none -cp "$contract_classpath" -d target/test-classes \
    test/prerna/contract/ContractReaderUnitTests.java \
    test/prerna/contract/OpenApiExporterUnitTests.java \
    test/prerna/reactor/AbstractReactor2UnitTests.java
mvn -Dtest=ContractReaderUnitTests,OpenApiExporterUnitTests,AbstractReactor2UnitTests surefire:test
```

Build/install SEMOSS before compiling Monolith, with matching `ci.version` values
as described in [Java onboarding](java_developer_onboarding.md). From Monolith, run:

```bash
mvn -Dlicense.skip=true -DskipTests compile
```
