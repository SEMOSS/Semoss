# REST and Pixel operation contracts

`prerna.contract` describes wire contracts for documentation and future TypeScript
and Python client generators. Monolith consumes the same classes through its
existing SEMOSS dependency. No catalog exporter or SDK generator is included yet.

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

## Reactors using AbstractReactor2

[`AbstractReactor2`](../../src/prerna/reactor/AbstractReactor2.java) extends the
existing base and derives keys, required flags, descriptions, and input/output
schemas directly from annotations on the reactor class. Declare the contract
above the class and implement `execute()` as before. No metadata provider,
builder, constructor, or metadata method overrides are needed:

```java
@Description("Echoes an identifier.")
@Parameter(name = "id", type = String.class, required = true,
           description = "Identifier to echo.")
@Returns(value = String.class, description = "The identifier.")
@Invocation(mode = Invocation.Mode.BOTH, order = "id")
public class Echo2Reactor extends AbstractReactor2 {
    @Override
    public NounMetadata execute() {
        organizeKeys();
        return new NounMetadata(keyValue.get("id"), PixelDataType.CONST_STRING);
    }
}
```

Import the contract annotations from `prerna.contract`,
`AbstractReactor2` from `prerna.reactor`, and the usual Pixel noun classes.

`StopPixelExecutionReactor` follows this pattern. Its `id` key, required flag,
help strings, and schemas come from its class annotations. Its cancellation
implementation is unchanged. Reading an annotated reactor's contract does not
construct or initialize the reactor.

Use repeated `@Parameter` annotations for additional inputs. Set `aliases` for
alternate spellings and `nullable = true` when explicit null is valid. Apply
`@Ignore`, `@Internal`, `@Deprecated`, and `@Deprecation` directly to the class.
Policies describe client use and do not change authorization or MCP registration.

### Argument semantics

- An argument declares a logical wire value. An array type denotes a list value;
  clients still apply Pixel's noun-row/vector serialization rules.
- `@Parameter` declares aliases and nullability. Aliases are alternate
  spellings of one canonical input, not additional required inputs.
- `@Invocation` is required for `AbstractReactor2`; the base reports an actionable
  error if it is missing. `NAMED`, `POSITIONAL`, and `BOTH` describe verified
  invocation styles. Positional order must name every argument once. A nonempty
  variadic name must be last.
- `canOmitParameters` is independent of individual optional inputs. It defaults
  to false; asserting true while an input is required is rejected.
- The base preserves existing `organizeKeys()` and `checkOptional()` behavior.
  Type metadata adds no runtime type validation. Complex positional/variadic
  interpretation still belongs in the reactor's existing execution code.
- Output metadata describes the serialized Pixel output, not `NounMetadata` or a
  Java frame/task object. Use a wire record when serialization transforms the value.

Existing specialized bases are unchanged. A reactor that needs one should keep
it until that base explicitly adopts the contract mechanism. Class-level
`@Description`, repeated `@Parameter`/`@Returns`, and `@Invocation` are also readable
on legacy reactors without changing their inheritance. Only `AbstractReactor2`
automatically derives runtime keys and metadata hooks from those declarations.

### Reusing a programmatic definition

Existing `@ReactorMetadata(SomeProvider.class)` definitions remain supported when
a shared programmatic definition is useful. The provider implements
`ReactorDefinition.Provider` and returns an immutable definition using
`ReactorDefinition.builder(...)`. Its builder supports `.internal(reason)`,
`.ignore(reason)`, and `.deprecated(since, forRemoval, reason, replacement)`.

Use either a provider or direct description/parameter/return/invocation annotations.
Combining those two sources is rejected; class policy annotations may accompany
either style. Direct annotations are the recommended starting point.

Providers must be pure: no database access, services, environment-dependent
branches, clock reads, or registration changes. They have a public no-argument
constructor. Java initialization and provider code do execute; the reader is not
a sandbox for untrusted Java code. The enclosing reactor is not constructed or
initialized by reading a pure nested provider.

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

Each public type belongs in its own file. `TypeRef<T>` supplies a class literal
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

TypeScript and Python generators should consume the same wire identities and
type graph. They own language-specific identifier escaping, class/interface names,
imports, and numeric representations. Preserve integer/decimal formats, null versus
absence, HTTP status/media distinctions, and Pixel argument conventions. A partial
contract must remain partial; it must not become an invented precise client type.

## Verification

From SEMOSS, under JDK 21:

```bash
mvn -Dtest=ContractReaderUnitTests,AbstractReactor2UnitTests test
```

The tests cover metadata without operation initialization, policies, contradictory
declarations, generic and recursive schemas, optional/null values, stable results,
derived runtime keys, aliases, and the actual Pixel output serializer. If unrelated
test sources fail compilation, run the focused tests after compiling production
sources, without modifying the Maven build configuration:

```bash
mvn -Dlicense.skip=true compile dependency:build-classpath \
    -Dmdep.outputFile=target/contract-classpath.txt -DincludeScope=test
contract_classpath="target/classes:$(cat target/contract-classpath.txt)"
mkdir -p target/test-classes
javac --release 21 -proc:none -cp "$contract_classpath" -d target/test-classes \
    test/prerna/contract/ContractReaderUnitTests.java \
    test/prerna/reactor/AbstractReactor2UnitTests.java
mvn -Dtest=ContractReaderUnitTests,AbstractReactor2UnitTests surefire:test
```

Build/install SEMOSS before compiling Monolith, with matching `ci.version` values
as described in [Java onboarding](java_developer_onboarding.md). From Monolith, run:

```bash
mvn -Dlicense.skip=true -DskipTests compile
```
