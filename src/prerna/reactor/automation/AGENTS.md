# Automation Engine — Agent Guide

Read `README.md` first for the execution model and DB schema.

## Adding a new node type

1. **Create the executor** in `nodes/` implementing `IAutomationNodeExecutor`:
   - Declare `private static final Logger classLogger = LogManager.getLogger(YourExecutor.class);`
   - Log execution at `DEBUG` level with node label and key params before dispatching
   - Use `AutomationExecutionUtils.resolve(value, scope, configMap)` for all `${var}` substitution
   - Throw `IllegalArgumentException` for missing required config fields
   - Use `AutomationExecutionUtils.GSON` — do not declare a local `Gson` instance

2. **Register it** in `AutomationNodeExecutors.EXECUTORS` map

3. **Add the type constant** to `AutomationConstants` (e.g. `NODE_TYPE_FOO = "foo-engine"`)

4. **Wire the FE** — add the node type to `automation.types.ts` and `automation.constants.ts` in `SemossWeb`

## Logging rules

Follow the platform standard — SLF4J `{}` placeholders, exception as the last argument:

```java
// ✅
classLogger.debug("Foo node \"{}\" executing operation={}", nodeLabel, operation);
classLogger.error("Foo node \"{}\" failed: {}", nodeLabel, e.getMessage(), e);

// ❌ — never concatenate strings in log calls
classLogger.error("Foo node " + nodeLabel + " failed: " + e.getMessage());
```

## Exception conventions

```java
// Missing/invalid user input — use IllegalArgumentException
throw new IllegalArgumentException("Foo node \"" + nodeLabel + "\": 'engineId' is required");

// User-facing reactor errors — use SemossPixelException
throw new SemossPixelException("Project does not exist or user does not have access");
```

## GSON

Use the shared instance — never declare your own:

```java
// ✅
AutomationExecutionUtils.GSON.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());

// ❌
private static final Gson GSON = new GsonBuilder().create();
```

## Reactor conventions

- Keep reactors thin: parse params, auth-check, delegate, return. Business logic belongs in `AutomationRunEngine`, `AutomationExecutionUtils`, or an executor.
- Always call `organizeKeys()` before reading `this.keyValue`
- Use `SecurityProjectUtils.testUserProjectIdForAlias` to resolve alias → UUID before any lookup
- Add `getReactorDescription()` to every reactor

## What not to change

- `AutomationDatabaseUtility` — DB access is intentional; use `setNullableString`, `SelectQueryStruct`, try-with-resources
- `PixelExecutionUtils` — timeout + ThreadStore propagation; do not bypass
- `CancelAutomationRunReactor` — dual-signal cancel (DB flag + in-memory); both signals are required for cluster safety
- `claimActiveRun` — PK-violation is the concurrency guard; do not add a separate lock
