# Engine Abstraction in SEMOSS

An engine is a configured resource with identity, lifecycle, and an implementation appropriate to its catalog type. [IEngine](../../src/prerna/engine/api/IEngine.java) supplies the shared contract. Specialized interfaces define resource-specific operations; a database query, model call, and file transfer are not the same interface operation.

## Catalog types

| Type | Resource | Guide |
| --- | --- | --- |
| `DATABASE` | Relational, graph, RDF, and other supported data sources | [Database engines](../engines/database_engines.md) |
| `MODEL` | Generation, embedding, and other model services | [Model engines](../engines/model_engines.md) |
| `VECTOR` | Document ingestion and vector retrieval | [Vector engines](../engines/vector_engines.md) |
| `STORAGE` | Object storage and file-transfer services | [Storage engines](../engines/storage_engines.md) |
| `FUNCTION` | Callable functions and integrations | [Function engines](../engines/function_engines.md) |
| `GUARDRAIL` | Configured validation/guardrail services | [Engine implementations](../../src/prerna/engine/impl/) |
| `PROJECT` | Applications, agents, skills, and other project assets | [Project types](../engines/project_engines.md) |
| `VENV` | Virtual-environment category retained in the engine enum | [IEngine catalog definition](../../src/prerna/engine/api/IEngine.java) |

## Lifecycle and configuration

`IEngine` provides identity (`getEngineId`, `getEngineName`), initialization from `.smss` properties, configuration access, catalog typing, and resource cleanup. Engine implementations can hold connections, clients, files, or language runtime state.

A resource's `.smss` file identifies its implementation and connection/configuration properties. Do not assume every catalog type uses the same property names. Use the relevant creation reactor and a matching example rather than manually copying unrelated connection fields.

`Utility` and runtime registries resolve engines and projects as needed. `DIHelper` holds shared configuration and runtime lookup state. Loading an engine is separate from authorizing its use: reactors and service methods must perform the access checks required by their operation.

## Security and metadata

[SecurityEngineUtils](../../src/prerna/auth/utils/SecurityEngineUtils.java) and related utilities manage engine catalog metadata and user/group permissions. Projects have their own [SecurityProjectUtils](../../src/prerna/auth/utils/SecurityProjectUtils.java) path.

Internal system engines use [SystemEngineRegistry](../../src/prerna/util/SystemEngineRegistry.java); they should not be treated as ordinary user-created database engines. See [internal databases](../platform_services/internal_databases.md).

## Engines in agent runs

The native harness selects an accessible text-generation model engine for its conversation. Resource tools can expose operations on additional engines through configured MCP resources and generated room tools. Workbenches supply context and tools for the current engine; attached skills teach the model how to use them.

An agent, a model engine, and a skill are separate catalog concepts. An engine appearing in an instruction or skill does not grant access or automatically attach its tools. See [agents](../agents/README.md) and [agent configuration](../agents/agent_configuration.md).
