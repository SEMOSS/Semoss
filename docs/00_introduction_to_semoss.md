# Introduction to SEMOSS

SEMOSS (Semantic Open Source Software) is an open source platform for building AI applications, running agents, connecting enterprise data, and creating analytical workflows. It combines a web experience with programmable APIs, a shared engine framework, and managed execution environments.

## What you can build

| Use case | Platform building blocks |
| --- | --- |
| AI assistants and agents | Model engines, persistent rooms, workspace agents, the SEMOSS harness, tools, and skills |
| Retrieval augmented generation | Document ingestion, vector engines, embedding models, and generation models |
| Data processing and analysis | Database engines, queries, frames, transformations, and Python/R execution |
| Applications and notebooks | Projects, frontend components, Python, model and data APIs, and reusable assets |
| Automated workflows | Automation projects, reactors, function engines, agent runs, and scheduling services |
| Shared enterprise resources | Engine and project permissions, authentication integration, usage logs, and audit services |

Capabilities depend on the models, engines, permissions, and runtime services configured in an installation. Connecting an engine makes that resource available through SEMOSS's APIs; it does not automatically grant every user access.

## How the repositories fit together

| Repository | Role |
| --- | --- |
| [Semoss](https://github.com/SEMOSS/Semoss) | Core Java runtime, Pixel and reactors, engines, agent harnesses, language integration, and runtime assets |
| [Monolith](https://github.com/SEMOSS/Monolith) | Tomcat web application, REST APIs, authentication and session integration, and streaming endpoints |
| [semoss-ui](https://github.com/SEMOSS/semoss-ui) | Frontend applications, workbenches, and shared client libraries |
| [SEMOSS-deployment](https://github.com/SEMOSS/SEMOSS-deployment) | Examples of complete Kubernetes deployments and infrastructure configuration |

Monolith embeds the core Java runtime as a dependency. Semoss and Monolith are separate source repositories, but they do not require separate network services between them.

## Engines, projects, and execution

An **engine** is a configured resource such as a database, model, vector store, storage provider, function, or guardrail. Engine interfaces provide common identity and lifecycle behavior; specialized interfaces define the operations each resource supports.

A **project** organizes application assets and access permissions. Project types include code applications, notebooks, automation, analytical insights, **WORKSPACE** agents, and **SKILL** packages. An agent's workspace defines its behavior; the project it edits can be a different project.

**Pixel** is the platform's command and query language. Java **reactors** implement Pixel operations. An **Insight** holds the current user's execution context, frames, variables, and runtime state. A persistent model **Room** holds conversation messages and options. A room and an insight can be associated, but they have different responsibilities and lifetimes.

See [engine abstractions](concepts/engine_abstraction.md), [projects](engines/project_engines.md), [Pixel](concepts/pixel_language.md), and [rooms and agent runs](agents/agent_runs.md).

## The SEMOSS harness, agents, and skills

The **SEMOSS harness** is the native execution loop behind `RunAgent(..., harnessType=["semoss"])`. It resolves the agent configuration, builds model context, calls a model, executes permitted tools, adds their results to the conversation, and continues until the turn finishes, needs user input, is cancelled, or reaches a limit.

These concepts are distinct:

| Concept | Meaning |
| --- | --- |
| **Harness** | The runtime that carries out an agent run; `semoss` is the default |
| **Agent / workspace** | Reusable instructions, model defaults, tools, attached skills, budgets, and delegation settings |
| **Skill** | A reusable project containing `SKILL.md` and optional references, scripts, or assets |
| **Tool / MCP integration** | An executable capability made available to the model |
| **Room** | Persistent conversation history and room options |
| **Run** | One tracked invocation, identified by a `runId`, with status, output, and pending actions |

Skills teach the agent how to perform a task. They do not themselves execute tools or grant access. At run time, attached skills are staged into the working directory, their summaries are advertised to the model, and the agent can load the relevant instructions with `LoadSkill`.

Runs have persisted lifecycle records and can pause for tool approvals. The native harness also supports bounded subagent delegation, configurable hooks, optional reflection, and context compaction. Alternative harnesses adapt external agent runtimes; their capabilities differ from the native harness.

Start with [agents, harnesses, and skills](agents/README.md), then read [the SEMOSS harness](agents/semoss_harness.md), [agent configuration](agents/agent_configuration.md), and [skill management](agents/skills/skills_doc.md).

## Running SEMOSS

- **Try the platform:** the [Semoss Docker Compose examples](../docker-compose-examples/README.md) run published images with PostgreSQL, with optional MinIO and Redis or ZooKeeper variants.
- **Develop the backend:** [Monolith's local Docker examples](https://github.com/SEMOSS/Monolith/tree/dev/local-docker-testing/local-docker-compose) build sibling Semoss and Monolith checkouts into a local application image.
- **Develop the frontend:** follow [semoss-ui's setup](https://github.com/SEMOSS/semoss-ui#readme).
- **Deploy a complete environment:** use [SEMOSS-deployment](https://github.com/SEMOSS/SEMOSS-deployment) and configure its dependencies for your infrastructure.

The current backend source targets Java 21 and Tomcat 11. See [Java developer onboarding](development_guides/java_developer_onboarding.md) for build order and verification.

## Next steps

Read the [backend architecture](01_backend_architecture_overview.md) for request flow, execution, and persistence. The [documentation index](README.md) links the detailed engine, Python, application, and operations guides.
