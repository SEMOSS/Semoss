# SEMOSS Configuration and Environment

Configuration spans the web application, the core runtime, individual engines/projects, and persistent agent settings. Use the files and startup scripts shipped with the deployed image as the source of truth; a source checkout's local properties can contain machine-specific paths.

## Configuration layers

| Layer | Purpose | Source/reference |
| --- | --- | --- |
| Monolith `web.xml` | Servlet mappings, filters, listeners, and paths into SEMOSS home | [Monolith configuration](https://github.com/SEMOSS/Monolith#configuration) |
| `RDF_Map.prop` | Core runtime properties, resource discovery, language settings, and feature flags | [Core properties](../../RDF_Map.prop), [DIHelper](../../src/prerna/util/DIHelper.java) |
| `social.properties` | Login/identity-provider and related social configuration | [SocialPropertiesUtil](../../src/prerna/util/SocialPropertiesUtil.java) |
| `log4j2.xml` | Application logging configuration | [Logging configuration](../../log4j2.xml) |
| Engine/project `.smss` | Identity, implementation, and resource-specific configuration | [Engine abstraction](../concepts/engine_abstraction.md), [projects](../engines/project_engines.md) |
| Workspace `CONFIG_JSON` and resource rows | Agent instructions, tools, skills, model default, budgets, and hooks | [Agent configuration](../agents/agent_configuration.md) |
| Room options | Conversation-specific agent selection, instructions, and additional resources | [Harness prompt composition](../agents/semoss_harness.md#prompt-composition) |
| Local Docker stack | Images, ports, networks, volumes, and service connections | [Local Docker configuration](../deployment/docker_configuration.md) |
| Container startup properties | semoss-artifacts property configuration and environment mappings | [SEMOSS-deployment](https://github.com/SEMOSS/SEMOSS-deployment) |

There is no generic promise that every Java property can be set by inventing a similarly named environment variable. Confirm mappings in the deployment scripts. A root `config.properties` credential file is not a required general setup step.

## SEMOSS home and runtime assets

The standard examples use `/opt/semosshome` inside the application container. It holds core configuration and resource directories such as `db/`, `project/`, `model/`, `vector/`, `storage/`, `function/`, `guardrail/`, and room files. Language assets live in `py/`, `R/`, and `js/` where packaged.

Projects resolve their assets through project utilities. New skill contents are under a SKILL project's `version/assets/public/`, with an installation-dependent project root prefix. Agent runs resolve a separate working target and stage attached skills there. Do not infer the target from the agent workspace ID.

Local development images can reuse assets from the published base. A Java rebuild does not necessarily replace every project or language file. See [local backend development](java_developer_onboarding.md#run-locally).

## Feature groups

- **System databases:** configure the appropriate system engine connections. The Compose examples use `CUSTOM_*` environment groups and PostgreSQL initialization SQL.
- **Persistent AI services:** enable and initialize the model-inference database for rooms, workspaces, run records, and approvals. `MODEL_INFERENCE_LOGS_ENABLED` is relevant to this storage, not only usage reporting.
- **Language workers:** the examples enable Python through `NETTY_PYTHON` and `NATIVE_PY_SERVER`, configure `SMSS_PYTHONHOME`, and disable R with `R_ON=false`.
- **Native agent tools:** workspace default-tool policy and deployment settings determine which built-in tools are offered. Node execution is optional; consult [js/README.md](../../js/README.md).
- **Authentication:** configure the intended provider and browser-facing redirect/cookie behavior. Local Compose files enable native registration for development.
- **Cloud storage and synchronization:** object storage, Redis/ZooKeeper asset synchronization, and Redis room/agent coordination have separate settings. See [cluster architecture](../cloud_and_cluster/README.md).
- **Process sandboxing:** [AgentSandboxConfig](../../src/prerna/reactor/agent/sandbox/AgentSandboxConfig.java) defines agent sandbox defaults. Enforcement is configuration- and runtime-dependent; it should not be inferred solely from a target path or skill attachment.

## Build environment

The current core and Monolith POMs target Java 21; Monolith targets Tomcat 11 and Jakarta Servlet 6.1. CI uses Maven 3.9.9. Semoss creates core JARs and Monolith creates the WAR. Follow [developer onboarding](java_developer_onboarding.md) for build order, matching `ci.version`, and tests.

## Diagnosing configuration problems

Start with the selected Compose/deployment file, the active image tag, startup logs, and the configuration actually loaded inside the running application. Confirm system database startup before diagnosing workspace or room failures. When agent behavior differs, inspect the selected workspace, room options, target directory, tool policy, and staged skills separately.

See [local Docker configuration](../deployment/docker_configuration.md), [internal databases](../platform_services/internal_databases.md), and [agent troubleshooting](../agents/agent_runs.md#troubleshooting). For Kubernetes and semoss-artifacts property configuration, use [SEMOSS-deployment](https://github.com/SEMOSS/SEMOSS-deployment).
