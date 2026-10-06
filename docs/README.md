# SEMOSS Developer Documentation

Documentation for the SEMOSS core runtime, agents and harnesses, engine integrations, and deployment. Start with the [introduction](00_introduction_to_semoss.md) and [backend architecture](01_backend_architecture_overview.md), or choose the path below that matches your task.

## Getting started

| Task | Guide |
| --- | --- |
| Understand the platform and repositories | [Introduction to SEMOSS](00_introduction_to_semoss.md) |
| Follow a request through the backend | [Backend architecture](01_backend_architecture_overview.md) |
| Run the platform locally | [Semoss Docker examples](../docker-compose-examples/README.md) |
| Run local backend changes | [Java developer onboarding](development_guides/java_developer_onboarding.md) and [Monolith local Docker examples](https://github.com/SEMOSS/Monolith/tree/dev/local-docker-testing/local-docker-compose) |
| Develop the web experience | [semoss-ui](https://github.com/SEMOSS/semoss-ui#readme) |
| Configure Kubernetes or deployment properties | [SEMOSS-deployment](https://github.com/SEMOSS/SEMOSS-deployment) |

## Agents, harnesses, and skills

- [Agents overview](agents/README.md): agents, workspaces, rooms, runs, tools, and skills.
- [The SEMOSS harness](agents/semoss_harness.md): model/tool loop, prompt composition, budgets, compaction, hooks, and delegation.
- [Agent configuration](agents/agent_configuration.md): create a workspace, select models and execution targets, attach tools and skills, and configure limits.
- [Run lifecycle and streaming](agents/agent_runs.md): durable states, asynchronous submission, approvals, cancellation, event polling, and cluster boundaries.
- [MCP tools and `_meta` options](agents/mcp_tools.md): tool sources, execution modes, deferred loading, UI hints, and Pixel vs Python support.
- [Agent skills](agents/skills/skills_doc.md): project-backed skills, authoring, discovery, attachment, staging, and loading.
- [Workbench default agents](agents/workbench-default-agents.md): frontend defaults, saved conversations, and deployment of system agents.
- [PowerPoint visual inspection](agents/pptx-visual-inspection.md): the specialized presentation workflow and validation.
- [Claude Code integration](claude_code/claude_code.md): the alternate `claude_code` harness.
- [AskRoom](pixel/ask_room.md): persistent model turns with caller-managed tool continuation.

## Core concepts

| Concept | Guide |
| --- | --- |
| Pixel language | [Syntax and execution](concepts/pixel_language.md) |
| Reactors | [Reactor framework](concepts/reactor_framework.md) |
| Engines | [Engine abstraction](concepts/engine_abstraction.md) |
| Data and queries | [DataFrames and QueryStructs](concepts/data_frames_and_query_struct.md) |
| Execution context | [The Insight object](concepts/insight_object.md) |
| Projects, agents, and skill packages | [Project types](engines/project_engines.md) |

## Engine integrations

- [Database engines](engines/database_engines.md)
- [Model engines](engines/model_engines.md)
- [Vector engines](engines/vector_engines.md)
- [Storage engines](engines/storage_engines.md)
- [Function engines](engines/function_engines.md)
- [Projects](engines/project_engines.md)
- [Supporting engine Docker examples](../docker-compose-examples/engines/README.md)

## Platform services and web integration

- [Internal databases and agent persistence](platform_services/internal_databases.md)
- [Authentication and authorization](platform_services/authentication_and_authorization.md)
- [Java–Python communication](platform_services/java_python_communication.md)
- [Monolith integration and API request flow](integrations/monolith_interaction.md)
- [Monolith endpoints by application](https://github.com/SEMOSS/Monolith/blob/dev/docs/endpoints.md): `/api` engine routes, compatibility APIs, webhooks, health, tokens, and administrator setup.
- [Monolith webhooks and change notifications](integrations/monolith_webhooks.md): GitHub pushes, Microsoft mailbox/calendar subscriptions, callbacks, and delivery diagnostics.
- [Monolith README](https://github.com/SEMOSS/Monolith#readme)

## Development and how-to guides

- [Development guides](development_guides/README.md)
- [Configuration and environment](development_guides/configuration_and_environment.md)
- [Java developer onboarding](development_guides/java_developer_onboarding.md)
- [How-to index](how-to-guides/README.md)
- [Write a custom reactor](how-to-guides/writing_custom_reactors.md)
- [Interact with databases](how-to-guides/interacting_with_databases.md)
- [Work with frames](how-to-guides/working_with_dataframes.md)
- [Use the NounStore](how-to-guides/using_the_nounstore_effectively.md)
- [Advanced Pixel scripting](how-to-guides/advanced_pixel_scripting.md)
- [TypeSafe / Jev models](how-to-guides/using_typesafe_jev.md)

## Python integration

The [GenAI client index](python_genai_client/README.md) covers provider clients, embeddings, tokenization, model limits, and generation. See also [reasoning and effort](python_genai_client/reasoning_and_effort.md) and the provider pages for [OpenAI](python_genai_client/text_generation/openai.md), [Anthropic](python_genai_client/text_generation/anthropic.md), [Google](python_genai_client/text_generation/google.md), [Bedrock](python_genai_client/text_generation/bedrock.md), and [Text Generation Inference](python_genai_client/text_generation/textgen.md).

The [Python GAAS tools index](python_gaas_tools/README.md) documents Python access to SEMOSS models, databases, vectors, storage, functions, and server communication. These clients are integration components; the native Java agent harness is documented separately under [agents](agents/README.md).

## Deployment and operations

- [Local Docker setup](cloud_and_cluster/docker_deployment.md)
- [Local Docker configuration](deployment/docker_configuration.md)
- [Supporting engine examples](../docker-compose-examples/engines/README.md)
- [Cloud and cluster architecture](cloud_and_cluster/README.md)
- [Central asset storage](cloud_and_cluster/central_cloud_storage.md)
- [Redis and ZooKeeper synchronization](cloud_and_cluster/cluster_synchronization.md)
- [CI/CD workflows](cloud_and_cluster/github_actions_workflows.md)
- [Kubernetes, infrastructure, and semoss-artifacts properties](https://github.com/SEMOSS/SEMOSS-deployment)

## Reading and maintaining these docs

Use the source links and the configuration shipped with your image to resolve version-specific behavior. Pixel examples containing `<...>` are templates: replace the placeholders with accessible resources and run them in an authenticated SEMOSS context.

When changing a reactor, harness, project type, or deployment setting, update the relevant guide and this index. Keep examples aligned with the same revisions of Semoss, Monolith, semoss-ui, and the packaged project assets.
