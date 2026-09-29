# Projects, Agents, and Skill Packages

Projects organize SEMOSS application assets, metadata, and access permissions. They share engine lifecycle behavior through [IProject](../../src/prerna/project/api/IProject.java) and the `PROJECT` catalog type, while [Project](../../src/prerna/project/impl/Project.java) provides the core implementation.

A project can represent a code application, notebook, automation, reusable agent, or skill package.

## Project types

The current `IProject.PROJECT_TYPE` values are:

| Type | Role |
| --- | --- |
| `BLOCKS` | Applications assembled from blocks |
| `CODE` | Applications and supporting code/assets |
| `INSIGHTS` | Analytical insights |
| `NOTEBOOK` | Notebook applications and assets |
| `AUTOMATION` | Workflow automation projects |
| `WORKSPACE` | Reusable agent identity and configuration |
| `SKILL` | Agent instruction packages and supporting files |

The enum is the source of truth. Older references to an `AppEngine` do not describe the current project implementation.

## Assets and access

Project helpers resolve the assets directory beneath the project's versioned root. The exact root can include `app_root`; use [AssetUtility](../../src/prerna/util/AssetUtility.java) and project methods rather than constructing machine paths.

[SecurityProjectUtils](../../src/prerna/auth/utils/SecurityProjectUtils.java) manages project access and dependencies. Public asset paths, editable assets, sharing, and project membership are separate from the external engine connections an application may use.

## Agent workspaces

An agent's project type is `WORKSPACE`. Its project ID is also the workspace identifier used by agent APIs. The model-inference database stores instructions and configuration in `WORKSPACE`, attachments in `WORKSPACE_RESOURCE`, and a configuration representation in `CONFIG_JSON`.

The agent workspace describes behavior and resources; `RunAgent.space` chooses the room/user/project target where work happens. A code agent can be reused against several editable application projects without changing its identity.

See [agent configuration](../agents/agent_configuration.md), [run lifecycle](../agents/agent_runs.md), and [workbench defaults](../agents/workbench-default-agents.md).

## Skill projects

A `SKILL` project stores `public/SKILL.md` and optional references, scripts, and assets beneath its assets directory. The project catalog and project permissions apply to skills. Attached skills are copied into an agent run's working directory for discovery and loading.

Use `MyProjects(type=["SKILL"])` for the catalog, `ListSkillFiles`/`ReadSkillFile` for a skill package's content, and `ListSkills` for staged-file discovery. See [the skill guide](../agents/skills/skills_doc.md).

## Platform project seeding

[ProjectWatcher](../../src/prerna/util/ProjectWatcher.java) catalogs packaged platform projects. [SystemDefaultEngines](../../src/prerna/util/SystemDefaultEngines.java) identifies built-in applications, MCP resources, skills, and agents. [SystemAgentSeeder](../../src/prerna/util/SystemAgentSeeder.java) creates or reconciles the workspace configuration for system agents after their project catalog entries exist.

Deploy descriptors and project assets together with matching backend code. A catalog entry alone is insufficient when the corresponding assets or required model-inference database are missing.

## Cloud storage

Project files can be pushed to and pulled from central storage through [ClusterUtil](../../src/prerna/cluster/util/ClusterUtil.java). Project/asset synchronization is distinct from room conversation persistence and live agent events. See [central storage](../cloud_and_cluster/central_cloud_storage.md).
