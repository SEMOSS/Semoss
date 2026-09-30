# Agent Skills

A skill is a **project of type `SKILL`** containing `SKILL.md` and optional reference files, scripts, and assets. The security database's project catalog supplies its identity and access controls. Skill contents live in project assets; there is no separate skill registry table.

Skills provide reusable instructions. They do not execute automatically, grant permissions, or replace tool schemas. The [SEMOSS harness](../semoss_harness.md) advertises discovered skills and lets the model load the relevant material with `LoadSkill`.

## Identity and content layout

New skills use this structure beneath their project assets folder:

```text
version/assets/
└── public/
    ├── SKILL.md
    ├── references/
    │   └── workflow.md
    ├── scripts/
    └── assets/
```

The concrete project layout can contain an `app_root` prefix; resolve it through project asset utilities rather than constructing host paths. [CreateSkillReactor](../../../src/prerna/reactor/agent/skill/CreateSkillReactor.java) writes `public/SKILL.md`. The older `assets/skill/` location remains a compatibility fallback, not the location for new content.

Frontmatter supplies the skill's name and description. The staged slug is derived from the name. When resolving older projects, the name can fall back to the project display name and then the project ID. A missing description is not invented from catalog metadata.

```markdown
---
name: dataset-review
description: Use when profiling a dataset and explaining data quality findings.
---

# Dataset review

Inspect the available schema and establish the population being analyzed.
Use the configured database tools for queries and report the query scope.
Read references/workflow.md for the detailed review procedure.
```

Keep the main file focused on when to use the skill, the workflow, expected outputs, and validation. Put substantial reference material in linked files and load it as needed. Scripts and reference files should use paths relative to the skill package.

## Catalog, content, and staged discovery

These operations answer different questions:

| Operation | What it reads |
| --- | --- |
| `MyProjects(type=["SKILL"])` | Skill projects the user can view in the catalog |
| `ListSkillFiles(project=["<skill-id>"])` | Files in one skill project's content folder |
| `ReadSkillFile(project=["<skill-id>"], filePath=["SKILL.md"])` | One file from that skill package |
| `GetWorkspace(workspaceId=["<workspace-id>"])` | Agent attachments and enriched skill identity |
| `ListSkills(...)` | Physical skill folders discovered under a room/project/insight working directory |
| Native tool `ListSkill` | The current run's discovered skill summaries |
| Native tool `LoadSkill` | A staged skill's instructions or supporting file |

`ListSkills` is not a way to read a skill project's own `public/SKILL.md`; use `ListSkillFiles` and `ReadSkillFile` for that. `MyProjects` does not include the frontmatter body or guarantee the skill has been staged into any room. The old `GetSkills` catalog API has been removed.

## Create, update, clone, and delete

| Reactor | Inputs and behavior |
| --- | --- |
| `CreateSkill` | `skillContent` required. `name` and `description` supply metadata when absent from frontmatter. Creates a SKILL project and `public/SKILL.md`. |
| `UpdateSkill` | `skillId` plus `skillContent` and/or `description`. Requires edit access; the name is immutable. |
| `CloneSkill` | `skillId`, optional new `name`. Copies content into a new skill project owned by the caller. |
| `DeleteSkill` | `skillId`. Owner-only; removes workspace references and the project. Built-in platform skills cannot be deleted through this path. |

Creation returns `skill_id`, `project_id`, `slug`, and `name`; the two IDs refer to the same project. Cloning also identifies the source skill. See the [skill reactors](../../../src/prerna/reactor/agent/skill/) for exact validation and response fields.

To inspect an existing package before attaching it:

```pixel
MyProjects(type=["SKILL"]);
ListSkillFiles(project=["<skill-id>"]);
ReadSkillFile(project=["<skill-id>"], filePath=["SKILL.md"]);
ReadSkillFile(project=["<skill-id>"], filePath=["references/workflow.md"]);
```

Read operations enforce project access. View-only users read public assets; access to legacy non-public content can require edit permission. Supporting files can be managed through the project's normal asset operations.

## Managing skills on a workspace

Attach or detach one skill without replacing the agent's other attachments:

```pixel
AttachSkillToWorkspace(workspaceId=["<workspace-id>"], skillId=["database"]);
DetachSkillFromWorkspace(workspaceId=["<workspace-id>"], skillId=["database"]);
GetWorkspace(workspaceId=["<workspace-id>"]);
```

The caller needs workspace edit access and view access to a skill being attached. Attachment is idempotent and uses the skill's **project ID**. It is stored in `WORKSPACE_RESOURCE` with `RESOURCE_TYPE='SKILL'`, mirrored into `CONFIG_JSON.skills` as `{"skill_id":"..."}`, and recorded in project dependencies.

`EditWorkspace` can replace the entire skill set through `skills=["<id>", "database"]`. Both `skills` and `mcp` are full replacement lists; preserve the complete intended configuration when editing. See [agent configuration](../agent_configuration.md#update-configuration).

Detaching does not delete the source skill. Legacy `platform_skills` and `platformSkills` forms are no longer the attachment mechanism; platform skills use project IDs just like user-created skills.

`GetWorkspace` enriches attached skills with `type="SKILL"`, name, slug, and description when available, alongside the configuration JSON. Use it to check catalog attachments before inspecting the staged files.

## ListSkills

`ListSkills` scans a selected working directory for conventional skill folders. It reads files rather than catalog rows, so manually copied skills can also appear.

| Key | Default | Behavior |
| --- | --- | --- |
| `project` | Unset | Scan that project's assets; requires view access |
| `roomId` | Unset | Scan the caller-owned room's folder |
| `includeContent` | `false` | Include the `SKILL.md` body after frontmatter |
| `includeAll` | `false` | Include supporting file contents as well; implies `includeContent` |

`project` and `roomId` are mutually exclusive. With neither, the reactor scans the current insight folder. For a run targeting a project or subdirectory, scanning the room folder is not necessarily scanning the run's actual working target; the native tools use that target directly.

```pixel
ListSkills(project=["<target-project-id>"]);
ListSkills(roomId=["<room-id>"], includeContent=[true]);
ListSkills(includeAll=[true]);
```

Discovery checks base directories in this order: the working root, `client/`, `java/`, and `py/`. Under each, it checks `.skills/`, `.agents/skills/`, `.agents/skill/`, `.claude/skills/`, and `.claude/skill/`. The first matching folder name wins. Plural paths and singular compatibility aliases are both recognized.

Results contain `name`, `path`, `directory`, and `description`. Paths are relative to the scanned root. Optional `content` contains the main body; optional `files` contains supporting file records. Empty directories are not represented. See [SkillScanner](../../../src/prerna/reactor/agent/skill/SkillScanner.java).

## Runtime staging and loading

[AgentConfigLoader](../../../src/prerna/reactor/agent/config/AgentConfigLoader.java) merges attached skill IDs from workspace resources, configuration JSON, and room additions. [SkillStager](../../../src/prerna/reactor/agent/skill/SkillStager.java) then resolves each package and copies its content to:

```text
<working-directory>/.claude/skills/<slug>/
```

The first attached skill resolving to a slug wins; duplicates are skipped with a warning. A `.skill-meta` sidecar records source identity and a modification-time fingerprint. On a detected source change, staging replaces the existing copy. Treat the project package as the source; edits to a staged copy can be overwritten.

Staging is best effort: failures are logged and do not automatically fail the run. Detachment is not a filesystem cleanup operation, and the scanner can still discover copies already present in a target. Check both attachments and physical files when investigating stale skill behavior. Although configuration references can carry `pinned_version`, the current stager resolves the available project content; do not assume it checks out a historical version.

The harness advertises discovered skill summaries. The native tool calls use the discovered folder name:

```json
{"skill_name":"dataset-review"}
```

The same `LoadSkill` tool reads a reference by appending its package-relative path:

```json
{"skill_name":"dataset-review/references/workflow.md","offset":0,"max_bytes":8192}
```

`LoadSkill` defaults to an 8 KiB chunk and reports when more content remains. Continue at the returned offset when a file is truncated. Loading a script as text does not execute it.

## Platform skills

Platform skills ship as `project/platform__<id>` assets and are cataloged as global SKILL projects. Their project IDs are stable names such as `database`, `python`, and `agent-run`.

The current catalog includes skills for application bootstrap and data, building/publishing, database access, exports, file uploads, frontend design, functions, MCP, models, pagination, permissions, presentations, Python, rooms, storage, users, vectors, and workflow automation. [SystemDefaultEngines](../../../src/prerna/util/SystemDefaultEngines.java) is the authoritative list; [SystemAgentSeeder](../../../src/prerna/util/SystemAgentSeeder.java) assigns subsets to platform agents.

Deploy the project folders and descriptors with the backend. A platform skill in the distribution is not automatically attached to every agent. Clone a skill when you need an independently maintained variant.

## Verification

1. Confirm the skill appears in the project catalog and its files can be read.
2. Confirm the workspace attachment and effective skill ID.
3. Start a run against the intended target and inspect the discovered skill list.
4. Load the main instructions and a reference file; check chunk continuation when applicable.
5. Verify the resulting task output and actual tool results. A correctly staged package alone does not prove the model followed the workflow.

Return to [agents](../README.md), [agent configuration](../agent_configuration.md), or [the SEMOSS harness](../semoss_harness.md).
