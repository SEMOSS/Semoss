# Agent Skills

A skill is a **Project of type `SKILL`**. There is no separate skill registry: the securitydb
`PROJECT` table is the catalog (with a `PROJECTMETA` row `tag = SKILL` as a secondary marker),
and the skill's content lives in the project's assets folder. This includes the built-in
**platform skills** (`agent-run`, `build-and-publish`, `database`, `file-uploads`, `model`,
`python`, `room`, `semoss-sdk`, `vector`), which ship as `project/platform__<id>` project folders and
are loaded at startup as global projects - their project id IS the old slug (e.g. `database`).

Two ways to see skills, answering different questions:

- **`MyProjects(type=["SKILL"])`** - the catalog listing. Skills come back as ordinary project
  rows with `project_type = "SKILL"`.
- **`ListSkills`** - reads the **physical skill files on disk** for a room, project, or the
  current insight. It does **not** read the database, so any skill files that were copied or
  added manually are picked up and returned.

Once you have a skill's project id, attach it to a workspace with **`AttachSkillToWorkspace`** /
**`DetachSkillFromWorkspace`**, or set a workspace's whole skill set with **`EditWorkspace`** -
see [Managing skills on a workspace](#managing-skills-on-a-workspace).

---

## Skill identity

The **SKILL.md frontmatter is the source of truth** for a skill's name and description. They are
read on demand wherever needed (GetWorkspace enrichment, staging, clone); nothing is mirrored
into a registry table.

- **Content location**: `<project>/version/assets/skill/SKILL.md` (written by `CreateSkill`).
  The shipped platform skill projects keep theirs under `version/assets/public/SKILL.md`; both
  locations are probed.
- **Name**: frontmatter `name`, falling back to the project display name, then the project id.
- **Slug**: the slugified name (lowercase, dashes). Used as the staged folder name under
  `.claude/skills/<slug>/` at agent run time.
- **Description**: frontmatter `description`; absent when the frontmatter has none.

Note: project-list views (`MyProjects`) do not carry the frontmatter description - fetch it
per-skill via `ListSkills(project=...)` or `GetWorkspace` when you need it.

---

## ListSkills

Lists the physical skill files discovered on disk under the conventional skill-host directories
(`.skills/`, `.agents/skills/`, `.claude/skills/`, and the `client/`, `java/`, `py/` variants),
deduplicated by name. Because it reads the filesystem rather than the database, manually copied
or added skill files are included.

The directory it scans is chosen by the inputs below; if neither `project` nor `roomId` is given
it falls back to the current insight's working directory.

### Parameters

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `project` | string | - | Project id whose assets folder to scan. Requires view access to the project. |
| `roomId` | string | - | Room id whose folder to scan. Must be a room you own. |
| `includeContent` | boolean | `false` | Adds a `content` key to each skill holding the `SKILL.md` body (everything after the frontmatter). |
| `includeAll` | boolean | `false` | Crawls each skill folder and adds a `files` array of every other file. Forces `includeContent` to `true`. |

**Notes**

- `project` and `roomId` are mutually exclusive - passing both is an error.
- When neither `project` nor `roomId` is given, the current insight is scanned.
- Setting `includeAll=true` always turns on `includeContent`, even if `includeContent=false` is passed.
- In the `files` array, the skill's own top-level `SKILL.md` is omitted (it is already in
  `content`), and genuinely empty directories are not represented.

### Examples

```
# project
ListSkills(project="b7787bb4-f543-44fa-bc4e-a2e5edf25171");

# room
ListSkills(roomId="a9059a32-c6ed-4495-9f09-3303022a35be");

# current insight
ListSkills();

# include the content after the frontmatter for each skill
ListSkills(project="b7787bb4-f543-44fa-bc4e-a2e5edf25171", includeContent=true);

# include every file and folder inside each skill folder (implies includeContent)
ListSkills(roomId="a9059a32-c6ed-4495-9f09-3303022a35be", includeAll=true);
```

### Response

Every skill is a map with `name`, `path`, `directory`, and `description`. `path` and `directory`
are relative to the scanned working directory. With `includeContent=true` each skill also
carries a `content` key; with `includeAll=true` each additionally carries a `files` array (every
file has its own `path` and `directory`, so the folder structure can be recreated).

```json
[
  {
    "name": "database",
    "path": ".claude/skills/database/SKILL.md",
    "directory": ".claude/skills/database",
    "description": "Use when writing code in an app that queries a relational or graph database on the platform...",
    "content": "# Database Engine\nQuery a database on the..."
  }
]
```

---

## Listing skill projects: MyProjects

`GetSkills` has been removed. List skills through the standard project catalog:

```
# every skill project you can view (platform skills are global, so always included)
MyProjects(type=["SKILL"]);
```

Each row is a normal `MyProjects` project row; skills are the ones with
`project_type = "SKILL"`. The platform skills have their old slug as the project id
(e.g. `project_id = "database"`).

---

## Creating and editing skills

- **`CreateSkill(skillContent=[...], name=[...], description=[...])`** - creates a SKILL-type
  project and writes `SKILL.md` into `version/assets/skill/`. `name`/`description` are required
  only when the frontmatter omits them (frontmatter wins). Returns
  `{skill_id, project_id, slug, name}` (skill_id == project_id).
- **`UpdateSkill(skillId=[...], skillContent=[...], description=[...])`** - rewrites the
  SKILL.md body and/or description. The name is immutable. At least one of
  `skillContent`/`description` is required. Requires edit permission on the skill project.
- **`CloneSkill(skillId=[...], name=[...])`** - copies a skill (including a platform skill) into
  a new skill-project owned by the caller. Returns
  `{skill_id, project_id, name, slug, source_skill_id}`.
- **`DeleteSkill(skillId=[...])`** - detaches the skill from every workspace (WORKSPACE_RESOURCE
  rows + `CONFIG_JSON.skills[]` mirrors) and deletes the underlying project. Owner only.
  **Built-in platform skills cannot be deleted** (they reload from the distribution at boot);
  `DeleteProject` enforces the same guard.

---

## Managing skills on a workspace

There are two ways to change which skills a workspace uses:

- **Incremental** - `AttachSkillToWorkspace` / `DetachSkillFromWorkspace` add or remove **one**
  skill at a time.
- **Bulk** - `EditWorkspace` sets the **whole** configuration (name, MCPs, skills) at once.

Every skill is identified by its **project id** (`skillId`). Attachment is stored as a
`WORKSPACE_RESOURCE__` row (`RESOURCE_TYPE='SKILL'`), mirrored into `CONFIG_JSON.skills[]` as
`{"skill_id": "..."}`, and recorded in `PROJECTDEPENDENCIES`.

All of these reactors require **edit** permission on the workspace.

> **Legacy note**: `CONFIG_JSON.platform_skills[]` (slug arrays from when platform skills were
> disk-backed built-ins) is no longer honored anywhere and is stripped the next time the
> workspace config is rewritten. Re-attach platform skills by project id, e.g.
> `AttachSkillToWorkspace(workspaceId=[...], skillId=["database"])`. The `slug` input and the
> `platformSkills` EditWorkspace key are gone, as is `room.options.platform_skills[]`.

---

## AttachSkillToWorkspace

Attaches a single skill to a workspace and keeps `WORKSPACE.CONFIG_JSON` in sync. Idempotent
(re-attaching the same skill is a no-op).

### Parameters

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `workspaceId` | string | - | Target workspace. **Required.** |
| `skillId` | string | - | Skill project to attach. **Required.** Requires view access to the skill. |

### Examples

```
# user skill (by project id)
AttachSkillToWorkspace(workspaceId=["0146f913-2ae3-4f6b-8b04-0e7a53a36145"], skillId=["019e4687-e52c-718a-8460-27cb795896ac"]);

# platform skill (its project id is the old slug)
AttachSkillToWorkspace(workspaceId=["0146f913-2ae3-4f6b-8b04-0e7a53a36145"], skillId=["database"]);
```

### Response

```json
{ "workspace_resource_id": "0d3...", "workspace_id": "0146f913-...", "skill_id": "database", "created": true }
```

---

## DetachSkillFromWorkspace

Removes a single skill from a workspace: deletes the `WORKSPACE_RESOURCE__` row, the
`CONFIG_JSON.skills[]` mirror entry, and the `PROJECTDEPENDENCIES` entry. No-op when the skill
is not attached. Does **not** delete the skill itself (use `DeleteSkill`).

### Parameters

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `workspaceId` | string | - | Target workspace. **Required.** |
| `skillId` | string | - | Skill project to detach. **Required.** |

### Example

```
DetachSkillFromWorkspace(workspaceId=["0146f913-2ae3-4f6b-8b04-0e7a53a36145"], skillId=["database"]);
```

---

## EditWorkspace

Sets a workspace's configuration in **bulk** - name, description, system prompt, active state,
the full MCP list, and the full skill list. Use it to declare the complete state rather than
nudge one item.

### Parameters

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `workspaceId` | string | - | Target workspace. **Required.** |
| `name` | string | - | Display name. **Required.** |
| `description` | string | - | Workspace description. |
| `systemPrompt` | string | - | Workspace system prompt (mirrored into `CONFIG_JSON.system_prompt`). |
| `isActive` | boolean | `true` | Active/inactive state (owner only to change). |
| `mcp` | list of maps | - | Full MCP set. Each entry is `{id, name, type}`; `type` is the catalog type (`PROJECT` for project/MCP tools, or an engine type such as `MODEL`). |
| `skills` | list of strings | - | Full skill set, as a flat list of skill project ids. |
| `modelId` | string | - | Default model engine (`CONFIG_JSON.model_id`). Omit to leave unchanged; pass blank to clear. |

### Example

```
EditWorkspace(
  workspaceId=["0146f913-2ae3-4f6b-8b04-0e7a53a36145"],
  name=["Ryan's Agent47"],
  description=["Builds React/TypeScript apps"],
  systemPrompt=["You are Agent 47..."],
  isActive=[true],
  mcp=[
    {"id":"ce722163-2a8c-4667-b504-ce8732d77123","name":"NodeBuilderMCP","type":"PROJECT"},
    {"id":"394404bf-02e5-44b2-bc7c-e93d9b698f58","name":"Database Maker","type":"PROJECT"}
  ],
  skills=["019e4687-e52c-718a-8460-27cb795896ac", "database", "model"]
);
```

**Notes**

- `skills` and `mcp` are **full replaces** - the resulting set is exactly what you pass.
  `skills=[]` removes every skill.
- Platform skills go in `skills` like any other skill, identified by project id.
- Any legacy `CONFIG_JSON.platform_skills[]` key is removed on write.

---

## Reading back what is attached: GetWorkspace

`GetWorkspace(workspaceId=["..."])` returns the workspace, including a `skills[]` array. Every
entry has `type = "SKILL"`; `name`, `slug`, and `description` are resolved from the skill
project's SKILL.md frontmatter (`description` is omitted when the frontmatter has none; `name`
falls back to the project display name for stale attachments so they stay detachable):

```json
"skills": [
  { "id": "019e4687-...", "type": "SKILL", "name": "csv-cleaner", "slug": "csv-cleaner", "description": "..." },
  { "id": "database", "type": "SKILL", "name": "database", "slug": "database", "description": "Use when writing code..." }
]
```

The full `config_json` is returned alongside, so you can read `skills[]` directly if you prefer
the raw form.

---

## Run-time staging

At agent run time, `SkillStager` copies each attached skill's content folder into
`<workingDir>/.claude/skills/<slug>/`. The slug is derived from the frontmatter name; when two
attached skills resolve to the same slug, the first one staged wins and the duplicate is skipped
with a warning. A `.skill-meta` sidecar caches the source fingerprint so unchanged skills are
not re-copied.
