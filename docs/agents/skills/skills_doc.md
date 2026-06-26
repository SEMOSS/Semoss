# Agent Skills

Two reactors surface the skills available on the platform. They answer different questions:

- **`ListSkills`** reads the **physical skill files on disk** for a room, project, or the current insight. It does **not** read the database, so any skill files that were copied or added manually are picked up and returned.
- **`GetSkills`** does a **database read** of the skills **registered as projects** on the platform, and also includes the **platform skills** (disk-backed built-ins).

Once you have a skill's identifier, attach it to a workspace with **`AttachSkillToWorkspace`** / **`DetachSkillFromWorkspace`**, or set a workspace's whole skill set with **`EditWorkspace`** - see [Managing skills on a workspace](#managing-skills-on-a-workspace).

---

## ListSkills

Lists the physical skill files discovered on disk under the conventional skill-host directories (`.skills/`, `.agents/skills/`, `.claude/skills/`, and the `client/`, `java/`, `py/` variants), deduplicated by name. Because it reads the filesystem rather than the database, manually copied or added skill files are included.

The directory it scans is chosen by the inputs below; if neither `project` nor `roomId` is given it falls back to the current insight's working directory.

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
- In the `files` array, the skill's own top-level `SKILL.md` is omitted (it is already in `content`), and genuinely empty directories are not represented.

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

Every skill is a map with `name`, `path`, `directory`, and `description`. `path` and `directory` are relative to the scanned working directory.

With `includeContent=true`, each skill also carries a `content` key:

```json
[
  {
    "name": "database",
    "path": ".claude/skills/database/SKILL.md",
    "directory": ".claude/skills/database",
    "description": "Use when writing code in an app that queries a relational or graph database on the platform, running SELECTs, inserts, updates, deletes, or fetching schema/table structure...",
    "content": "# Database Engine\nQuery a database on the..."
  },
  {
    "name": "file-uploads",
    "path": ".claude/skills/file-uploads/skill.md",
    "directory": ".claude/skills/file-uploads",
    "description": "Implementing two-step image upload + LLM pixel call (SEMOSS pattern)",
    "content": "Overview\nWhen a user attaches a file..."
  }
]
```

With `includeAll=true`, each skill additionally carries a `files` array. Every file has its own `path` and `directory` (the same shape as the skill itself), so the folder structure can be recreated. Note that `content` is also present, because `includeAll` implies `includeContent`:

```json
[
  {
    "name": "pptx",
    "path": ".claude/skills/pptx/SKILL.md",
    "directory": ".claude/skills/pptx",
    "description": "Use this skill any time a .pptx file...",
    "content": "# PPTX\n...",
    "files": [
      {
        "path": ".claude/skills/pptx/editing.md",
        "directory": ".claude/skills/pptx",
        "content": "# Editing Presentations..."
      }
    ]
  }
]
```

---

## GetSkills

Returns the skills available to you: the **registry skills** (registered as Projects) you can view, **plus** all **platform skills** (disk-backed built-ins). Supports a `filter` to scope the results. This reactor does **not** return skill file contents - use `ListSkills` for that.

### Parameters

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `filter` | string | `accessible` | One of `mine` (registry skills you created), `platform` (platform skills only), or `accessible` (default - every registry skill you can view **plus** all platform skills). |

**Notes**

- A registry skill has a `skill_id` and an `origin` of `USER`, `IMPORTED`, or `GENERATED`. A platform skill has `origin = PLATFORM` and a `slug` but **no `skill_id`** - that is how you tell them apart in the merged `accessible` result.
- `mine` returns registry skills only; `platform` returns platform skills only.

### Examples

```
# defaults to accessible
GetSkills();

GetSkills(filter="mine");
GetSkills(filter="platform");
GetSkills(filter="accessible");
```

### Response

```json
[
  {
    "date_updated": "2026-06-15T14:30:10Z",
    "date_created": "2026-06-15T14:30:10Z",
    "origin": "USER",
    "name": "ppt-master",
    "description": "AI-driven multi-format SVG content generation system. Converts source documents (PDF/DOCX/URL/Markdown) into high-quality SVG pages and exports to PPTX through multi-role collaboration...",
    "skill_id": "019ecbb0-9d26-7302-bcad-c6691bfdfc00",
    "created_by": "rweiler",
    "slug": "ppt-master"
  },
  {
    "date_updated": "2026-06-11T17:57:49Z",
    "date_created": "2026-06-11T17:57:49Z",
    "origin": "USER",
    "name": "pptx",
    "description": "Use this skill any time a .pptx file is involved in any way - as input, output, or both...",
    "skill_id": "019eb7d5-4998-76b4-8620-5e9a516816db",
    "created_by": "rweiler",
    "slug": "pptx"
  },
  {
    "origin": "PLATFORM",
    "name": "database",
    "description": "Use when writing code that queries a relational or graph database on the platform...",
    "slug": "database"
  }
]
```

The last entry is a **platform skill**: note `origin` is `PLATFORM`, it carries a `slug`, and it has **no `skill_id`**. Registry skills always carry a `skill_id`.

---

## Managing skills on a workspace

There are two ways to change which skills a workspace uses:

- **Incremental** - `AttachSkillToWorkspace` / `DetachSkillFromWorkspace` add or remove **one** skill at a time.
- **Bulk** - `EditWorkspace` sets the **whole** configuration (name, MCPs, registry skills, and - when you pass `platformSkills` - platform skills) at once.

A skill is identified one of two ways, and the reactors branch on which you pass:

| | Registry skill | Platform skill |
| --- | --- | --- |
| Identifier | `skillId` (UUID) | `slug` (folder name) |
| Stored in `CONFIG_JSON` as | `skills[]` (`{"skill_id": "..."}`) | `platform_skills[]` (`["slug", ...]`) |
| `WORKSPACE_RESOURCE__` row | yes | no |

All of these reactors require **edit** permission on the workspace.

---

## AttachSkillToWorkspace

Attaches a single skill to a workspace and keeps `WORKSPACE.CONFIG_JSON` in sync. Pass **`skillId`** for a registry skill **or** **`slug`** for a platform skill - exactly one. Idempotent (re-attaching the same skill is a no-op).

### Parameters

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `workspaceId` | string | - | Target workspace. **Required.** |
| `skillId` | string | - | Registry skill to attach. Required **unless** `slug` is given. |
| `slug` | string | - | Platform skill to attach. Required **unless** `skillId` is given. |

### Examples

```
# registry skill (by id)
AttachSkillToWorkspace(workspaceId=["0146f913-2ae3-4f6b-8b04-0e7a53a36145"], skillId=["019e4687-e52c-718a-8460-27cb795896ac"]);

# platform skill (by slug)
AttachSkillToWorkspace(workspaceId=["0146f913-2ae3-4f6b-8b04-0e7a53a36145"], slug=["database"]);
```

**Notes**

- Registry path: writes a `WORKSPACE_RESOURCE__` row (`RESOURCE_TYPE='SKILL'`) and mirrors into `CONFIG_JSON.skills[]` as `{"skill_id": "..."}`.
- Platform path: adds the slug to `CONFIG_JSON.platform_skills[]` (a plain array of slug strings); no `WORKSPACE_RESOURCE__` row.
- Attaching a `slug` with no folder under `<BASE_FOLDER>/skills/` is an error. Passing both `skillId` and `slug`, or neither, is also an error.

### Response

```json
{ "workspace_id": "0146f913-...", "skill_id": "019e4687-...", "created": true }
```

For a platform skill:

```json
{ "workspace_id": "0146f913-...", "slug": "database", "type": "PLATFORM_SKILL" }
```

---

## DetachSkillFromWorkspace

Removes a single skill from a workspace. Same `skillId`-or-`slug` rule as `AttachSkillToWorkspace`. No-op when the skill is not attached. Does **not** delete the skill itself (use `DeleteSkill` for a registry skill; platform skills are read-only and are never deleted through reactors).

### Parameters

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `workspaceId` | string | - | Target workspace. **Required.** |
| `skillId` | string | - | Registry skill to detach. Required **unless** `slug` is given. |
| `slug` | string | - | Platform skill to detach. Required **unless** `skillId` is given. |

### Examples

```
# registry skill (by id)
DetachSkillFromWorkspace(workspaceId=["0146f913-2ae3-4f6b-8b04-0e7a53a36145"], skillId=["019e4687-e52c-718a-8460-27cb795896ac"]);

# platform skill (by slug)
DetachSkillFromWorkspace(workspaceId=["0146f913-2ae3-4f6b-8b04-0e7a53a36145"], slug=["database"]);
```

---

## EditWorkspace

Sets a workspace's configuration in **bulk** - name, description, system prompt, active state, the full MCP list, and the full **registry**-skill list. Use it to declare the complete state rather than nudge one item.

### Parameters

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `workspaceId` | string | - | Target workspace. **Required.** |
| `name` | string | - | Display name. **Required.** |
| `description` | string | - | Workspace description. |
| `systemPrompt` | string | - | Workspace system prompt (mirrored into `CONFIG_JSON.system_prompt`). |
| `isActive` | boolean | `true` | Active/inactive state (owner only to change). |
| `mcp` | list of maps | - | Full MCP set. Each entry is `{id, name, type}`; `type` is the catalog type (`PROJECT` for project/MCP tools, or an engine type such as `MODEL`). |
| `skills` | list of strings | - | Full **registry**-skill set, as a flat list of `skill_id`s. |
| `platformSkills` | list of strings | - | Platform skills, as a flat list of **slugs**. Opt-in: omit it to leave existing platform skills untouched; pass it (even empty) to full-replace `CONFIG_JSON.platform_skills[]`. |

### Examples

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
  skills=["019e4687-e52c-718a-8460-27cb795896ac"],
  platformSkills=["database","model"]
);
```

**Notes**

- `skills` and `mcp` are **full replaces** - the resulting set is exactly what you pass. `skills=[]` removes every registry skill.
- `platformSkills` is **opt-in**: omit the key and any existing `CONFIG_JSON.platform_skills[]` is left untouched (so callers that don't send it never wipe platform skills); pass it and it is a **full replace**, where `platformSkills=[]` clears them all. Each slug is validated against the on-disk catalog - an unknown slug fails the call.

### Which to use

| Need | Use |
| --- | --- |
| Add/remove one registry skill | `AttachSkillToWorkspace` / `DetachSkillFromWorkspace` with `skillId` |
| Add/remove one platform skill | `AttachSkillToWorkspace` / `DetachSkillFromWorkspace` with `slug` |
| Replace the entire registry-skill + MCP set | `EditWorkspace` |
| Add/remove one platform skill | `Attach`/`Detach` with `slug` |
| Replace the entire platform-skill set | `EditWorkspace` with `platformSkills=[...]` |

---

## Reading back what is attached: GetWorkspace

`GetWorkspace(workspaceId=["..."])` returns the workspace, including a `skills[]` array that contains **both** registry and platform skills, distinguished by `type` (`SKILL` vs `PLATFORM_SKILL`):

```json
"skills": [
  { "id": "019e4687-...", "type": "SKILL", "name": "csv-cleaner", "slug": "csv-cleaner", "description": "..." },
  { "id": "database", "type": "PLATFORM_SKILL", "name": "database", "slug": "database", "description": "..." }
]
```

For a platform skill the `id` is the slug (platform skills have no project id). The full `config_json` is returned alongside, so you can read `skills[]` and `platform_skills[]` directly if you prefer the raw form.
