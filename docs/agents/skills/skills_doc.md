# Agent Skills

Two reactors surface the skills available on the platform. They answer different questions:

- **`ListSkills`** reads the **physical skill files on disk** for a room, project, or the current insight. It does **not** read the database, so any skill files that were copied or added manually are picked up and returned.
- **`GetSkills`** does a **database read** of the skills that are **registered as projects** on the platform.

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

Does a database read to return the skills **registered as projects** on the platform. Supports a `filter` to scope the results. This reactor does **not** return skill file contents - use `ListSkills` for that.

### Parameters

| Key | Type | Default | Description |
| --- | --- | --- | --- |
| `filter` | string | `accessible` | One of `mine` (skills you created), `platform` (platform-origin skills), or `accessible` (every skill-project you can view). |

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
  }
]
```
