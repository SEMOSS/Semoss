# Default skills in collaboration rooms

Every `RunAgent` resolution for a room whose project is `SYSTEM__COLLABORATION`
includes `collaboration` and `python`. This covers new sessions, thread
rooms, selected-agent runs, follow-ups, and resumed runs. Normal Playground rooms
retain their existing skill selection.

## One new skill, one reused default

`project/platform__collaboration.smss` registers the new platform skill. Its canonical
instructions live in `project/platform__collaboration/app_root/version/assets/public/`:

```text
SKILL.md
references/
  files/find-and-read.md
  files/write-and-deliver.md
  documents/read-and-extract.md
  documents/create-and-verify.md
  sources/ground-an-answer.md
  sources/citations-and-uncertainty.md
  email/drafts-and-recipients.md
  email/attachments-and-send.md
  actions/identify-and-deduplicate.md
  actions/update-and-report.md
```

The Python and PPTX packages are separate defaults. Root collaboration runs whose
agent has no managed PPTX workflow are offered `PreparePptxEdit`, `ApplyPptxEdits`
and `BuildPptx` next to the collaboration tools, and get
`CollaborationPrompts.PPTX_PROMPT`. The managed workflow (validation, the system PPTX
Reviewer, restore on broken output) starts on the first of those calls
(`PptxWorkflow.onDemand`); a turn that never calls one runs as an ordinary chat.
`InspectPptx` is disabled in collaboration rooms through the resolved default-tool
policy, so it is omitted from their tool catalog and stale calls are rejected.
Every collaboration room also gets the platform Pixabay MCP
(`SystemDefaultEngines.getCollaborationMCPs()`, resolved in
`Room.getAllToolsJsonForRoom`). Ordinary Playground tools remain unchanged.
Collaboration references
distinguish Python's process directory from the execution's `ROOT`. Document reading
prefers the base image's Docling converter for structured Markdown, with bounded
conversion, local model checks, notes/provenance handling, and native-reader fallbacks
for exact Office semantics.
`from smssutil import get_document_markdown` exposes the shared converter: relative
paths resolve to ROOT, Markdown includes source labels/notes, originals stay intact,
and no output is written. The helper guidance lives in the collaboration skill and
its document/file references. The collaboration runtime prompt advertises it before
optional skill loading so reading does not default to visual inspection or hand-written
Office XML parsing. Other base skills are unchanged by this wrapper update.
This feature does not register a presentation UI app or enable new action-item tools.

## Resolution and loading

`SystemDefaultEngines.getCollaborationSkills()` owns the immutable defaults.
`AgentConfigLoader.resolveSkills()` first merges workspace resource rows, workspace
`CONFIG_JSON.skills`, and `room.options.skills`; it then appends missing collaboration
defaults. References are deduplicated by `skill_id`. An explicit reference retains
its existing pinned-version metadata instead of being replaced by an unpinned default.

Clients need not send a skills argument or persist the defaults in room options.
The backend applies them each time it resolves the effective run configuration.
The `AgentConfigLoader: collaboration skills` log records default and effective IDs.

The existing `SkillStager` materializes packages under the active working directory's
`.claude/skills/`. The harness advertises discovered names, descriptions, and locations
in `<available_skills>` in the model's actual system prompt. Full instructions load
on demand through `LoadSkill`, including reference paths such as
`collaboration/references/email/attachments-and-send.md`. No staging-path migration
is included.

A skill is instruction content, not a grant of permissions or tool access. References
use exposed schemas and describe missing-capability behavior. In particular, chat
todos and proposed action lists do not imply saved Work items.

## Deployment and verification

Generated files must be verified with room file tools as well as format-specific
readback. The collaboration references require a Markdown `room://` link for each
verified room deliverable. Teamwork resolves that link through its current room's
insight and existing file panel, including its Download action. The runtime prompt
and Python tool description repeat the explicit `ROOT` path requirement so it is
available before an optional skill read. Recovery guidance continues authorized
local repairs while preserving edits and respecting rejected external actions.

Deploy the new `.smss` and public skill directory with the backend change. Updating
Java classes alone does not distribute the Markdown package. Existing platform
project startup registration catalogs the skill through `getSystemSkills()`.

Validate an agentless collaboration session, a thread-context room, and a follow-up:
inspect effective IDs and the actual prompt, load the entry and a nested reference,
read/write a room file, and execute a small calculation through managed Python.
Check that ordinary rooms do not gain these defaults, and that overlapping explicit
skill references keep their precedence without duplicate IDs. Avoid sending mail
or changing external systems during these checks.
