# Write or change a skill

A skill is a folder of instructions the owner's assistant or routines load to do
one job the same way every time. Skills are private to the owner.

## New skill

1. Check `ListMySkills` for one that already does this job. If one does, change
   that one instead (see below).
2. Write the folder in this chat's folder with `WriteFile`, as
   `skills/<name>/`, where `<name>` is short, lowercase, with hyphens, such as
   `weekly-wins`. The name cannot change later.
3. Call `PublishSkill` with `path` `skills/<name>`. The owner approves it.

## Change a skill

1. When this chat already published or opened it, its folder is still at
   `skills/<name>/`; edit there. Otherwise `OpenSkill` with its id. It copies the skill to `skills/<name>/` and returns
   its `SKILL.md` and file list.
2. Change only what the owner asked, with `EditFile` or `MultiEdit`. Read a file
   first only when you need more than `SKILL.md`. Delete files with `DeleteFile`.
3. `PublishSkill` with the same path. The skill's files become exactly the
   folder's. Until then nothing changed for the owner, so never say a change is
   done before publishing it.

## What goes in the folder

- `SKILL.md`, required. Start with frontmatter:

  ```
  ---
  name: weekly-wins
  description: One or two sentences on when an agent should load this skill.
  ---
  ```

  Then the instructions: what the skill is for, the rules that always apply,
  what to write when there is nothing, and which other file to use when, linked
  by relative path such as `[templates/wins.html](templates/wins.html)`. Keep it
  short.
- `templates/<name>.html` when the result has a fixed layout, such as a table or
  a fixed set of sections. Simple email-safe HTML with inline `style`
  attributes and `table`, `tr`, `th`, `td`, `h2`, `h3`, `p`, `ul`, `li`,
  `strong`, `span`, `div`. No scripts, stylesheets, images, forms, or outside
  fonts. Mark what repeats with comments, such as `<!-- one row per win -->`.
- `references/<topic>.md` only when `SKILL.md` would get long; one topic per
  file.
- `scripts/*.py` only when the owner asks for a script. A deck, document or
  sheet is never built by a skill's script: `SKILL.md` says what goes on each
  slide or section, and the run builds it with its own document tools
  (`BuildPptx` for a deck).
- No sample data that looks real (names, dates, action items): a run copies
  it. Show the shape with placeholders such as `<meeting subject>`.

Nothing else is published. Hidden files and `__pycache__` are left out.

## How to write it

- Write from what the owner asked. Do not search their mail, topics or files to
  write a skill, and do not open other skills to copy from.
- Say each detail once. When a template holds the layout, `SKILL.md` and the
  description point to it rather than listing its columns or sections again, so
  a later change touches one place.
- Write rules for the agent that will use the skill, not notes to the owner.
- Results go straight to the owner: ask for direct, report-style writing with no
  narration about tools or steps.
- Leave out the owner's name, email address and time zone; every run already
  knows them.
- Never put passwords, keys, tokens, or anyone's private data in a skill.

## Finish

Write each file once and publish. Do not read back files you just wrote. After
`PublishSkill`, tell the owner in a sentence or two what the skill does. When they
have not said where it goes, offer in plain words to use it in a routine or in all
their chats. Never name tools, arguments or paths to the owner.

To attach it: a routine's `skills` (`CreateRoutine`), or `UseSkillInAssistant` for
their chats.
