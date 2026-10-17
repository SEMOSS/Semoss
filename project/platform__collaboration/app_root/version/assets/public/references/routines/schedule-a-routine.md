# Schedule a routine

A routine is a task the owner's agent repeats on a schedule while they are away.
Each run has read tools only, nobody can approve anything during it, and the
platform delivers the final answer to the owner alone, by email, as a Teams message
to themselves, or only in Routines. Use `CreateRoutine` to save or change one,
`ListRoutines` to find one, and `ManageRoutine` to pause, resume, run, or delete it.

## Write the task

The `prompt` is what the run reads every time. Write it as the owner's request,
in their words, as a short paragraph:

- Say what to look at and what the result should contain. Keep the owner's own
  limits, such as "skip all-day events" or "only clients".
- Leave out delivery. Never name a recipient, an address, a chat, or "send". The
  `delivery` argument decides where the result goes, and a run that reads "send a
  Teams message" will try and fail.
- Leave out tool names, tool options, and steps. The run chooses its own reads.
- Leave out the time zone and the schedule. They are separate arguments, and the
  run is told the owner's time zone.
- Leave out formatting detail longer than a sentence. Put a lasting layout in a
  skill (below) instead.

Good: "Find my first meeting today, or the next day with one, skipping all-day and
canceled events. Give its subject, time, and who it is with."

Bad: "Review my Microsoft 365 calendar (default Calendar) using ListEvents... Send
a Teams message to myself (me@example.com) with... No email delivery."

Fill `time`, `days`, and `timezone` from what the owner said; ask only when the
schedule is missing entirely. Pick `delivery` from their words: email when they
want to be alerted, Teams for a quiet note in their own chat, none to only keep it
in Routines. Give the routine a short `name` they will recognize.

## Give a recurring result a skill

When the result has a fixed shape the owner will read again and again, such as a
table of meetings, a digest grouped by client, or a status report, offer to make a
skill for it and attach it through the routine's `skills`. A skill keeps the format
the same on every run and lets the task stay short.

`CreateSkill` hands a brief to a skill builder that works in the skill's own folder
and writes the files: `SKILL.md`, and when useful `references/` for longer rules,
`templates/` for an email-safe HTML layout the run fills in, and `scripts/` for a
mechanical step. The builder cannot see this chat, so the `brief` must carry
everything: what the skill is for, the owner's rules and preferences, what the
result contains, its sections or columns and their order, what to write when there
is nothing, and whether it needs a template. Building takes a few minutes and
returns the files it wrote; tell the owner what the skill now does.

Skip the skill for a one-line answer.

When the owner later wants a routine's result to look or work differently, change
its skill with `EditSkill` (the skill's id from `ListMySkills`) rather than
lengthening the routine's task, and never create a second skill for the same job.
`ReadMySkill` lists a skill's files or reads one, to show the owner what it says or
to check before editing. `DeleteMySkill` removes one for good.

## After saving

Report the routine's name, when it runs, and where the result goes, in one or two
sentences. Offer to run it now with `ManageRoutine` (action `run`); when the owner
asks to try it, run it yourself rather than telling them where to click.

## Running one now

`ManageRoutine` with action `run` starts the routine on its own, apart from this
chat. It takes a few minutes and is not a sub-agent: never use `WaitForSubAgent`,
`CheckSubAgentStatus`, or any other sub-agent tool on a routine id, and do not keep
checking in a loop. Tell the owner it started and where the result will go, then
end your turn. When they later ask whether it finished, call `ListRoutines` once
and report the last run's status, and its error if it failed.
