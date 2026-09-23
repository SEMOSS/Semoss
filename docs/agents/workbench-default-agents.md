# Workbench default agents

Workbench assistants use these platform WORKSPACE projects by default:

| Workbench | Agent ID | Display name | Skills |
| --- | --- | --- | --- |
| Code app | `app-builder` | App Building Agent | Existing app-building skill set |
| Database | `database-explorer` | Database Explorer | database, python, pagination, exports |
| Notebook app | `notebook-analyst` | Notebook Analyst | python, database, file-uploads, storage, exports |
| Automation | `workflow-automation-builder` | Automation Building Agent | workflow-automation |

Database Explorer and Notebook Analyst also receive the platform `reactor-help` MCP. The active workbench supplies its engine or project tools and context. No model engine is hardcoded; the user-selected assistant model remains in use.

App Building Agent, Database Explorer, and Notebook Analyst ask clarifying questions in a final chat reply, end the turn, and wait for the user's next prompt before continuing. Clarification does not use a tool or keep the agent running while it waits.

Database Explorer inspects the current engine's schema and runs bounded queries, explains data and query results, and uses Python when useful. Exploration is read-only unless the user requests a change.

The database workbench supplies the catalog subtype and user permission. Room tools are generated from the loaded engine: relational databases receive `SqlQuery` with the actual SQL dialect in its description; RDF databases receive `SparqlQuery`. SQL retains approval mode `ask` because that reactor also accepts writes; SPARQL SELECT uses `auto`. Tinker, JanusGraph, and DataStax graph tools explain the supported structured `Database | Select | Collect` route translated to Gremlin; no raw `GremlinQuery` reactor is advertised. Neo4j is identified as Cypher, and unknown implementations do not default to SQL. Tools refresh before each assistant submission, including resumed conversations.

Database context belongs to the workbench, execution capabilities to the generated tools, and analysis behavior to the agent. The `database` skill's `references/exploration.md` provides a general workflow for understanding the question, grounding an analysis in schema and data, selecting database or Python operations, and checking conclusions. Help lookups resolve specific uncertainties; there is no fixed lookup quota. The agent adapts when results stop adding useful evidence, rather than repeating unsuccessful calls. Preview sizes are never treated as whole-dataset counts.

Database Explorer asks for clarification when missing information would make an answer misleading, such as competing definitions or incompatible units. An optional filter need not prevent a useful broad answer: the agent states the actual population, filters, time coverage, and units, then offers a relevant refinement grounded in discovered dimensions. It carries established definitions and filters into follow-up analysis instead of silently resetting them. For example, a request for revenue with a clear definition can receive the supported total and its scope, followed by an offer to filter or compare geographies if the data contains that dimension; an unresolved gross-versus-net definition calls for clarification first.

Validate behavior with varied questions: explain what the data represents, investigate duplicate or missing records, compare groups or time periods, troubleshoot an incorrect join, and examine an unexpected result. Repeat suitable cases against SQL, RDF, and graph engines. Check whether the agent chooses a supported route, executes useful work, revises from actual errors, and grounds its conclusion in returned data. Prompt review and tool-routing unit tests do not establish the quality of a live analysis.

Notebook Analyst saves substantive answers in notebook cells by default, including conceptual explanations and examples without an explicit request to write code. Markdown cells hold explanations and conclusions; code cells hold useful computations and visualizations. Chat briefly reports the path, changes, and actual checks after saving. Explicit chat-only requests, clarification questions, and status messages can stay in chat; unavailable write tools or permissions are reported without claiming a save.

It inspects existing notebooks before choosing a destination. An explicit destination takes priority; related follow-ups continue the established notebook. `public/main.ipynb` is the fallback for new or empty projects, not a claim about the currently selected editor tab. A clearly unrelated topic gets a descriptively named new notebook under `public`, preserving existing work. Ambiguous topic changes or destinations prompt a focused clarification. Notebook editing, preserving cells and metadata, reproducible data analysis, and distinguishing inline Python checks from a full notebook run belong to the agent's instructions. The shared `python` skill stays focused on Python execution and the standard image's libraries, based on `py/install_config/pyproject.toml`; actual worker availability can differ.

Validate notebook behavior by asking for Monte Carlo examples without mentioning files: a blank project should receive explanation and useful example cells in `public/main.ipynb`, followed by a short chat summary. A related follow-up should extend that notebook; a clearly unrelated analysis should create a separate notebook without altering the first. An ambiguous destination should lead to clarification, and an explicit chat-only request should leave notebook files unchanged. Check saved JSON validity and confirm that claimed writes and executions match tool results.

## Registration and deployment

The frontend agent IDs must match `Constants`, `SystemDefaultEngines`, and `SystemAgentSeeder`. Each system agent also needs its `project/platform__<id>.smss` and project directory in the deployed SEMOSS home.

Deploy the backend build and project assets together with the frontend changes, then restart the backend. `ProjectWatcher.init()` catalogs the global system agents and `SystemAgentSeeder` writes their prompts, tools, and skills into the model-inference database. Model-inference logging must be enabled for workspace seeding. Existing system agent configuration is reconciled on subsequent starts.

## Selection and saved conversations

The workbench configures `defaultAgent`; the user's `agent` selection is an optional override. Refreshing workbench context does not replace that override. **Use default** clears it, returning to the current workbench's agent.

At message submission, the client captures one effective agent and uses it for both room options and the RunAgent workspace ID. Changing the selection while submission is pending applies to the next message.

Workbench `configure({ systemPrompt })` appends that text after the selected agent's authored prompt. Use `configure({ systemPrompt, overrideSystemPrompt: true })` to replace the agent's authored prompt with that text instead. This applies to both default and custom agent selections; blank text keeps the agent prompt. Agent tools, skills, and harness instructions remain in place in either mode.

The workbench sends these values as room `instructions` and `overrideSystemPrompt` on every submission, including resumed rooms. The workbench flag defaults to `false`; legacy room clients that omit the flag retain their existing replacement behavior. Deploy the backend prompt-composition change with the frontend so the flag is honored.

Room options store the effective `workspace` and `workbenchAgentMode` (`default` or `custom`) on submission. Resuming a default-mode conversation uses the current workbench default; custom selections are restored. Older conversations with a saved workspace and no mode retain that workspace as a custom selection. A selection that has not been used for a message is not yet persisted to the server.
