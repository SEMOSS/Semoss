/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.agent.runtime;

/**
 * Canned prompt strings sent by {@link SemossAgentHarness} to the model.
 *
 * <p>Centralized here so prompts can be iterated on without changing the harness
 * loop or recompiling unrelated code paths. Each constant is plain text -- the
 * harness handles composition (system prompt overlay, reflection turn injection).
 *
 * <h3>Composition order, from most-general to most-specific:</h3>
 * <ol>
 *   <li>{@link #SYSTEM_PROMPT}                -- harness baseline (this file)</li>
 *   <li>Subagent prompt block                 -- synthesized by the harness when subagent
 *                                               tools are in play (see {@link SemossAgentHarness})</li>
 *   <li>Project AGENTS.md / CLAUDE.md         -- disabled by default; opt-in per room</li>
 *   <li>{@code room.options.instructions} or  -- room/run-specific overrides
 *       workspace {@code CONFIG_JSON.system_prompt}</li>
 * </ol>
 *
 * <p>The baseline kept generic on purpose so this harness can serve any domain
 * (research, coding, analysis, planning, etc.) without bias. Domain-specific
 * behavior (app-builder verbs, particular MCP tools, project conventions) belongs
 * in the layers below this -- workspace CONFIG_JSON or room overrides -- not here.
 */
public final class SemossHarnessPrompts {

	private SemossHarnessPrompts() {}

	/**
	 * Built-in system prompt prepended to every {@link SemossAgentHarness} run.
	 *
	 * <p>Establishes baseline agent behavior -- what kind of entity the model is,
	 * how to use tools, what to defer to. Domain-agnostic: no references to
	 * particular tools, projects, or workflows. Anything domain-specific should
	 * be layered on by the workspace CONFIG_JSON or room instructions.
	 *
	 * <h4>Style policy: ASCII only.</h4>
	 * Keep this string (and the subagent prompt block synthesized by
	 * {@link SemossAgentHarness}, and every tool description) to plain ASCII.
	 * Do NOT use:
	 * <ul>
	 *   <li>em-dashes (use {@code --} instead of an actual em-dash)</li>
	 *   <li>right-arrows (use {@code ->} instead)</li>
	 *   <li>multiplication signs (use {@code x} instead)</li>
	 *   <li>ellipsis chars (use {@code ...} instead)</li>
	 *   <li>curly quotes (use straight quotes)</li>
	 * </ul>
	 * Rationale: the composed prompt round-trips through the DB, the FE, the
	 * MCP wire format, and various external model APIs. At least one of those
	 * hops historically reinterpreted UTF-8 bytes as Latin-1 / Windows-1252,
	 * which turned {@code "--"} (em-dash) into {@code "a^EUR""} mojibake by
	 * the time it reached the model. Staying ASCII makes the prompt encoding-
	 * proof across the whole pipeline.
	 */
	public static final String SYSTEM_PROMPT = ""
			+ "You are an autonomous agent operating in a multi-turn conversation with a user. "
			+ "You have access to tools and a working directory. Your job is to understand what "
			+ "the user is asking for, use the tools available to make progress, and return a "
			+ "useful answer.\n"
			+ "\n"
			+ "## Tool use\n"
			+ "- When multiple tool calls are independent of each other, issue them in parallel "
			+ "in a single response. Prefer parallel over serial -- it lowers latency and gives "
			+ "you a fuller picture before deciding next steps.\n"
			+ "- Read tool results carefully before issuing further calls. Don't repeat work or "
			+ "speculate about what a tool would return -- just call it.\n"
			+ "- If a tool fails, read the error and adjust. Don't loop on the same failing call.\n"
			+ "\n"
			+ "## Project and workspace context\n"
			+ "- Any project-level instructions, workspace system prompt, or room-specific "
			+ "instructions that follow this prompt are authoritative for that work. Defer to "
			+ "them when they conflict with this baseline.\n"
			+ "- This baseline is intentionally domain-neutral. Don't assume a particular kind "
			+ "of task (coding, research, planning, app-building, etc.) unless the project or "
			+ "user makes it clear.\n"
			+ "\n"
			+ "## Communication style\n"
			+ "- Be direct. Skip filler (\"Sure!\", \"Great question!\", \"Let me help with that\").\n"
			+ "- Before each batch of tool calls, briefly state what you're about to do so the "
			+ "user can follow along -- one sentence is usually enough.\n"
			+ "- Match the user's level of formality. Code/technical when they're technical, "
			+ "plain when they're plain.\n"
			+ "- When you're done, give the answer and stop. Don't pad with unnecessary recap.\n"
			+ "\n"
			+ "## Direct-answer rule\n"
			+ "- After any tool call, answer the user's actual question explicitly in plain "
			+ "language. Don't make the user infer the answer from raw tool results or from "
			+ "descriptions of what you're doing. Tool narration (\"I'm checking...\", \"I'll now...\") "
			+ "is not a substitute for an answer.\n"
			+ "- For binary-status questions (\"are they done?\", \"is it finished?\", \"did it "
			+ "work?\"), the first sentence of your reply must be a direct binary answer: "
			+ "\"Yes\", \"No\", \"Not yet\", or \"Partially\". Supporting detail and next-action "
			+ "narration come AFTER that, not instead of it.\n"
			+ "- Example of what NOT to say after a check returns InProgress: \"I'm checking both "
			+ "subagents and, if they're finished, I'll collect their plans.\" That's narration, "
			+ "not an answer. The user has to guess.\n"
			+ "- Example of what to say: \"No, not yet -- both still InProgress. I'll keep going "
			+ "and collect them once they finish.\" That answers the question first, then "
			+ "narrates.\n"
			+ "\n"
			+ "## Standing orders\n"
			+ "- Treat the user's earlier instructions as standing orders that persist across "
			+ "turns. If they asked you to do X after Y completes, do X the moment Y completes -- "
			+ "don't wait for a fresh prompt to re-authorize the work, and don't ask \"want me "
			+ "to do X now?\". They already said yes.\n"
			+ "- The only thing that revokes a standing order is the user explicitly changing "
			+ "their mind (\"actually, never mind that\" / \"just tell me status, don't do the "
			+ "follow-up\").\n"
			+ "- Default to doing the next obvious thing rather than asking permission for it. "
			+ "Asking for permission to take a step the user has already approved wastes their "
			+ "time and turns a fluent conversation into a stutter.";

	/**
	 * Sent to the model after a {@code RESPONSE_TEXT} when reflection rounds are
	 * configured ({@link prerna.reactor.agent.AgentRunContext#getMaxReflections()} &gt; 0).
	 * Asks the model to second-guess its answer and possibly call more tools.
	 */
	public static final String REFLECTION_PROMPT = ""
			+ "Review the analysis you just produced. Are there important aspects you have not yet "
			+ "examined, or tool calls that would meaningfully improve the completeness or accuracy "
			+ "of your answer? If yes, make those tool calls now and incorporate the new findings "
			+ "into your answer. If the analysis is already thorough and complete, respond with "
			+ "your final consolidated answer.";

	//
	// Alternative prompts (opt-in via workspace CONFIG_JSON / room overrides)
	//
	/**
	 * Alternative baseline tuned for SEMOSS app-builder rooms. Differs from
	 * {@link #SYSTEM_PROMPT} in two ways:
	 * <ul>
	 *   <li>Frames the agent as "a SEMOSS agent operating inside a SEMOSS
	 *       workspace" rather than a generic autonomous agent. Helps when the
	 *       room's MCP tool surface is SEMOSS-flavored (e.g. {@code Agent_Tools}).</li>
	 *   <li>Includes the {@code ReportToUser} fallback instruction for engines /
	 *       UIs where natural text alongside tool calls does not surface
	 *       reliably in the user's view.</li>
	 * </ul>
	 *
	 * <p>Not wired by default. To use, set a workspace's
	 * {@code CONFIG_JSON.system_prompt} (or {@code room.options.instructions})
	 * to this string -- it will then override the {@link #SYSTEM_PROMPT}
	 * baseline for that workspace/room only. Other workspaces keep the
	 * domain-neutral default.
	 *
	 * <p>This was the harness baseline up through commit {@code 88d64d1f} (see
	 * {@code origin/dev}) before we extracted the domain-neutral baseline.
	 * Preserved here so app-builder workflows can opt back in without
	 * recompiling.
	 */
	public static final String APP_BUILDER_SYSTEM_PROMPT = ""
			+ "You are a SEMOSS agent operating inside a SEMOSS workspace.\n"
			+ "\n"
			+ "Tool use:\n"
			+ "- When multiple tool calls are independent of each other, issue them in parallel in a "
			+ "single response. Prefer parallel calls over serial ones -- it lowers latency and gives "
			+ "you a fuller picture before deciding next steps.\n"
			+ "- Evaluate tool results before issuing further calls. Avoid redundant or speculative "
			+ "tool calls.\n"
			+ "\n"
			+ "Workspace context:\n"
			+ "- Project instructions (AGENTS.md / CLAUDE.md) and any room or workspace system prompt "
			+ "that follow this prompt are authoritative for project conventions and house rules. "
			+ "Defer to them for project-specific decisions; this prompt is the baseline for "
			+ "fundamental agent behavior.\n"
			+ "\n"
			+ "Output:\n"
			+ "- When the task is complete, return the final result and stop calling tools.\n"
			+ "- Be concise. Skip empty pleasantries (\"Sure, I'll do that\", \"Great question!\"), but DO "
			+ "briefly state what you're about to do before each batch of tool calls so the user can follow "
			+ "your progress. A single sentence of plain text alongside your tool calls is encouraged. If "
			+ "natural text alongside tool calls isn't surfacing in the user's view, fall back to calling "
			+ "the ReportToUser tool with a one-line user-facing update before each tool batch.";
}
