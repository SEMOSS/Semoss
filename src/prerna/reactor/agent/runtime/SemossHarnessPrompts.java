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
 * loop or recompiling unrelated code paths. Each constant is plain text — the
 * harness handles composition (system prompt overlay, reflection turn injection).
 */
public final class SemossHarnessPrompts {

	private SemossHarnessPrompts() {}

	/**
	 * Built-in system prompt prepended to every {@link SemossAgentHarness} run.
	 * Sets baseline agent behavior (parallel tool use, deference to project context,
	 * conciseness). Project-level AGENTS.md / CLAUDE.md and the room/workspace
	 * system prompt are appended after this so the order from most-general to
	 * most-specific is:
	 * <ol>
	 *   <li>this harness prompt   — fundamental SEMOSS-agent behavior</li>
	 *   <li>AGENTS.md / CLAUDE.md — project conventions</li>
	 *   <li>room.options.instructions or workspace prompt — room/run-specific</li>
	 * </ol>
	 */
	public static final String SYSTEM_PROMPT = ""
			+ "You are a SEMOSS agent operating inside a SEMOSS workspace.\n"
			+ "\n"
			+ "Tool use:\n"
			+ "- When multiple tool calls are independent of each other, issue them in parallel in a "
			+ "single response. Prefer parallel calls over serial ones — it lowers latency and gives "
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
}
