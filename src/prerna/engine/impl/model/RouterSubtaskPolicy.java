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
package prerna.engine.impl.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessagePart;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.message.ToolResultMessagePart;
import prerna.engine.impl.model.message.ToolResultPart;

/**
 * The validated {@code subtasks[]} of a {@code mode: "subtask"} router (see {@link ModelRouterEngine}) and the
 * classifier prompt built from them. What the classifier sees about a conversation (goal, previous subtask, last
 * tool steps) is derived from the room's messages, never stored, so every cluster node routes the same way.
 */
public final class RouterSubtaskPolicy {

	/** One {@code subtasks[]} entry, as in router.json; {@code route} names an existing {@code routes[]} entry. */
	public static class Subtask {
		String id;
		List<String> recognition_rules;
		String route;
		String skill_id;

		public String id() {
			return id;
		}

		public String route() {
			return route;
		}

		public String skillId() {
			return skill_id;
		}
	}

	private final List<Subtask> subtasks;
	private final Subtask defaultSubtask;
	private final String version;

	private RouterSubtaskPolicy(List<Subtask> subtasks, Subtask defaultSubtask, String version) {
		this.subtasks = subtasks;
		this.defaultSubtask = defaultSubtask;
		this.version = version;
	}

	/** Validates the subtask settings of a router config; every subtask must name one of {@code routeNames}. */
	public static RouterSubtaskPolicy parse(List<Subtask> subtasks, String defaultSubtask, String version,
			Collection<String> routeNames, String configName) {
		if (subtasks == null || subtasks.isEmpty()) {
			throw new IllegalArgumentException("ModelRouterEngine: mode 'subtask' requires at least one subtask in " + configName);
		}
		Set<String> routes = new HashSet<>();
		routeNames.forEach(name -> routes.add(name.toLowerCase(Locale.ROOT)));
		Set<String> seen = new HashSet<>();
		Subtask fallback = null;
		for (Subtask s : subtasks) {
			if (s == null || (s.id = trimOrNull(s.id)) == null || !seen.add(s.id.toLowerCase(Locale.ROOT))) {
				throw new IllegalArgumentException("ModelRouterEngine: a subtask in " + configName + " has a missing or duplicate id");
			}
			s.route = trimOrNull(s.route);
			if (s.route == null || !routes.contains(s.route.toLowerCase(Locale.ROOT))) {
				throw new IllegalArgumentException("ModelRouterEngine: subtask '" + s.id + "' in " + configName
						+ " names route '" + s.route + "', which is not one of the routes");
			}
			s.skill_id = trimOrNull(s.skill_id);
			if (s.id.equalsIgnoreCase(trimOrNull(defaultSubtask))) {
				fallback = s;
			}
		}
		if (fallback == null) {
			throw new IllegalArgumentException("ModelRouterEngine: default_subtask must be one of the subtasks in " + configName);
		}
		return new RouterSubtaskPolicy(List.copyOf(subtasks), fallback, trimOrNull(version));
	}

	public Subtask defaultSubtask() {
		return defaultSubtask;
	}

	public String version() {
		return version;
	}

	/** The subtask with this id (case-insensitive); null when there is none. */
	public Subtask find(String id) {
		return subtasks.stream().filter(s -> s.id.equalsIgnoreCase(trimOrNull(id))).findFirst().orElse(null);
	}

	/** The subtask a classifier reply names, ignoring case, quotes and surrounding punctuation; null when none. */
	public Subtask parseChoice(String reply) {
		return reply == null ? null : find(reply.trim().replaceAll("^[\\s\"'`]+|[\\s\"'`.]+$", ""));
	}

	/** The prompt asking which subtask the agent's next step belongs to. */
	public String classifierPrompt(String goal, String previousSubtask, List<String> recentSteps) {
		StringBuilder prompt = new StringBuilder("You are a routing classifier for an agent. Choose the one subtask "
				+ "that the agent's NEXT step belongs to. Reply with ONLY the subtask id - no explanation, "
				+ "no punctuation, no quotes.\n\nSubtasks:\n");
		for (Subtask s : subtasks) {
			prompt.append("- ").append(s.id);
			if (s.recognition_rules != null && !s.recognition_rules.isEmpty()) {
				prompt.append(": ").append(String.join("; ", s.recognition_rules));
			}
			prompt.append('\n');
		}
		prompt.append("\nGoal: ").append(goal).append("\nPrevious subtask: ")
				.append(previousSubtask == null ? "none" : previousSubtask).append("\nLast ").append(recentSteps.size())
				.append(" completed steps (oldest first):").append(recentSteps.isEmpty() ? " none" : "");
		for (int i = 0; i < recentSteps.size(); i++) {
			prompt.append('\n').append(i + 1).append(". ").append(recentSteps.get(i));
		}
		return prompt.append("\n\nSubtask id:").toString();
	}

	/** The latest user-written request in the provider context; empty when there is none. */
	public String goalOf(List<AbstractMessage> providerContext) {
		for (AbstractMessage message : providerContext.reversed()) {
			if (message instanceof InputMessage request && request.hasTextPart() && !request.hasToolResultPart()
					&& !request.isPlatformGenerated() && request.getSummaryLeafMessageId() == null
					&& !"reflection_input".equals(request.getOrnament("agentRunRole"))) {
				String text = trimOrNull(request.getInputUIPrompt());
				return text != null ? text : Objects.toString(trimOrNull(request.getFullInputPrompt()), "");
			}
		}
		return "";
	}

	/** The subtask recorded in the routing trace of the latest routed response; null when none. */
	public static String previousSubtaskOf(List<AbstractMessage> branch) {
		for (int i = branch.size() - 1; i >= 0; i--) {
			if (branch.get(i).getSummaryLeafMessageId() == null && branch.get(i) instanceof ResponseMessage response
					&& response.getOrnament("routing") instanceof Map<?, ?> routing
					&& routing.get("trace") instanceof Map<?, ?> trace
					&& trace.get("subtask_id") instanceof String id && !id.isBlank()) {
				return id;
			}
		}
		return null;
	}

	/** The current contiguous tool batch, oldest first, with tool names and status only. */
	public List<String> recentStepsOf(List<AbstractMessage> branch) {
		List<String> steps = new ArrayList<>();
		for (AbstractMessage message : branch.reversed()) {
			if (!(message instanceof InputMessage input) || !input.hasToolResultPart()) {
				break;
			}
			for (MessagePart part : input.getParts().reversed()) {
				ToolResultPart result = part instanceof ToolResultMessagePart tool ? tool.getToolResult() : null;
				if (result != null) {
					String name = trimOrNull(result.getToolName());
					String status = trimOrNull(result.getToolStatus());
					steps.add((name == null ? "tool" : name) + (status == null ? "" : " (" + status + ")"));
				}
			}
		}
		Collections.reverse(steps);
		return steps;
	}

	private static String trimOrNull(String s) {
		return s == null || s.isBlank() ? null : s.trim();
	}
}
