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
package prerna.collaboration;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import prerna.auth.User;

/**
 * Marks an agent run as tainted once it has read mail or Teams content the owner did not write.
 * Such text can carry instructions meant for the agent (prompt injection), so an approval for a
 * run that read it should say so. This only records what was read; it never blocks a tool.
 */
public final class CollaborationTaint {

	/** Tools whose results hand the model text written by other people. */
	private static final Set<String> UNTRUSTED_SOURCE_TOOLS = Set.of("ReadThread", "SearchMail", "RelatedThreads",
			"ListM365Mail", "DownloadAttachment", "ListChatMessages", "SearchTeamsMessages", "GetChatMessage",
			"GetChat", "GetMeetingTranscript", "DownloadMessageAttachment");

	private static final Set<String> SENDER_KEYS = Set.of("from", "sender");
	private static final int MAX_SOURCES = 10;

	/** Keys in a pending action's toolMeta that carry the run's taint. */
	public static final String META_UNTRUSTED = "SMSS_UNTRUSTED_CONTENT";
	public static final String META_SOURCES = "SMSS_UNTRUSTED_SOURCES";

	// by run id, so a run that pauses for approval and resumes keeps its mark within the process
	private static final Cache<String, List<Map<String, Object>>> SOURCES = Caffeine.newBuilder()
			.expireAfterAccess(Duration.ofHours(2)).maximumSize(5_000).build();

	private CollaborationTaint() {
	}

	/** Whether the tool returns content other people wrote. */
	public static boolean isUntrustedSource(String toolName) {
		return toolName != null && UNTRUSTED_SOURCE_TOOLS.contains(toolName);
	}

	/**
	 * Records the run as tainted when this tool result holds content from anyone but the owner. A
	 * result whose sender cannot be told counts as outside the owner.
	 */
	public static void record(String runId, User user, String toolName, String output) {
		if (runId == null || runId.isBlank() || !isUntrustedSource(toolName) || output == null || output.isBlank()) {
			return;
		}
		List<String> outsiders = outsideSenders(output, CollaborationToolResults.ownEmail(user));
		if (outsiders == null) {
			return;
		}
		List<Map<String, Object>> sources = SOURCES.get(runId, id -> new CopyOnWriteArrayList<>());
		if (sources.size() >= MAX_SOURCES) {
			return;
		}
		String from = outsiders.isEmpty() ? null : outsiders.get(0);
		for (Map<String, Object> source : sources) {
			if (toolName.equals(source.get("tool")) && java.util.Objects.equals(from, source.get("from"))) {
				return;
			}
		}
		Map<String, Object> source = new LinkedHashMap<>();
		source.put("tool", toolName);
		if (from != null) {
			source.put("from", from);
		}
		sources.add(source);
	}

	/** Whether the run has read content from outside the owner. */
	public static boolean isTainted(String runId) {
		return !sources(runId).isEmpty();
	}

	/** What the run read from outside the owner: the tool, and the sender when known. */
	public static List<Map<String, Object>> sources(String runId) {
		List<Map<String, Object>> sources = runId == null ? null : SOURCES.getIfPresent(runId);
		return sources == null ? List.of() : new ArrayList<>(sources);
	}

	/** Adds the run's taint to a pending action's toolMeta; a run that read nothing outside adds nothing. */
	public static void stamp(String runId, Map<String, Object> meta) {
		List<Map<String, Object>> sources = sources(runId);
		if (!sources.isEmpty()) {
			meta.put(META_UNTRUSTED, true);
			meta.put(META_SOURCES, sources);
		}
	}

	/**
	 * Brings back the taint of a resumed run from the actions it paused on, for when this process
	 * no longer holds it.
	 */
	@SuppressWarnings("unchecked")
	public static void restore(String runId, List<Map<String, Object>> actions) {
		if (runId == null || runId.isBlank() || actions == null || isTainted(runId)) {
			return;
		}
		for (Map<String, Object> action : actions) {
			Object meta = action.get("toolMeta");
			if (meta instanceof String text && !text.isBlank()) {
				meta = CollaborationDbUtils.parseMap(text);
			}
			if (meta instanceof Map<?, ?> map && Boolean.TRUE.equals(map.get(META_UNTRUSTED))
					&& map.get(META_SOURCES) instanceof List<?> stored) {
				List<Map<String, Object>> sources = SOURCES.get(runId, id -> new CopyOnWriteArrayList<>());
				for (Object source : stored) {
					if (source instanceof Map<?, ?> one && sources.size() < MAX_SOURCES && !sources.contains(one)) {
						sources.add((Map<String, Object>) one);
					}
				}
			}
		}
	}

	/**
	 * The senders other than the owner, an empty list when the result names none, or null when
	 * every sender is the owner.
	 */
	private static List<String> outsideSenders(String output, String owner) {
		Object parsed = CollaborationDbUtils.parseMap(output);
		if (parsed == null) {
			return List.of();
		}
		List<String> senders = new ArrayList<>();
		boolean[] found = { false };
		collectSenders(parsed, owner, senders, found);
		if (found[0] && senders.isEmpty()) {
			return null;
		}
		return senders;
	}

	private static void collectSenders(Object value, String owner, List<String> outsiders, boolean[] found) {
		if (value instanceof Map<?, ?> map) {
			for (Map.Entry<?, ?> entry : map.entrySet()) {
				if (entry.getKey() instanceof String key && SENDER_KEYS.contains(key) && entry.getValue() != null) {
					found[0] = true;
					List<String> labels = new ArrayList<>();
					boolean isOwner = !owner.isEmpty() && labels(entry.getValue(), labels, owner);
					if (!isOwner) {
						outsiders.add(labels.isEmpty() ? "unknown" : labels.get(0));
					}
				} else {
					collectSenders(entry.getValue(), owner, outsiders, found);
				}
			}
		} else if (value instanceof List<?> list) {
			for (Object item : list) {
				collectSenders(item, owner, outsiders, found);
			}
		}
	}

	// gathers the strings under a sender value; true when one is the owner's address
	private static boolean labels(Object value, List<String> labels, String owner) {
		if (value instanceof String text) {
			labels.add(text);
			return text.equalsIgnoreCase(owner);
		}
		boolean isOwner = false;
		if (value instanceof Map<?, ?> map) {
			for (Object nested : map.values()) {
				isOwner |= labels(nested, labels, owner);
			}
		} else if (value instanceof List<?> list) {
			for (Object nested : list) {
				isOwner |= labels(nested, labels, owner);
			}
		}
		return isOwner;
	}
}
