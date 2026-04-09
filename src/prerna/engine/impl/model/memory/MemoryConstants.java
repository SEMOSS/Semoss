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
package prerna.engine.impl.model.memory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Constants for the persistent memory system — prompt templates, default limits,
 * stop words, and kwArg keys.
 * <p>
 * These prompts are sent to the LLM during async post-turn extraction
 * and write-time deduplication. They are never shown to the user.
 */
public final class MemoryConstants {

	private MemoryConstants() {
		// utility class
	}

	// =========================================================================
	// kwArgMap key for passing memory context through to the Python model layer
	// =========================================================================

	/** Parameter key in kwArgMap for memory context injection. */
	public static final String MEMORY_CONTEXT_KEY = "memory_context";

	// =========================================================================
	// Default limits
	// =========================================================================

	/** Default maximum number of memories to recall per turn. */
	public static final int DEFAULT_RECALL_LIMIT = 10;

	/** Default maximum token budget for memory injection. */
	public static final int DEFAULT_TOKEN_BUDGET = 500;

	/** Approximate characters per token for budget estimation. */
	public static final int CHARS_PER_TOKEN_ESTIMATE = 4;

	// =========================================================================
	// Room option keys
	// =========================================================================

	public static final String OPT_MEMORY_ENABLED = "memoryEnabled";
	public static final String OPT_MEMORY_RECALL_LIMIT = "memoryRecallLimit";
	public static final String OPT_MEMORY_TOKEN_BUDGET = "memoryTokenBudget";
	public static final String OPT_MEMORY_EXTRACTION_ENABLED = "memoryExtractionEnabled";

	// =========================================================================
	// Extraction prompt — sent to LLM to extract facts from a conversation turn
	// =========================================================================

	/**
	 * Prompt template for extracting memorable facts from a conversation turn.
	 * Placeholders: {@code {USER_MESSAGE}} and {@code {ASSISTANT_RESPONSE}}.
	 */
	public static final String EXTRACTION_PROMPT = """
			Analyze this conversation turn and extract any facts worth remembering \
			about the user for future conversations.

			Rules:
			- Only extract facts the user explicitly states about themselves, their work, or their preferences.
			- Extract preferences, decisions, personal details, work context, and technical choices.
			- Do NOT extract: passwords, API keys, SSNs, credit card numbers, health conditions, or any secrets.
			- Do NOT extract casual mentions, hypotheticals, or things the user is asking about (not stating).
			- Do NOT extract facts about the assistant or the conversation itself.
			- Each fact must be a single, self-contained statement.
			- Return a JSON array of objects: [{"content": "...", "type": "FACT|PREFERENCE|EPISODE"}]
			- FACT = objective information (name, job, location, project details)
			- PREFERENCE = likes, dislikes, style preferences, tool choices
			- EPISODE = events, milestones, decisions made
			- Return an empty array [] if nothing is worth remembering.

			User said:
			{USER_MESSAGE}

			Assistant said:
			{ASSISTANT_RESPONSE}

			Extract memories as JSON array:""";

	// =========================================================================
	// Deduplication prompt — decides ADD/UPDATE/NOOP for a new fact
	// =========================================================================

	/**
	 * Prompt template for deduplication decisions.
	 * Placeholders: {@code {NEW_FACT}} and {@code {EXISTING_MEMORIES}}.
	 */
	public static final String DEDUP_PROMPT = """
			You are comparing a new fact against existing stored memories for the same user.

			New fact: "{NEW_FACT}"

			Existing memories:
			{EXISTING_MEMORIES}

			Decide one action:
			- ADD: The new fact is genuinely new information with no overlap with any existing memory.
			- UPDATE: The new fact replaces or updates an existing memory (same topic, different value). Return the ID of the memory to replace.
			- NOOP: The new fact is effectively a duplicate of an existing memory. No action needed.

			Return exactly one JSON object:
			{"action": "ADD" or "UPDATE" or "NOOP", "updateMemoryId": "id-of-memory-to-replace or null", "reason": "brief explanation"}""";

	// =========================================================================
	// Injection preamble — prepended to recalled memories before sending to LLM
	// =========================================================================

	/**
	 * Preamble text added before recalled memories in the memory context block.
	 */
	public static final String INJECTION_PREAMBLE = "The following are facts you know about this user "
			+ "from previous conversations. Use them naturally when relevant. "
			+ "Do not mention that you have a memory system unless the user asks.";

	// =========================================================================
	// Stop words for keyword extraction from user messages
	// =========================================================================

	/** Common English stop words filtered out during keyword extraction. */
	public static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
			"a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
			"have", "has", "had", "do", "does", "did", "will", "would", "could",
			"should", "may", "might", "shall", "can", "need", "must",
			"i", "me", "my", "myself", "we", "our", "ours", "ourselves",
			"you", "your", "yours", "yourself", "yourselves",
			"he", "him", "his", "himself", "she", "her", "hers", "herself",
			"it", "its", "itself", "they", "them", "their", "theirs", "themselves",
			"what", "which", "who", "whom", "this", "that", "these", "those",
			"am", "in", "on", "at", "to", "for", "of", "with", "by", "from",
			"as", "into", "through", "during", "before", "after", "above", "below",
			"between", "out", "off", "over", "under", "again", "further", "then",
			"once", "here", "there", "when", "where", "why", "how", "all", "both",
			"each", "few", "more", "most", "other", "some", "such", "no", "nor",
			"not", "only", "own", "same", "so", "than", "too", "very",
			"and", "but", "or", "if", "about", "just", "also", "like", "get",
			"got", "go", "going", "know", "think", "want", "make", "use",
			"tell", "say", "said", "please", "thanks", "thank", "okay", "ok",
			"yes", "no", "yeah", "sure", "right", "well", "now", "still"
	));
}
