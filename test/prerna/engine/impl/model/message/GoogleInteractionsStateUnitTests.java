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
package prerna.engine.impl.model.message;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonParser;

import prerna.engine.impl.model.responses.AskModelEngineResponse;

class GoogleInteractionsStateUnitTests {

	private static Map<String, Object> replay() {
		return Map.of("version", 1, "model", "gemini-4-carbon", "steps", List.of(
				Map.of("type", "thought", "signature", "opaque"),
				Map.of("type", "google_search_result", "signature", "search-signature", "result", List.of()),
				Map.of("type", "model_output", "content", List.of(Map.of("type", "text", "text", "Answer")))));
	}

	@Test
	void responseRoundTripsPrivateStepsThroughStorageCopyAndPythonWithoutExposingThem() {
		AskModelEngineResponse<?> response = AskModelEngineResponse.fromMap(Map.of("response", "Answer",
				"metadata", Map.of(GoogleInteractionsState.KEY, replay(), "ignored", "arbitrary")));
		ResponseMessage message = ResponseMessage.Builder.fromAskModelEngineResponse(response).build();
		String stored = MessageUtils.toJson(message);
		AbstractMessage loaded = MessageUtils.fromJson(stored, null);
		AbstractMessage fork = MessageUtils.deepCopy(loaded);
		String python = MessageUtils.toJsonArrayWithImageData(List.of(fork));
		assertEquals(JsonParser.parseString(stored).getAsJsonObject().get(GoogleInteractionsState.KEY),
				JsonParser.parseString(python).getAsJsonArray().get(0).getAsJsonObject().get(GoogleInteractionsState.KEY));
		assertFalse(MessageUtils.jsonToMapForPixelReturn(stored).containsKey(GoogleInteractionsState.KEY));
		Map<String, Object> display = new HashMap<>(Map.of(GoogleInteractionsState.KEY, replay(), "content", "Answer"));
		assertFalse(MessageUtils.removeSystemPromptFromMessageMap(display).containsKey(GoogleInteractionsState.KEY));
		assertFalse(stored.contains("arbitrary"));
	}

	@Test
	void compactionStartsANewContextWithoutLosingTheProtocolOrInventingSignatures() {
		Map<String, Object> boundary = GoogleInteractionsState.contextBoundary(replay(), "Conversation summary");
		assertEquals("gemini-4-carbon", boundary.get("model"));
		assertEquals(true, boundary.get("context_boundary"));
		assertEquals(List.of(Map.of("type", "model_output", "content",
				List.of(Map.of("type", "text", "text", "Conversation summary")))), boundary.get("steps"));
		assertEquals(boundary, GoogleInteractionsState.validate(boundary));
	}

	@Test
	void invalidOrOversizedReplayIsRejected() {
		assertThrows(IllegalArgumentException.class, () -> GoogleInteractionsState.validate(Map.of("version", 2)));
		assertThrows(IllegalArgumentException.class, () -> GoogleInteractionsState.validate(Map.of(
				"version", 1, "model", "gemini", "steps", List.of(Map.of("type", "user_input")))));
		assertThrows(IllegalArgumentException.class, () -> GoogleInteractionsState.validate(Map.of(
				"version", 1, "model", "gemini", "steps", java.util.Collections.nCopies(4097, Map.of("type", "thought")))));
	}

	@Test
	void fullPromptRestoresBothTextAndToolTurns() {
		List<Map<String, Object>> prompt = List.of(Map.of("role", "assistant", "content", "Answer",
				GoogleInteractionsState.KEY, replay()));
		List<AbstractMessage> converted = MessageUtils.convertFullPrompt(prompt, null, null);
		assertEquals(replay(), converted.getFirst().getGoogleInteractions());
	}

	@Test
	void signedToolTurnsAreNotPartiallyPruned() {
		ResponseMessage call = ResponseMessage.builder().withToolResponses(List.of(new HashMap<>(Map.of(
				"id", "a", "type", "function", "name", "lookup", "arguments", Map.of("q", "retain"))))).build();
		call.setGoogleInteractions(replay());
		ResponseMessage tail = ResponseMessage.builder().withText("Done").build();
		tail.setParentMessageId(call.getMessageId());
		call.setPruneToolsAbove(true);
		List<AbstractMessage> branch = MessageUtils.getMessageBranchWithNewMessage(new ArrayList<>(List.of(call, tail)), null);
		ToolCallMessagePart part = (ToolCallMessagePart) branch.getFirst().getParts().getFirst();
		assertEquals(Map.of("q", "retain"), part.getToolCall().get("arguments"));
	}

	@Test
	void providerErrorUsageSurvivesJavaParsingForBilling() {
		AskModelEngineResponse<?> error = AskModelEngineResponse.fromMap(Map.of("messageType", "ERROR",
				"message", "Interrupted", "code", 500, "numberOfTokensInPrompt", 21, "numberOfTokensInResponse", 13,
				"numberOfThinkingTokens", 8, "numberOfCacheReadTokens", 5));
		assertEquals(21, error.getNumberOfTokensInPrompt());
		assertEquals(13, error.getNumberOfTokensInResponse());
		assertEquals(8, error.getNumberOfThinkingTokens());
		assertEquals(5, error.getNumberOfCacheReadTokens());
	}
}
