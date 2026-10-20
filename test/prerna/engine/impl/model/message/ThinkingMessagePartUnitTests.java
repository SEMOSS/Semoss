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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import prerna.engine.impl.model.responses.AskModelEngineResponse;

class ThinkingMessagePartUnitTests {

	@ParameterizedTest
	@ValueSource(strings = { "reasoning_content", "reasoning" })
	void providerReasoningSurvivesModelResponsePersistenceAndPythonHistoryWithTools(String source) {
		String reasoning = "Check the source first.\nThen compare α and β.";
		Map<String, Object> toolCall = Map.of("id", "call-1", "type", "function", "name", "lookup", "arguments",
				Map.of("query", "example"));
		AskModelEngineResponse<?> response = AskModelEngineResponse.fromMap(Map.of(
				"response", List.of(toolCall), "messageType", "TOOL", "schemaVersion", 2, "io", "OUTPUT",
				"parts", List.of(
						Map.of("type", "THINKING", "thinking", reasoning, "source", source),
						Map.of("type", "TEXT", "text", "I'll check that."),
						Map.of("type", "TOOL_CALL", "tool_call", toolCall))));

		ResponseMessage message = ResponseMessage.Builder.fromAskModelEngineResponse(response).build();
		String storedJson = MessageUtils.toJson(message);
		AbstractMessage reloaded = MessageUtils.fromJson(storedJson, null);
		ThinkingMessagePart thinking = (ThinkingMessagePart) reloaded.getParts().getFirst();
		assertEquals(reasoning, thinking.getThinking());
		assertEquals(source, thinking.getSource());
		assertEquals(MessageType.RESPONSE_TOOL, reloaded.getMessageType());

		JsonObject providerMessage = JsonParser.parseString(MessageUtils.toJsonArrayWithImageData(List.of(reloaded)))
				.getAsJsonArray().get(0).getAsJsonObject();
		JsonObject providerThinking = providerMessage.getAsJsonArray("parts").get(0).getAsJsonObject();
		assertEquals(reasoning, providerThinking.get("thinking").getAsString());
		assertEquals(source, providerThinking.get("source").getAsString());
		assertEquals("I'll check that.", providerMessage.getAsJsonArray("parts").get(1).getAsJsonObject()
				.get("text").getAsString());
		assertEquals("call-1", providerMessage.getAsJsonArray("parts").get(2).getAsJsonObject()
				.getAsJsonObject("toolCall").get("id").getAsString());
	}

	@Test
	void existingThinkingSummaryKeepsSourceAbsent() {
		ResponseMessage message = ResponseMessage.builder().withThinking("A display summary").withText("Answer").build();
		String storedJson = MessageUtils.toJson(message);
		AbstractMessage reloaded = MessageUtils.fromJson(storedJson, null);
		ThinkingMessagePart thinking = (ThinkingMessagePart) reloaded.getParts().getFirst();
		assertEquals("A display summary", thinking.getThinking());
		assertNull(thinking.getSource());
		assertFalse(JsonParser.parseString(storedJson).getAsJsonObject().getAsJsonArray("parts").get(0)
				.getAsJsonObject().has("source"));
		assertFalse(JsonParser.parseString(MessageUtils.toJsonWithImage(reloaded)).getAsJsonObject()
				.getAsJsonArray("parts").get(0).getAsJsonObject().has("source"));
	}
}
