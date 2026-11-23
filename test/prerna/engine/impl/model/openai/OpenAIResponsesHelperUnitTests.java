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
package prerna.engine.impl.model.openai;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.api.IModelEngine;
import static org.mockito.Mockito.*;

class OpenAIResponsesHelperUnitTests {
	@ParameterizedTest
	@ValueSource(booleans = { false, true })
	void convertsToolDefinitionsAndForcedChoiceInBothOpenAIFormats(boolean responses) {
		Map<String, Object> schema = Map.of("type", "object", "properties", Map.of("q", Map.of("type", "string")));
		Map<String, Object> function = Map.of("name", "lookup", "parameters", schema);
		Map<String, Object> tool = responses ? Map.of("type", "function", "name", "lookup", "parameters", schema)
				: Map.of("type", "function", "function", function);
		var converted = MessageUtils.convertOpenAIToMCPTools(List.of(tool)).getFirst();
		assertEquals("lookup", converted.get("name"));
		assertEquals(schema, converted.get("inputSchema"));
		Object choice = responses ? Map.of("type", "function", "name", "lookup")
				: Map.of("type", "function", "function", Map.of("name", "lookup"));
		assertEquals(Map.of("type", "forced", "name", "lookup"), MessageUtils.toMCPToolChoice(choice));
	}

	@Test
	@SuppressWarnings("unchecked")
	void preservesImageInputsThroughTheModelMessageBoundary() {
		String image = "data:image/png;base64,aW1hZ2U=";
		List<Map<String, Object>> content = List.of(Map.of("type", "input_text", "text", "Describe"),
				Map.of("type", "input_image", "image_url", image));
		Object normalized = OpenAIResponsesHelper.normalizeMessages(List.of(Map.of("role", "user", "content", content)));
		assertEquals(content, ((List<Map<String, Object>>) normalized).getFirst().get("content"));
		Room room = mock(Room.class);
		when(room.getId()).thenReturn("room-1");
		var messages = MessageUtils.convertFullPrompt(normalized, room, mock(IModelEngine.class));
		String json = MessageUtils.toJsonArrayWithImageData(messages);
		assertTrue(json.contains("Describe"));
		assertTrue(json.contains("MEDIA"));
		assertTrue(json.contains("aW1hZ2U="));
	}

	@Test
	@SuppressWarnings("unchecked")
	void ignoresReasoningItemsAndPreservesToolIdsAndArguments() {
		var calls = List.of(Map.of("type", "message", "role", "assistant", "content",
				List.of(Map.of("type", "output_text", "text", "Checking."))),
				Map.of("type", "function_call", "call_id", "call_1", "name", "lookup",
				"arguments", Map.of("query", "Paris")),
				Map.of("type", "reasoning", "summary", List.of()),
				Map.of("type", "function_call_output", "call_id", "call_1", "output", "Sunny"));
		var normalized = (List<Map<String, Object>>) OpenAIResponsesHelper.normalizeMessages(calls);
		assertEquals(2, normalized.size());
		assertEquals("Checking.", normalized.getFirst().get("content"));
		var call = ((List<Map<String, Object>>) normalized.getFirst().get("tool_calls")).getFirst();
		assertEquals("call_1", call.get("id"));
		assertEquals("{\"query\":\"Paris\"}", ((Map<?, ?>) call.get("function")).get("arguments"));
		assertEquals("call_1", normalized.getLast().get("tool_call_id"));
	}

	@Test
	void providerStateAndThinkingDoNotCreateEmptyResponseItems() throws Exception {
		var stream = new OpenAIResponsesStream("model", "response", 0, "job");
		var writer = new java.io.StringWriter();
		assertFalse(stream.writeChunk(Map.of("stream_type", "google_interactions", "data", Map.of("signature", "private")), writer));
		assertFalse(stream.writeChunk(Map.of("stream_type", "thinking", "data", Map.of("thinking", "Thought")), writer));
		assertEquals("", writer.toString());
	}
}
