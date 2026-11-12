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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.comm.PixelJobManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class OpenAIChatCompletionsStreamUnitTests {

	private static final String JOB = "job-1";

	private static Map<String, Object> chunk(String streamType, Map<String, Object> data) {
		Map<String, Object> chunk = new HashMap<>();
		chunk.put("stream_type", streamType);
		chunk.put("data", new HashMap<>(data));
		return chunk;
	}

	private static OpenAIChatCompletionsStream stream() {
		return new OpenAIChatCompletionsStream("engine-1", "chatcmpl-1", 0L, JOB);
	}

	@Test
	void translatesEitherVocabularyIntoAChatCompletionsFinishReason() {
		assertEquals("tool_calls", OpenAIChatCompletionsStream.chatFinishReason("tool_use", false));
		assertEquals("tool_calls", OpenAIChatCompletionsStream.chatFinishReason("tool_calls", false));
		assertEquals("stop", OpenAIChatCompletionsStream.chatFinishReason("end_turn", false));
		assertEquals("stop", OpenAIChatCompletionsStream.chatFinishReason(null, false));
		assertEquals("length", OpenAIChatCompletionsStream.chatFinishReason("max_tokens", false));
		assertEquals("content_filter", OpenAIChatCompletionsStream.chatFinishReason("content_filter", false));
		assertEquals("tool_calls", OpenAIChatCompletionsStream.chatFinishReason("stop", true));
		assertEquals("length", OpenAIChatCompletionsStream.chatFinishReason("length", true));
	}

	@Test
	void finishesAStreamedToolCallWithToolCalls() throws Exception {
		OpenAIChatCompletionsStream stream = stream();
		StringWriter out = new StringWriter();
		Map<String, Object> function = new HashMap<>();
		function.put("name", "get_weather");
		function.put("arguments", "{}");
		Map<String, Object> delta = new HashMap<>();
		delta.put("index", 0);
		delta.put("id", "call_1");
		delta.put("function", function);

		stream.writeChunk(chunk("tool", delta), out);
		assertTrue(stream.writeChunk(chunk("tool", Map.of("finish_reason", "tool_use")), out));

		assertTrue(out.toString().contains("\"finish_reason\":\"tool_calls\""), out.toString());
	}

	@Test
	void sendsNothingForThinkingOrMedia() throws Exception {
		OpenAIChatCompletionsStream stream = stream();
		StringWriter out = new StringWriter();

		assertFalse(stream.writeChunk(chunk("thinking", Map.of("thinking", "Let me see")), out));
		assertFalse(stream.writeChunk(chunk("media", Map.of("url", "https://example.com/a.png")), out));

		assertEquals("", out.toString());
	}

	@Test
	void sendsAToolResultThatWasNotStreamedAfterStreamedThinking() throws Exception {
		OpenAIChatCompletionsStream stream = stream();
		StringWriter out = new StringWriter();
		stream.writeChunk(chunk("thinking", Map.of("thinking", "Look it up")), out);

		Map<String, Object> finalOutput = new HashMap<>();
		finalOutput.put("messageType", "TOOL");
		finalOutput.put("response",
				List.of(Map.of("id", "call_1", "type", "function", "name", "lookup", "arguments", "{\"q\":\"x\"}")));
		PixelRunner runner = mock(PixelRunner.class);
		when(runner.getResults()).thenReturn(List.of(new NounMetadata(finalOutput, PixelDataType.MAP)));
		PixelJobManager manager = mock(PixelJobManager.class);
		when(manager.getOutput(JOB)).thenReturn(runner);
		try (MockedStatic<PixelJobManager> managers = mockStatic(PixelJobManager.class)) {
			managers.when(PixelJobManager::getManager).thenReturn(manager);
			stream.writeJobCompleted(out);
		}

		String sent = out.toString();
		assertTrue(sent.contains("\"lookup\""), sent);
		assertTrue(sent.contains("\"tool_calls\""), sent);
		assertFalse(sent.contains("Look it up"), sent);
	}
}
