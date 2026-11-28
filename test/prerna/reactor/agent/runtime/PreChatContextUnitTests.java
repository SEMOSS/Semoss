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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

class PreChatContextUnitTests {

	@Test
	void specNeedsToolboxAndTool() {
		assertNull(PreChatContext.Spec.of(null, "GetRoomResources", null, null));
		assertNull(PreChatContext.Spec.of("ecu", "  ", null, null));
		PreChatContext.Spec spec = PreChatContext.Spec.of(" ecu ", " GetRoomResources ", null, null);
		assertEquals(List.of("ecu"), spec.toolboxIds());
		assertEquals("GetRoomResources", spec.tool());
		assertTrue(spec.args().isEmpty());
		assertEquals(PreChatContext.DEFAULT_MAX_CHARS, spec.maxChars());
		assertEquals(PreChatContext.DEFAULT_MAX_CHARS, PreChatContext.Spec.of("e", "t", null, -5).maxChars());
		assertEquals(10, PreChatContext.Spec.of("e", "t", Map.of("a", 1), 10).maxChars());
		assertEquals(List.of("a", "b"),
				PreChatContext.Spec.anyOf(java.util.Arrays.asList(" a ", null, "", "b", "a"), "t", null, null).toolboxIds());
		assertNull(PreChatContext.Spec.anyOf(List.of(), "t", null, null));
	}

	@Test
	void parsesArgsAndLimits() {
		assertEquals(Map.of("limit", 5), PreChatContext.parseArgs("{\"limit\": 5}"));
		assertTrue(PreChatContext.parseArgs("not json").isEmpty());
		assertTrue(PreChatContext.parseArgs(" ").isEmpty());
		assertEquals(42, PreChatContext.parseInt(" 42 "));
		assertNull(PreChatContext.parseInt("lots"));
	}

	@Test
	void noteWrapsAndTruncates() {
		String whole = PreChatContext.note("GetRoomResources", "{\"room_files\":[]}", 100);
		assertTrue(whole.startsWith("<pre_chat_context tool=\"GetRoomResources\">\n{\"room_files\":[]}\n</pre_chat_context>"));
		assertTrue(whole.contains("Don't call GetRoomResources again"));
		assertFalse(whole.contains("truncated"));

		String cut = PreChatContext.note("T<x>", "abcdefghij", 4);
		assertTrue(cut.startsWith("<pre_chat_context tool=\"T&lt;x&gt;\">\nabcd\n[truncated: 6 more characters]"));
		assertTrue(cut.contains("The result was truncated"));
	}

	@Test
	void stringifiesToolOutput() {
		assertEquals("text", PreChatContext.stringify("text"));
		assertEquals("ok", PreChatContext.stringify(new NounMetadata("ok", PixelDataType.CONST_STRING)));
		assertEquals("[1,2]", PreChatContext.stringify(List.of(1, 2)));
		assertNull(PreChatContext.stringify(null));
	}
}
