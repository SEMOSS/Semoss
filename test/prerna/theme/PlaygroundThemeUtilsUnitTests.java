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
package prerna.theme;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.gson.JsonParser;

import prerna.reactor.agent.runtime.PreChatContext;

class PlaygroundThemeUtilsUnitTests {

	@Test
	void readsPreChatTool() {
		PreChatContext.Spec spec = PlaygroundThemeUtils.parsePreChatTool(JsonParser.parseString(
				"{\"toolboxId\":\"ecu\",\"tool\":\"GetRoomResources\",\"args\":{\"mode\":\"brief\"},\"maxChars\":500}"));
		assertEquals("ecu", spec.toolboxId());
		assertEquals("GetRoomResources", spec.tool());
		assertEquals(Map.of("mode", "brief"), spec.args());
		assertEquals(500, spec.maxChars());
	}

	@Test
	void ignoresIncompleteOrMalformedPreChatTool() {
		assertNull(PlaygroundThemeUtils.parsePreChatTool(null));
		assertNull(PlaygroundThemeUtils.parsePreChatTool(JsonParser.parseString("\"GetRoomResources\"")));
		assertNull(PlaygroundThemeUtils.parsePreChatTool(JsonParser.parseString("{\"tool\":\"GetRoomResources\"}")));
		PreChatContext.Spec spec = PlaygroundThemeUtils.parsePreChatTool(JsonParser.parseString(
				"{\"toolboxId\":\"ecu\",\"tool\":\"T\",\"args\":[1],\"maxChars\":\"big\"}"));
		assertEquals(Map.of(), spec.args());
		assertEquals(PreChatContext.DEFAULT_MAX_CHARS, spec.maxChars());
	}
}
