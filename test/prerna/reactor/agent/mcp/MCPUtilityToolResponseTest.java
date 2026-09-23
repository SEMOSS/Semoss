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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *******************************************************************************/
package prerna.reactor.agent.mcp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.engine.impl.model.message.ResponseMessage;

class MCPUtilityToolResponseTest {

	@Test
	@SuppressWarnings("unchecked")
	void enrichesExecutionFunctionAndMissingSchemaDefaults() {
		Map<String, Object> arguments = new HashMap<>();
		arguments.put("paramValues", Map.of("field_", "new search"));
		Map<String, Object> toolCall = new HashMap<>();
		toolCall.put("name", "projectprefix_play_search");
		toolCall.put("arguments", arguments);
		ResponseMessage response = ResponseMessage.toolResponse(toolCall);

		Map<String, Object> properties = new HashMap<>();
		properties.put("recording_file", Map.of("type", "string", "default", "search.json"));
		properties.put("project_id", Map.of("type", "string", "default", "project-1"));
		properties.put("start_url", Map.of("type", "string"));
		Map<String, Object> lookupTool = new HashMap<>();
		lookupTool.put("inputSchema", Map.of("type", "object", "properties", properties));
		lookupTool.put("_meta",
				Map.of(MCPUtility.SMSS_FUNCTION_NAME, "PlayPlaywrightSocketsRoomRecording"));

		MCPUtility.updateToolResponseWithProjectMeta(response, new HashMap<>(),
				Map.of("projectprefix_play_search", lookupTool));

		Map<String, Object> enriched = response.getToolResponses().get(0);
		assertEquals("PlayPlaywrightSocketsRoomRecording", enriched.get("original_name"));
		Map<String, Object> enrichedArguments = (Map<String, Object>) enriched.get("arguments");
		assertEquals("search.json", enrichedArguments.get("recording_file"));
		assertEquals("project-1", enrichedArguments.get("project_id"));
		assertEquals(Map.of("field_", "new search"), enrichedArguments.get("paramValues"));
		assertFalse(enrichedArguments.containsKey("start_url"));
	}
}
