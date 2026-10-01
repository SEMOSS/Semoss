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
package prerna.reactor.agent.mcp;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MCPResponseBuilder {

	public static final String SEMOSS_MULTIMODAL_TOOL_RESPONSE_KEY = "SEMOSSMultimodalToolResponse";

	/**
	 * Creates a text content part for {@link #response(Map[])}.
	 *
	 * @param text response text
	 * @return immutable text content part
	 */
	public static Map<String, Object> textPart(String text) {
		if (text == null || text.isBlank()) {
			throw new IllegalArgumentException("MCP response text must not be blank");
		}
		return Map.of("type", "text", "text", text);
	}

	/**
	 * Creates an image content part containing paths relative to the active room.
	 * The files are resolved and validated when the message is serialized for model
	 * execution.
	 *
	 * @param roomRelativePaths one or more image paths beneath the active room
	 * @return immutable image content part
	 */
	public static Map<String, Object> imagePart(String... roomRelativePaths) {
		if (roomRelativePaths == null || roomRelativePaths.length == 0) {
			throw new IllegalArgumentException("MCP response image paths must not be empty");
		}

		List<String> paths = new ArrayList<>(roomRelativePaths.length);
		for (String path : roomRelativePaths) {
			if (path == null || path.isBlank()) {
				throw new IllegalArgumentException("MCP response image paths must not be blank");
			}
			if (Path.of(path).isAbsolute()) {
				throw new IllegalArgumentException("MCP response image paths must be relative to the active room");
			}
			paths.add(path);
		}

		return Map.of("type", "image", "image", List.copyOf(paths));
	}

	/**
	 * Creates an ordered multimodal result for a Pixel reactor exposed as an MCP
	 * tool.
	 *
	 * @param parts ordered text and image parts
	 * @return immutable {@code SEMOSSMultimodalToolResponse} envelope
	 */
	public static Map<String, Object> response(List<Map<String, Object>> parts) {
		if (parts == null || parts.isEmpty()) {
			throw new IllegalArgumentException("MCP response must contain at least one part");
		}

		List<Map<String, Object>> orderedParts = new ArrayList<>(parts.size());
		for (Map<String, Object> part : parts) {
			if (part == null) {
				throw new IllegalArgumentException("MCP response parts must not be null");
			}
			orderedParts.add(Map.copyOf(part));
		}

		return Map.of(SEMOSS_MULTIMODAL_TOOL_RESPONSE_KEY, List.copyOf(orderedParts));
	}
}
