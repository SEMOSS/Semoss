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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;

/** Private, versioned provider state. Display parts cannot reproduce signed steps. */
public final class GoogleInteractionsState {

	public static final String KEY = "google_interactions";
	public static final int MAX_BYTES = 64 * 1024 * 1024;
	private static final Gson GSON = new Gson();

	private GoogleInteractionsState() {
	}

	public static Map<String, Object> contextBoundary(Map<String, Object> priorState, String summary) {
		validate(priorState);
		return Map.of("version", 1, "model", priorState.get("model"), "context_boundary", true,
				"steps", List.of(Map.of("type", "model_output", "content", List.of(Map.of("type", "text", "text", summary)))));
	}

	@SuppressWarnings("unchecked")
	public static Map<String, Object> validate(Object value) {
		if (value == null) {
			return null;
		}
		if (!(value instanceof Map<?, ?> map) || !(map.get("version") instanceof Number version)
				|| version.doubleValue() != 1 || !(map.get("model") instanceof String model) || model.isBlank()
				|| !(map.get("steps") instanceof List<?> steps) || steps.size() > 4096) {
			throw new IllegalArgumentException("Invalid Google Interactions replay state");
		}
		for (Object item : steps) {
			if (!(item instanceof Map<?, ?> step) || !(step.get("type") instanceof String type)
					|| type.isBlank() || "user_input".equals(type)) {
				throw new IllegalArgumentException("Invalid Google Interactions output step");
			}
		}
		if (GSON.toJson(value).getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) {
			throw new IllegalArgumentException("Google Interactions replay exceeds 64 MiB per message");
		}
		return (Map<String, Object>) value;
	}
}
