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
package prerna.collaboration;

import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;

import prerna.auth.User;

// local testing: serves messages from a brain-mail-v1 mail.json instead of Graph
final class BrainFixtureMessageSource implements BrainMessageSource {

	private static final Map<String, BrainFixtureMessageSource> LOADED = new ConcurrentHashMap<>();

	private final Map<String, Map<String, Object>> byId = new HashMap<>();

	private BrainFixtureMessageSource(String path) {
		try (Reader reader = Files.newBufferedReader(Path.of(path), StandardCharsets.UTF_8)) {
			for (Object item : new Gson().fromJson(reader, List.class)) {
				@SuppressWarnings("unchecked")
				Map<String, Object> message = (Map<String, Object>) item;
				byId.put((String) message.get("id"), message);
			}
		} catch (Exception e) {
			throw new IllegalStateException("Cannot read the " + FIXTURE_ENV + " file", e);
		}
	}

	static BrainFixtureMessageSource of(String path) {
		return LOADED.computeIfAbsent(path, BrainFixtureMessageSource::new);
	}

	@Override
	public Map<String, Object> fetch(User user, String source, String conversationId, String graphId) {
		return "email".equals(source) && graphId != null ? byId.get(graphId) : null;
	}
}
