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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;

import prerna.auth.User;

// local testing: headers from a brain-mail-v1 mail.json; the mailbox owner is whoever sent the Sent items
final class BrainFixtureHeaderSource implements BrainMailHeaderSource {

	private static final Map<String, BrainFixtureHeaderSource> LOADED = new ConcurrentHashMap<>();

	private final List<Map<String, Object>> messages = new ArrayList<>();

	@SuppressWarnings("unchecked")
	private BrainFixtureHeaderSource(String path) {
		try (Reader reader = Files.newBufferedReader(Path.of(path), StandardCharsets.UTF_8)) {
			for (Object item : new Gson().fromJson(reader, List.class)) {
				Map<String, Object> header = new LinkedHashMap<>((Map<String, Object>) item);
				header.remove("body");
				header.remove("uniqueBody");
				messages.add(header);
			}
		} catch (Exception e) {
			throw new IllegalStateException("Cannot read the " + BrainMessageSource.FIXTURE_ENV + " file", e);
		}
		messages.sort(Comparator.comparing((Map<String, Object> m) -> (String) m.get("receivedDateTime")).reversed());
	}

	static BrainFixtureHeaderSource of(String path) {
		return LOADED.computeIfAbsent(path, BrainFixtureHeaderSource::new);
	}

	@Override
	@SuppressWarnings("unchecked")
	public Map<String, Object> me(User user) {
		for (Map<String, Object> m : messages) {
			if (inFolder(m, SENT) && m.get("from") instanceof Map<?, ?> from
					&& from.get("emailAddress") instanceof Map<?, ?> address) {
				Map<String, Object> me = new LinkedHashMap<>();
				me.put("id", "fixture-owner");
				me.put("displayName", ((Map<String, Object>) address).get("name"));
				me.put("mail", ((Map<String, Object>) address).get("address"));
				return me;
			}
		}
		return Map.of();
	}

	@Override
	public Map<String, Object> manager(User user) {
		return null;
	}

	@Override
	public List<Map<String, Object>> list(User user, String folder, Instant since, int max) {
		List<Map<String, Object>> out = new ArrayList<>();
		for (Map<String, Object> m : messages) {
			if (out.size() < max && inFolder(m, folder)
					&& Instant.parse((String) m.get("receivedDateTime")).compareTo(since) >= 0) {
				out.add(m);
			}
		}
		return out;
	}

	// fixture folder ids end in the folder name, for example AAMkAD-fld-sentitems
	private static boolean inFolder(Map<String, Object> m, String folder) {
		return String.valueOf(m.get("parentFolderId")).endsWith("-" + folder);
	}
}
