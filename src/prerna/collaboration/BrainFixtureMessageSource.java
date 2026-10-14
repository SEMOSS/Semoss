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
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;

import prerna.auth.User;

// local testing: serves messages from a brain-mail-v1 mail.json instead of Graph
final class BrainFixtureMessageSource implements BrainMessageSource {

	private static final Map<String, BrainFixtureMessageSource> LOADED = new ConcurrentHashMap<>();

	private final Map<String, Map<String, Object>> byId = new HashMap<>();
	// teams.json next to mail.json, by chat and message id
	private final Map<String, Map<String, Object>> chats = new HashMap<>();

	private BrainFixtureMessageSource(String path) {
		try (Reader reader = Files.newBufferedReader(Path.of(path), StandardCharsets.UTF_8)) {
			for (Object item : new Gson().fromJson(reader, List.class)) {
				@SuppressWarnings("unchecked")
				Map<String, Object> message = (Map<String, Object>) item;
				byId.put((String) message.get("id"), message);
			}
		} catch (Exception e) {
			throw new IllegalStateException("Cannot read the " + FIXTURE_SETTING + " file", e);
		}
		Path teams = Path.of(path).resolveSibling("teams.json");
		if (Files.exists(teams)) {
			try (Reader reader = Files.newBufferedReader(teams, StandardCharsets.UTF_8)) {
				for (Object item : new Gson().fromJson(reader, List.class)) {
					@SuppressWarnings("unchecked")
					Map<String, Object> message = (Map<String, Object>) item;
					chats.put(message.get("chatId") + "/" + message.get("id"), message);
				}
			} catch (Exception e) {
				throw new IllegalStateException("Cannot read " + teams, e);
			}
		}
	}

	static BrainFixtureMessageSource of(String path) {
		return LOADED.computeIfAbsent(path, BrainFixtureMessageSource::new);
	}

	@Override
	public Map<String, Object> fetch(User user, String source, String conversationId, String graphId) {
		if ("teams".equals(source) && graphId != null) {
			Map<String, Object> chat = chats.get(conversationId + "/" + graphId);
			return chat == null ? null : BrainGraphMessageSource.fromChat(chat);
		}
		return "email".equals(source) && graphId != null ? byId.get(graphId) : null;
	}

	// a mail.json message may carry "attachments" in Graph shape, with base64 contentBytes
	@Override
	public Map<String, List<Map<String, Object>>> attachments(User user, String source, List<String> graphIds) {
		Map<String, List<Map<String, Object>>> out = new LinkedHashMap<>();
		if (!"email".equals(source)) {
			return out;
		}
		for (String graphId : graphIds) {
			List<Map<String, Object>> described = new ArrayList<>();
			for (Map<String, Object> attachment : attached(graphId)) {
				Map<String, Object> entry = BrainAttachments.describe(attachment);
				if (!Boolean.TRUE.equals(entry.get("isInline"))) {
					described.add(entry);
				}
			}
			out.put(graphId, described);
		}
		return out;
	}

	@Override
	public Map<String, Object> attachment(User user, String source, String graphId, String attachmentId) {
		Map<String, Object> attachment = "email".equals(source) ? find(graphId, attachmentId) : null;
		if (attachment == null) {
			return null;
		}
		Map<String, Object> copy = new LinkedHashMap<>(attachment);
		copy.remove("contentBytes");
		return copy;
	}

	@Override
	public Path download(User user, String source, String graphId, String attachmentId, Path dir, String fileName)
			throws Exception {
		Map<String, Object> attachment = "email".equals(source) ? find(graphId, attachmentId) : null;
		if (attachment == null || !(attachment.get("contentBytes") instanceof String bytes)) {
			throw new IllegalArgumentException("The fixture has no bytes for this attachment");
		}
		Path file = dir.resolve(fileName);
		Files.write(file, Base64.getDecoder().decode(bytes));
		return file;
	}

	private Map<String, Object> find(String graphId, String attachmentId) {
		for (Map<String, Object> attachment : attached(graphId)) {
			if (attachmentId != null && attachmentId.equals(attachment.get("id"))) {
				return attachment;
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> attached(String graphId) {
		Map<String, Object> message = graphId == null ? null : byId.get(graphId);
		List<Map<String, Object>> out = new ArrayList<>();
		if (message != null && message.get("attachments") instanceof List<?> items) {
			for (Object item : items) {
				if (item instanceof Map<?, ?> attachment) {
					out.add((Map<String, Object>) attachment);
				}
			}
		}
		return out;
	}
}
