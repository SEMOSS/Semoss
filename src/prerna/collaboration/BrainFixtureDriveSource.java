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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveItemMapper;

// local testing: drive items from a drive.json next to the mail.json fixture instead of Graph;
// reread when the file changes so renames, moves and deletes can be tried without a restart
final class BrainFixtureDriveSource implements BrainDriveSource {

	private static final Map<String, BrainFixtureDriveSource> LOADED = new ConcurrentHashMap<>();

	private final Path file;
	private long modified = -1;
	private Map<String, Object> data;

	private BrainFixtureDriveSource(Path file) {
		this.file = file;
	}

	static BrainFixtureDriveSource of(String mailPath) {
		Path drive = Path.of(mailPath).resolveSibling("drive.json");
		return LOADED.computeIfAbsent(drive.toString(), key -> new BrainFixtureDriveSource(drive));
	}

	@Override
	public Map<String, Object> item(User user, String driveId, String itemId) {
		return MicrosoftOneDriveItemMapper.toDriveItem(raw(driveOrMine(driveId), itemId), null);
	}

	@Override
	public Map<String, Object> resolve(User user, String shareUrl) {
		Object shared = map(data().get("shares")).get(shareUrl);
		if (shared instanceof Map<?, ?> address) {
			return item(user, (String) address.get("driveId"), (String) address.get("itemId"));
		}
		for (Map<String, Object> item : list("items")) {
			if (shareUrl.equals(item.get("webUrl"))) {
				return item(user, driveOf(item), (String) item.get("id"));
			}
		}
		throw new Unavailable(false, "The Microsoft 365 file was not found");
	}

	@Override
	public List<Map<String, Object>> children(User user, String driveId, String folderId, int limit) {
		String drive = driveOrMine(driveId);
		String folder = folderId != null ? folderId : (String) drive(drive).get("rootId");
		if (folderId != null) {
			raw(drive, folderId);
		}
		List<Map<String, Object>> out = new ArrayList<>();
		for (Map<String, Object> item : list("items")) {
			if (drive.equals(driveOf(item)) && Objects.equals(folder, map(item.get("parentReference")).get("id"))
					&& !Boolean.TRUE.equals(item.get("forbidden")) && (limit <= 0 || out.size() < limit)) {
				out.add(MicrosoftOneDriveItemMapper.toDriveItem(item, null));
			}
		}
		return out;
	}

	@Override
	public List<Map<String, Object>> shared(User user, int limit) {
		List<Map<String, Object>> out = new ArrayList<>();
		for (Map<String, Object> item : list("items")) {
			if (Boolean.TRUE.equals(item.get("sharedWithMe")) && !Boolean.TRUE.equals(item.get("forbidden"))
					&& (limit <= 0 || out.size() < limit)) {
				out.add(MicrosoftOneDriveItemMapper.toDriveItem(item, null));
			}
		}
		return out;
	}

	@Override
	public List<Map<String, Object>> search(User user, String query, int limit) {
		String wanted = query.trim().toLowerCase(Locale.ROOT);
		List<Map<String, Object>> out = new ArrayList<>();
		for (Map<String, Object> item : list("items")) {
			String text = (item.get("name") + " " + Objects.toString(item.get("text"), "")).toLowerCase(Locale.ROOT);
			if (text.contains(wanted) && !Boolean.TRUE.equals(item.get("forbidden"))
					&& (limit <= 0 || out.size() < limit)) {
				out.add(MicrosoftOneDriveItemMapper.toDriveItem(item, null));
			}
		}
		return out;
	}

	@Override
	public List<Map<String, Object>> teams(User user) {
		return named(list("teams"));
	}

	@Override
	public List<Map<String, Object>> channels(User user, String teamId) {
		return named(team(teamId).get("channels") instanceof List<?> channels ? maps(channels) : List.of());
	}

	@Override
	public List<Map<String, Object>> sites(User user) {
		return named(list("sites"));
	}

	@Override
	public List<Map<String, Object>> libraries(User user, String siteId) {
		List<Map<String, Object>> out = new ArrayList<>();
		for (Map<String, Object> drive : list("drives")) {
			if (siteId.equals(drive.get("siteId"))) {
				out.add(drive);
			}
		}
		return named(out);
	}

	@Override
	public Map<String, Object> channelFolder(User user, String teamId, String channelId) {
		Object channels = team(teamId).get("channels");
		for (Map<String, Object> channel : channels instanceof List<?> list ? maps(list) : List.<Map<String, Object>>of()) {
			if (channelId.equals(channel.get("id"))) {
				Map<String, Object> out = new LinkedHashMap<>();
				out.put("driveId", channel.get("driveId"));
				out.put("itemId", channel.get("folderId"));
				return out;
			}
		}
		throw new Unavailable(false, "The Teams channel was not found");
	}

	@Override
	public Path download(User user, String driveId, String itemId, Path dir) throws Exception {
		Map<String, Object> item = raw(driveOrMine(driveId), itemId);
		Path base = file.getParent().toRealPath();
		Path content = base.resolve(String.valueOf(item.get("content"))).toRealPath();
		if (!content.startsWith(base) || !Files.isRegularFile(content)) {
			throw new IllegalArgumentException("The fixture has no content for this file");
		}
		return Files.copy(content, dir.resolve((String) item.get("name")));
	}

	private Map<String, Object> raw(String driveId, String itemId) {
		for (Map<String, Object> item : list("items")) {
			if (Objects.equals(itemId, item.get("id")) && driveId.equals(driveOf(item))) {
				if (Boolean.TRUE.equals(item.get("forbidden"))) {
					throw new Unavailable(true, "You do not have access to this Microsoft 365 file");
				}
				return item;
			}
		}
		throw new Unavailable(false, "The Microsoft 365 file was not found");
	}

	private String driveOrMine(String driveId) {
		if (driveId != null) {
			return driveId;
		}
		for (Map<String, Object> drive : list("drives")) {
			if (Boolean.TRUE.equals(drive.get("isMine"))) {
				return (String) drive.get("id");
			}
		}
		throw new IllegalStateException("drive.json has no drive with isMine true");
	}

	private Map<String, Object> drive(String driveId) {
		for (Map<String, Object> drive : list("drives")) {
			if (driveId.equals(drive.get("id"))) {
				return drive;
			}
		}
		throw new Unavailable(false, "The drive was not found");
	}

	private Map<String, Object> team(String teamId) {
		for (Map<String, Object> team : list("teams")) {
			if (teamId.equals(team.get("id"))) {
				return team;
			}
		}
		throw new Unavailable(false, "The team was not found");
	}

	private static String driveOf(Map<String, Object> item) {
		return (String) map(item.get("parentReference")).get("driveId");
	}

	private static List<Map<String, Object>> named(List<Map<String, Object>> rows) {
		List<Map<String, Object>> out = new ArrayList<>();
		for (Map<String, Object> row : rows) {
			Map<String, Object> entry = new LinkedHashMap<>();
			entry.put("id", row.get("id"));
			entry.put("name", row.get("displayName") != null ? row.get("displayName") : row.get("name"));
			if (row.get("webUrl") != null) {
				entry.put("webUrl", row.get("webUrl"));
			}
			out.add(entry);
		}
		return out;
	}

	private List<Map<String, Object>> list(String key) {
		return data().get(key) instanceof List<?> values ? maps(values) : List.of();
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> maps(List<?> values) {
		List<Map<String, Object>> out = new ArrayList<>();
		for (Object value : values) {
			if (value instanceof Map<?, ?>) {
				out.add((Map<String, Object>) value);
			}
		}
		return out;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> map(Object value) {
		return value instanceof Map<?, ?> ? (Map<String, Object>) value : Map.of();
	}

	private synchronized Map<String, Object> data() {
		try {
			long current = Files.getLastModifiedTime(file).toMillis();
			if (data == null || current != modified) {
				try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
					data = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create()
							.fromJson(reader, new TypeToken<Map<String, Object>>() {
							}.getType());
				}
				modified = current;
			}
			return data;
		} catch (Exception e) {
			throw new IllegalStateException("Cannot read " + file + " next to the " + BrainMessageSource.FIXTURE_SETTING
					+ " file", e);
		}
	}
}
