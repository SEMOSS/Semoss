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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.MicrosoftTokenFiller;
import prerna.io.connector.ms.onedrive.MicrosoftOneDriveHelper;
import prerna.io.connector.ms.teams.MicrosoftTeamsHelper;
import prerna.security.HttpHelperUtility;

// Microsoft Graph with the owner's delegated token, through the existing OneDrive and Teams helpers
final class BrainGraphDriveSource implements BrainDriveSource {

	private static final String GRAPH = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0";
	private static final Pattern STATUS = Pattern.compile(" returned HTTP (\\d{3})");

	@FunctionalInterface
	private interface Call<T> {
		T run(String token) throws Exception;
	}

	@Override
	public Map<String, Object> item(User user, String driveId, String itemId) throws Exception {
		return graph(user, token -> MicrosoftOneDriveHelper.getItem(token, driveId, itemId, null));
	}

	@Override
	public Map<String, Object> resolve(User user, String shareUrl) throws Exception {
		return graph(user, token -> MicrosoftOneDriveHelper.resolveSharingLink(token, shareUrl));
	}

	@Override
	public List<Map<String, Object>> children(User user, String driveId, String folderId, int limit) throws Exception {
		return graph(user, token -> MicrosoftOneDriveHelper.listFiles(token, driveId, folderId, null, limit));
	}

	@Override
	public List<Map<String, Object>> shared(User user, int limit) throws Exception {
		return graph(user, token -> MicrosoftOneDriveHelper.listSharedFiles(token, null, limit));
	}

	@Override
	public List<Map<String, Object>> search(User user, String query, int limit) throws Exception {
		return graph(user, token -> MicrosoftOneDriveHelper.searchEverywhere(token, query, limit));
	}

	@Override
	public List<Map<String, Object>> teams(User user) throws Exception {
		return named(graph(user, token -> MicrosoftTeamsHelper.listTeams(token, 0)), "displayName");
	}

	@Override
	public List<Map<String, Object>> channels(User user, String teamId) throws Exception {
		return named(graph(user, token -> MicrosoftTeamsHelper.listChannels(token, teamId, 0)), "displayName");
	}

	@Override
	public List<Map<String, Object>> sites(User user) throws Exception {
		return named(values(user, GRAPH + "/me/followedSites?$select=id,displayName,webUrl"), "displayName");
	}

	@Override
	public List<Map<String, Object>> libraries(User user, String siteId) throws Exception {
		return named(values(user, GRAPH + "/sites/" + URLEncoder.encode(siteId, StandardCharsets.UTF_8)
				+ "/drives?$select=id,name,webUrl"), "name");
	}

	@Override
	public Map<String, Object> channelFolder(User user, String teamId, String channelId) throws Exception {
		Map<String, Object> folder = graph(user,
				token -> MicrosoftTeamsHelper.getChannelFilesFolder(token, teamId, channelId));
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("driveId", folder.get("driveId"));
		out.put("itemId", folder.get("folderId"));
		return out;
	}

	@Override
	public Path download(User user, String driveId, String itemId, Path dir) throws Exception {
		Map<String, Object> written = graph(user, token -> MicrosoftOneDriveHelper.downloadFile(token, driveId,
				itemId, null, null, dir.toString(), null));
		return dir.resolve(String.valueOf(written.get("filePath")));
	}

	private static List<Map<String, Object>> values(User user, String url) throws Exception {
		return graph(user, token -> {
			Map<String, Object> page = new Gson().fromJson(
					HttpHelperUtility.getRequest(url, MicrosoftLoginUtils.getBearerHeader(token), null, null, null),
					new TypeToken<Map<String, Object>>() {
					}.getType());
			Object value = page == null ? null : page.get("value");
			List<Map<String, Object>> out = new ArrayList<>();
			if (value instanceof List<?> list) {
				for (Object entry : list) {
					if (entry instanceof Map<?, ?> map) {
						@SuppressWarnings("unchecked")
						Map<String, Object> typed = (Map<String, Object>) map;
						out.add(typed);
					}
				}
			}
			return out;
		});
	}

	private static List<Map<String, Object>> named(List<Map<String, Object>> rows, String nameKey) {
		List<Map<String, Object>> out = new ArrayList<>();
		for (Map<String, Object> row : rows) {
			Map<String, Object> entry = new LinkedHashMap<>();
			entry.put("id", row.get("id"));
			entry.put("name", row.get(nameKey));
			if (row.get("webUrl") != null) {
				entry.put("webUrl", row.get("webUrl"));
			}
			out.add(entry);
		}
		return out;
	}

	// a 404 or 403 from Graph becomes Unavailable; a missing Microsoft login stays a login error
	private static <T> T graph(User user, Call<T> call) throws Exception {
		String token = MicrosoftLoginUtils.getValidAccessToken(user);
		try {
			return call.run(token);
		} catch (IllegalArgumentException e) {
			Matcher status = STATUS.matcher(String.valueOf(e.getMessage()));
			if (status.find()) {
				String code = status.group(1);
				if ("404".equals(code)) {
					throw new Unavailable(false, "The Microsoft 365 file was not found");
				}
				if ("403".equals(code)) {
					throw new Unavailable(true, "You do not have access to this Microsoft 365 file");
				}
			}
			// the raw message carries Graph urls and response bodies, which stay in the server log
			throw new IllegalStateException("Microsoft 365 request failed", e);
		}
	}
}
