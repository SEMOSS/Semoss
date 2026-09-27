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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.MicrosoftTokenFiller;
import prerna.security.HttpHelperUtility;

// headers from Graph with the owner's delegated token, paged 100 at a time
final class BrainGraphHeaderSource implements BrainMailHeaderSource {

	private static final String BASE = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0";
	private static final String SELECT = "id,internetMessageId,conversationId,subject,from,toRecipients,ccRecipients,"
			+ "receivedDateTime,parentFolderId,sender,inferenceClassification";
	// Graph allows up to 1000 messages a page; headers only, so a page stays small
	private static final int PAGE = 500;

	@Override
	public Map<String, Object> me(User user) throws Exception {
		return get(user, BASE + "/me?$select=id,displayName,mail,userPrincipalName");
	}

	@Override
	public List<String> aliases(User user) {
		// "SMTP:primary@x" and "smtp:alias@x"; not every tenant returns them, so none is not an error
		List<String> out = new ArrayList<>();
		try {
			if (get(user, BASE + "/me?$select=proxyAddresses").get("proxyAddresses") instanceof List<?> list) {
				for (Object value : list) {
					String v = String.valueOf(value);
					if (v.regionMatches(true, 0, "smtp:", 0, 5)) {
						out.add(v.substring(5).trim().toLowerCase());
					}
				}
			}
		} catch (Exception e) {
			return out;
		}
		return out;
	}

	@Override
	public Map<String, Object> manager(User user) {
		// no manager, or no directory permission: not an error for onboarding
		try {
			return get(user, BASE + "/me/manager?$select=displayName,mail,userPrincipalName");
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> list(User user, String folder, Instant since, int max) throws Exception {
		List<Map<String, Object>> out = new ArrayList<>();
		String url = BASE + "/me/mailFolders/" + folder + "/messages?$select=" + SELECT + "&$top=" + PAGE
				+ "&$orderby=receivedDateTime%20desc&$filter=" + encode("receivedDateTime ge " + since);
		while (url != null && out.size() < max) {
			Map<String, Object> page = get(user, url);
			if (page.get("value") instanceof List<?> values) {
				for (Object value : values) {
					if (out.size() < max && value instanceof Map<?, ?> m) {
						out.add((Map<String, Object>) m);
					}
				}
			}
			url = (String) page.get("@odata.nextLink");
		}
		return out;
	}

	private static Map<String, Object> get(User user, String url) throws Exception {
		String token = MicrosoftLoginUtils.getValidAccessToken(user);
		return CollaborationDbUtils
				.parseMap(HttpHelperUtility.getRequest(url, MicrosoftLoginUtils.getBearerHeader(token), null, null, null));
	}

	private static String encode(String value) {
		return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
	}
}
