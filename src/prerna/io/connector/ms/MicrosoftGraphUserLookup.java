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
package prerna.io.connector.ms;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.util.Constants;
import prerna.util.SocialPropertiesUtil;
import prerna.util.ValueUtils;

/**
 * Looks up users in the Microsoft Graph directory on behalf of the user search
 * and sharing endpoints.
 *
 * <p>
 * {@code ms_graphapi_lookup} in the social properties makes the directory
 * available and the default place a user search looks. A caller can still ask
 * for the security database instead through the {@link #LOOKUP_PARAM} request
 * parameter. Results come back in the SEMOSS user shape ({@code id},
 * {@code name}, {@code email}, {@code username}, {@code type}), remapped by
 * {@code ms_graphapi_jsonPattern} when that is set. A user picked from the
 * directory is added to the security database as a Microsoft user, which their
 * first Microsoft login matches on the Graph id. Their name, email and username
 * are read from the directory when they are added, never taken from the
 * request.
 * </p>
 */
public class MicrosoftGraphUserLookup {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftGraphUserLookup.class);

	private static final Gson GSON = new Gson();

	/** Social property that makes the directory available and the default. */
	public static final String LOOKUP_ENABLED_PROPERTY = "ms_graphapi_lookup";
	/** Social property naming a Graph group that scopes every search. */
	public static final String GROUP_ID_PROPERTY = "ms_graphapi_groupId";
	/**
	 * Social property that searches with the application credentials instead of the
	 * signed in user's Microsoft login.
	 */
	public static final String APPLICATION_CREDENTIALS_PROPERTY = "ms_graphapi_application_credentials";
	/**
	 * Social property holding a JSON map of SEMOSS user key to Graph user key.
	 */
	public static final String JSON_PATTERN_PROPERTY = "ms_graphapi_jsonPattern";

	/**
	 * Request parameter choosing where a user search looks: {@code true} for the
	 * directory, {@code false} for the security database. A request without it uses
	 * the directory whenever the directory is available.
	 */
	public static final String LOOKUP_PARAM = "msGraphLookup";

	private static final String NEXT_LINK_KEY = "@odata.nextLink";

	/**
	 * The Graph user properties a lookup by id returns when no field mapping is set
	 */
	private static final List<String> DEFAULT_SELECT = List.of(Constants.MS_GRAPH_ID, Constants.MS_GRAPH_DISPLAY_NAME,
			Constants.MS_GRAPH_EMAIL, Constants.MS_GRAPH_USER_PRINCIPAL_NAME);

	private MicrosoftGraphUserLookup() {

	}

	/**
	 * One page of directory users.
	 */
	public static class UserPage {
		private final List<Map<String, Object>> users;
		private final List<Map<String, Object>> graphUsers;
		private final String nextLink;

		public UserPage(List<Map<String, Object>> users, List<Map<String, Object>> graphUsers, String nextLink) {
			this.users = users;
			this.graphUsers = graphUsers;
			this.nextLink = nextLink;
		}

		/**
		 * @return the users on this page, in the SEMOSS user shape
		 */
		public List<Map<String, Object>> getUsers() {
			return users;
		}

		/**
		 * @return the users on this page as Graph returns them, in the same order as
		 *         {@link #getUsers()}
		 */
		public List<Map<String, Object>> getGraphUsers() {
			return graphUsers;
		}

		/**
		 * @return the link to the next page, or null when this is the last page
		 */
		public String getNextLink() {
			return nextLink;
		}
	}

	/**
	 * @return whether the directory is available to user searches
	 */
	public static boolean isEnabled() {
		return Boolean.parseBoolean("" + SocialPropertiesUtil.getInstance().getProperty(LOOKUP_ENABLED_PROPERTY));
	}

	/**
	 * Decides where a user search looks.
	 *
	 * @param requested the {@link #LOOKUP_PARAM} value sent with the request, or
	 *                  null when it was not sent
	 * @return true to search the directory, false to search the security database.
	 *         The directory is only searched when it is available, so a request for
	 *         it on a server without it searches the security database.
	 */
	public static boolean useDirectory(String requested) {
		return isEnabled() && ValueUtils.parseBoolean(requested, true);
	}

	/**
	 * @return whether searches use the application credentials rather than the
	 *         signed in user's Microsoft login
	 */
	public static boolean usesApplicationCredentials() {
		return Boolean
				.parseBoolean("" + SocialPropertiesUtil.getInstance().getProperty(APPLICATION_CREDENTIALS_PROPERTY));
	}

	/**
	 * @return the Graph group that scopes searches, or null to search the whole
	 *         tenant
	 */
	public static String getGroupId() {
		return ValueUtils.trimToNull(SocialPropertiesUtil.getInstance().getProperty(GROUP_ID_PROPERTY));
	}

	/**
	 * Fetches one page of directory users.
	 *
	 * @param user       the signed in user. Their Microsoft login is used unless
	 *                   application credentials are configured, and a refreshed
	 *                   login is saved back onto them.
	 * @param searchTerm text matched against the display name, mail and user
	 *                   principal name; blank lists everyone
	 * @param nextLink   the link from the previous page, or null for the first page
	 * @return the page
	 * @throws IllegalAccessException when searches need the user's Microsoft login
	 *                                and they are not signed in to Microsoft
	 * @throws Exception              when the Graph call fails
	 */
	public static UserPage searchUsers(User user, String searchTerm, String nextLink) throws Exception {
		AccessToken delegatedToken = getDelegatedToken(user);
		MicrosoftGraphUserSearchClient.GraphApiResponse response = new MicrosoftGraphUserSearchClient()
				.getUserDetails(delegatedToken, getGroupId(), searchTerm, nextLink);
		keepRefreshedToken(user, delegatedToken, response);

		JSONObject body = new JSONObject(response.getResponseBody());
		JSONArray values = body.optJSONArray(Constants.MS_GRAPH_VALUE);
		List<Map<String, Object>> graphUsers = Collections.emptyList();
		if (values != null) {
			graphUsers = GSON.fromJson(values.toString(), new TypeToken<List<Map<String, Object>>>() {
			}.getType());
		}

		Map<String, String> fieldMapping = getFieldMapping();
		List<Map<String, Object>> users = new ArrayList<>(graphUsers.size());
		for (Map<String, Object> graphUser : graphUsers) {
			users.add(toUserMap(graphUser, fieldMapping));
		}
		return new UserPage(users, graphUsers, ValueUtils.trimToNull(body.optString(NEXT_LINK_KEY, null)));
	}

	/**
	 * Looks a directory user up by their id, so what is stored for them comes from
	 * the directory rather than from the request.
	 *
	 * @param user   the signed in user. Their Microsoft login is used unless
	 *               application credentials are configured, and a refreshed login
	 *               is saved back onto them.
	 * @param userId the user's id in the SEMOSS user shape: their Graph id, or the
	 *               Graph property {@code ms_graphapi_jsonPattern} maps the id to
	 * @return the user in the SEMOSS user shape, or null when the directory has no
	 *         user with exactly that id
	 * @throws IllegalAccessException when lookups need the user's Microsoft login
	 *                                and they are not signed in to Microsoft
	 * @throws Exception              when the Graph call fails
	 */
	public static Map<String, Object> findDirectoryUser(User user, String userId) throws Exception {
		AccessToken delegatedToken = getDelegatedToken(user);
		Map<String, String> fieldMapping = getFieldMapping();
		String idProperty = Constants.MS_GRAPH_ID;
		Set<String> select = new LinkedHashSet<>(DEFAULT_SELECT);
		if (fieldMapping != null && !fieldMapping.isEmpty()) {
			idProperty = fieldMapping.getOrDefault(Constants.USER_MAP_ID, Constants.MS_GRAPH_ID);
			select.addAll(fieldMapping.values());
		}

		MicrosoftGraphUserSearchClient.GraphApiResponse response = new MicrosoftGraphUserSearchClient()
				.findUser(delegatedToken, getGroupId(), idProperty, userId, select);
		keepRefreshedToken(user, delegatedToken, response);

		JSONArray values = new JSONObject(response.getResponseBody()).optJSONArray(Constants.MS_GRAPH_VALUE);
		if (values == null || values.length() == 0) {
			return null;
		}
		Map<String, Object> graphUser = GSON.fromJson(values.getJSONObject(0).toString(),
				new TypeToken<Map<String, Object>>() {
				}.getType());
		Map<String, Object> directoryUser = toUserMap(graphUser, fieldMapping);
		// Graph compares strings without case, but the stored id must be exact
		return userId.equals(ValueUtils.trimToNull(directoryUser.get(Constants.USER_MAP_ID))) ? directoryUser : null;
	}

	/**
	 * @return the user's Microsoft login for a directory call, or null when the
	 *         calls use the application credentials
	 * @throws IllegalAccessException when the calls need the user's Microsoft login
	 *                                and they are not signed in to Microsoft
	 */
	private static AccessToken getDelegatedToken(User user) throws IllegalAccessException {
		if (usesApplicationCredentials()) {
			return null;
		}
		AccessToken delegatedToken = user == null ? null : user.getAccessToken(AuthProvider.MICROSOFT);
		if (delegatedToken == null) {
			throw new IllegalAccessException("Sign in with Microsoft to use your organization's directory");
		}
		return delegatedToken;
	}

	/**
	 * Saves a delegated login the Graph client refreshed back onto the user.
	 */
	private static void keepRefreshedToken(User user, AccessToken delegatedToken,
			MicrosoftGraphUserSearchClient.GraphApiResponse response) {
		if (delegatedToken != null && response.getAccessToken() != null) {
			user.setAccessToken(response.getAccessToken());
		}
	}

	/**
	 * Converts a Graph user to the SEMOSS user shape.
	 *
	 * @param graphUser    the user as Graph returns it
	 * @param fieldMapping SEMOSS user key to Graph user key, or null or empty for
	 *                     the default mapping
	 * @return the user, with {@code type} set to the Microsoft provider
	 */
	public static Map<String, Object> toUserMap(Map<String, Object> graphUser, Map<String, String> fieldMapping) {
		Map<String, Object> userMap = new HashMap<>();
		if (fieldMapping != null && !fieldMapping.isEmpty()) {
			fieldMapping.forEach((userKey, graphKey) -> userMap.put(userKey, graphUser.get(graphKey)));
		} else {
			userMap.put(Constants.USER_MAP_ID, graphUser.get(Constants.MS_GRAPH_ID));
			userMap.put(Constants.USER_MAP_NAME, graphUser.get(Constants.MS_GRAPH_DISPLAY_NAME));
			userMap.put(Constants.USER_MAP_EMAIL, graphUser.get(Constants.MS_GRAPH_EMAIL));
			userMap.put(Constants.USER_MAP_USERNAME, graphUser.get(Constants.MS_GRAPH_USER_PRINCIPAL_NAME));
		}
		userMap.put(Constants.USER_MAP_TYPE, AuthProvider.MICROSOFT.name());
		return userMap;
	}

	/**
	 * @return the {@code ms_graphapi_jsonPattern} mapping, or null when it is not
	 *         set
	 */
	public static Map<String, String> getFieldMapping() {
		String jsonPattern = ValueUtils
				.trimToNull(SocialPropertiesUtil.getInstance().getProperty(JSON_PATTERN_PROPERTY));
		if (jsonPattern == null) {
			return null;
		}
		return GSON.fromJson(jsonPattern, new TypeToken<Map<String, String>>() {
		}.getType());
	}

	/**
	 * Finds which users already have an account. A user has one when the security
	 * database holds their id, or holds their email as an id or an email (an admin
	 * can add a user under their email before they first sign in).
	 *
	 * @param users users in the SEMOSS user shape
	 * @return the ids of the users that already have an account
	 */
	public static Set<String> findExistingUserIds(List<Map<String, Object>> users) {
		Set<String> ids = new HashSet<>();
		Set<String> emails = new HashSet<>();
		for (Map<String, Object> user : users) {
			String id = ValueUtils.trimToNull(user.get(Constants.USER_MAP_ID));
			if (id != null) {
				ids.add(id);
			}
			String email = normalizeEmail(user.get(Constants.USER_MAP_EMAIL));
			if (email != null) {
				emails.add(email);
			}
		}

		Set<String> existingIds = new HashSet<>();
		if (ids.isEmpty() && emails.isEmpty()) {
			return existingIds;
		}
		Set<String> matched = SecurityQueryUtils.findExistingUserIdsAndEmails(ids, emails);
		for (Map<String, Object> user : users) {
			String id = ValueUtils.trimToNull(user.get(Constants.USER_MAP_ID));
			String email = normalizeEmail(user.get(Constants.USER_MAP_EMAIL));
			if (id != null && (matched.contains(id) || (email != null && matched.contains(email)))) {
				existingIds.add(id);
			}
		}
		return existingIds;
	}

	/**
	 * Adds the users in a permission request that are not in the security database
	 * yet, as Microsoft users, so the permissions can be granted to them. Each
	 * entry carries {@code userid}, the way the add permission endpoints receive
	 * it; the users' details are read from the directory.
	 *
	 * @param user            the signed in user making the request
	 * @param userPermissions the permission request entries
	 * @return the number of users added, including users who were waiting under an
	 *         admin added row keyed by their email
	 * @throws IllegalArgumentException when a user is in neither the security
	 *                                  database nor the directory
	 * @throws IllegalAccessException   when lookups need the user's Microsoft login
	 *                                  and they are not signed in to Microsoft
	 */
	public static int addMissingUsers(User user, List<? extends Map<String, ?>> userPermissions)
			throws IllegalAccessException {
		if (userPermissions == null) {
			return 0;
		}
		int added = 0;
		for (Map<String, ?> entry : userPermissions) {
			if (addMissingUser(user, ValueUtils.trimToNull(entry.get(Constants.MAP_USERID)))) {
				added++;
			}
		}
		return added;
	}

	/**
	 * Adds a directory user to the security database as a Microsoft user when they
	 * are not in it yet, with the name, email and username the directory holds for
	 * them.
	 *
	 * @param user   the signed in user adding them
	 * @param userId the directory user's id
	 * @return true when the user was added, including a user who was waiting under
	 *         an admin added row keyed by their email; false when they were already
	 *         in the security database
	 * @throws IllegalArgumentException when the directory has no user with that id,
	 *                                  the lookup fails, or the user could not be
	 *                                  added
	 * @throws IllegalAccessException   when lookups need the user's Microsoft login
	 *                                  and they are not signed in to Microsoft
	 */
	public static boolean addMissingUser(User user, String userId) throws IllegalAccessException {
		String id = ValueUtils.trimToNull(userId);
		if (id == null || SecurityQueryUtils.checkUserExist(id)) {
			return false;
		}

		Map<String, Object> directoryUser;
		try {
			directoryUser = findDirectoryUser(user, id);
		} catch (IllegalAccessException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to look up directory user {}", id, e);
			throw new IllegalArgumentException(
					"Could not look up user " + id + " in your organization's directory. Try again.");
		}
		if (directoryUser == null) {
			throw new IllegalArgumentException("User " + id + " is not in your organization's directory");
		}

		AccessToken token = new AccessToken();
		token.setId(id);
		token.setName(ValueUtils.trimToNull(directoryUser.get(Constants.USER_MAP_NAME)));
		token.setEmail(ValueUtils.trimToNull(directoryUser.get(Constants.USER_MAP_EMAIL)));
		token.setUsername(ValueUtils.trimToNull(directoryUser.get(Constants.USER_MAP_USERNAME)));
		token.setProvider(AuthProvider.MICROSOFT);
		// addOAuthUser returns false when it adopts a row an admin added under
		// the user's email, so check the outcome rather than the return value
		SecurityUpdateUtils.addOAuthUser(token);
		if (SecurityQueryUtils.checkUserExist(id)) {
			return true;
		}
		classLogger.warn("Could not add directory user {} to the security database", id);
		throw new IllegalArgumentException("Could not add user " + id + " from your organization's directory");
	}

	private static String normalizeEmail(Object email) {
		String trimmed = ValueUtils.trimToNull(email);
		return trimmed == null ? null : trimmed.toLowerCase(Locale.ROOT);
	}
}
