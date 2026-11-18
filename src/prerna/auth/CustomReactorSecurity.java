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
package prerna.auth;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.io.connector.ms.MicrosoftGraphUserLookup;
import prerna.util.Constants;

/**
 * What an app's custom reactors read about people and permissions. The classes
 * that work on the system databases are closed to project code, and this class
 * is the way in: it answers what the signed in user may do with an engine, a
 * project or an insight, says which rules an admin set, and finds people in the
 * organization. Every answer about a person is about the user the reactor runs
 * as, and nothing here changes a permission, a user or a setting. A custom
 * reactor that asks the security utilities the same questions directly still
 * runs, and the compile output names the method here to call instead.
 *
 * <p>
 * A people search looks in the Microsoft directory when the server uses it for
 * user searches, and in the security database otherwise, the same choice the
 * user search endpoint makes. Each person comes back as their id, login type,
 * name and email. A page is picked by limit and offset in either place, and a
 * directory search follows the directory's own pages on the server until the
 * page is full.
 */
public final class CustomReactorSecurity {

	/**
	 * The most people one search returns.
	 */
	public static final int MAX_LIMIT = 100;

	/**
	 * How far into the matches a search reaches: a page that starts at or past this
	 * many matches is empty.
	 */
	public static final int MAX_RESULTS = 1000;

	private static final Logger classLogger = LogManager.getLogger(CustomReactorSecurity.class);

	private static final String SEARCH_FAILED_MESSAGE = "Could not search your organization's directory. Try again.";

	/**
	 * The keys each person comes back with.
	 */
	private static final List<String> PERSON_KEYS = List.of(Constants.USER_MAP_ID, Constants.USER_MAP_TYPE,
			Constants.USER_MAP_NAME, Constants.USER_MAP_EMAIL);

	private CustomReactorSecurity() {
	}

	/**
	 * Whether the user can view an engine.
	 *
	 * @param user     the signed in user
	 * @param engineId the engine
	 * @return true when the user can view the engine; false with no signed in user
	 */
	public static boolean userCanViewEngine(User user, String engineId) {
		return isSignedIn(user) && SecurityEngineUtils.userCanViewEngine(user, engineId);
	}

	/**
	 * Whether the user can edit an engine.
	 *
	 * @param user     the signed in user
	 * @param engineId the engine
	 * @return true when the user can edit the engine; false with no signed in user
	 */
	public static boolean userCanEditEngine(User user, String engineId) {
		return isSignedIn(user) && SecurityEngineUtils.userCanEditEngine(user, engineId);
	}

	/**
	 * Whether the user owns an engine.
	 *
	 * @param user     the signed in user
	 * @param engineId the engine
	 * @return true when the user owns the engine; false with no signed in user
	 */
	public static boolean userIsEngineOwner(User user, String engineId) {
		return isSignedIn(user) && SecurityEngineUtils.userIsOwner(user, engineId);
	}

	/**
	 * Whether the user can view a project.
	 *
	 * @param user      the signed in user
	 * @param projectId the project
	 * @return true when the user can view the project; false with no signed in user
	 */
	public static boolean userCanViewProject(User user, String projectId) {
		return isSignedIn(user) && SecurityProjectUtils.userCanViewProject(user, projectId);
	}

	/**
	 * Whether the user can edit a project.
	 *
	 * @param user      the signed in user
	 * @param projectId the project
	 * @return true when the user can edit the project; false with no signed in user
	 */
	public static boolean userCanEditProject(User user, String projectId) {
		return isSignedIn(user) && SecurityProjectUtils.userCanEditProject(user, projectId);
	}

	/**
	 * Whether the user owns a project.
	 *
	 * @param user      the signed in user
	 * @param projectId the project
	 * @return true when the user owns the project; false with no signed in user
	 */
	public static boolean userIsProjectOwner(User user, String projectId) {
		return isSignedIn(user) && SecurityProjectUtils.userIsOwner(user, projectId);
	}

	/**
	 * Whether the user can view an insight.
	 *
	 * @param user      the signed in user
	 * @param projectId the project that holds the insight
	 * @param insightId the insight
	 * @return true when the user can view the insight; false with no signed in user
	 */
	public static boolean userCanViewInsight(User user, String projectId, String insightId) {
		return isSignedIn(user) && SecurityInsightUtils.userCanViewInsight(user, projectId, insightId);
	}

	/**
	 * Whether the user can edit an insight.
	 *
	 * @param user      the signed in user
	 * @param projectId the project that holds the insight
	 * @param insightId the insight
	 * @return true when the user can edit the insight; false with no signed in user
	 */
	public static boolean userCanEditInsight(User user, String projectId, String insightId) {
		return isSignedIn(user) && SecurityInsightUtils.userCanEditInsight(user, projectId, insightId);
	}

	/**
	 * Whether the user may export.
	 *
	 * @param user the signed in user
	 * @return true when the user may export; false with no signed in user
	 */
	public static boolean userIsExporter(User user) {
		return isSignedIn(user) && SecurityQueryUtils.userIsExporter(user);
	}

	/**
	 * Whether the user may publish.
	 *
	 * @param user the signed in user
	 * @return true when the user may publish; false with no signed in user
	 */
	public static boolean userIsPublisher(User user) {
		return isSignedIn(user) && SecurityQueryUtils.userIsPublisher(user);
	}

	/**
	 * Whether an admin kept exporting to the users they made exporters, so a user
	 * exports only when {@link #userIsExporter(User)} answers yes as well.
	 *
	 * @return true when only exporters may export
	 */
	public static boolean adminSetExporter() {
		return AbstractSecurityUtils.adminSetExporter();
	}

	/**
	 * Searches for people where the server searches by default: the Microsoft
	 * directory when the server uses it, otherwise the security database.
	 *
	 * @param user       the signed in user, whose Microsoft login searches the
	 *                   directory unless the server searches it with application
	 *                   credentials
	 * @param searchTerm text matched against name, email and id; blank matches
	 *                   everyone
	 * @param limit      the page size, up to {@link #MAX_LIMIT}; 0 or less for
	 *                   {@link #MAX_LIMIT}
	 * @param offset     how many matches come before the page
	 * @return the people on the page, each with {@code id}, {@code type},
	 *         {@code name} and {@code email}
	 * @throws IllegalAccessException when no user is signed in, or the directory
	 *                                search needs the user's Microsoft login and
	 *                                they are not signed in to Microsoft
	 */
	public static List<Map<String, Object>> searchUsers(User user, String searchTerm, int limit, int offset)
			throws IllegalAccessException {
		return searchUsers(user, searchTerm, limit, offset, true);
	}

	/**
	 * Searches for people in the Microsoft directory or in the security database.
	 *
	 * @param user         the signed in user, whose Microsoft login searches the
	 *                     directory unless the server searches it with application
	 *                     credentials
	 * @param searchTerm   text matched against name, email and id; blank matches
	 *                     everyone
	 * @param limit        the page size, up to {@link #MAX_LIMIT}; 0 or less for
	 *                     {@link #MAX_LIMIT}
	 * @param offset       how many matches come before the page
	 * @param useDirectory true to search the Microsoft directory when the server
	 *                     uses it, false to search the security database
	 * @return the people on the page, each with {@code id}, {@code type},
	 *         {@code name} and {@code email}
	 * @throws IllegalAccessException when no user is signed in, or the directory
	 *                                search needs the user's Microsoft login and
	 *                                they are not signed in to Microsoft
	 */
	public static List<Map<String, Object>> searchUsers(User user, String searchTerm, int limit, int offset,
			boolean useDirectory) throws IllegalAccessException {
		if (!isSignedIn(user)) {
			throw new IllegalAccessException("Sign in to search for people");
		}
		String term = searchTerm == null ? "" : searchTerm.trim();
		int pageSize = limit <= 0 ? MAX_LIMIT : Math.min(limit, MAX_LIMIT);
		int from = Math.max(offset, 0);
		if (from >= MAX_RESULTS) {
			return new ArrayList<>();
		}
		int to = Math.min(from + pageSize, MAX_RESULTS);

		List<Map<String, Object>> matches = useDirectory && MicrosoftGraphUserLookup.isEnabled()
				? searchDirectory(user, term, to)
				: SecurityQueryUtils.searchForUser(term);

		List<Map<String, Object>> page = new ArrayList<>();
		for (int i = from; i < Math.min(to, matches.size()); i++) {
			page.add(personOf(matches.get(i)));
		}
		return page;
	}

	/**
	 * Whether a user is signed in with at least one login.
	 *
	 * @param user the user, or null
	 * @return true when the user has a login
	 */
	private static boolean isSignedIn(User user) {
		return user != null && !user.getLogins().isEmpty();
	}

	/**
	 * The directory's matches, read page by page until there are enough or the
	 * directory has no more.
	 *
	 * @param user   the signed in user
	 * @param term   the search text
	 * @param needed how many matches the page needs, counting the ones before it
	 * @return the matches, in the SEMOSS user shape
	 * @throws IllegalAccessException when the search needs the user's Microsoft
	 *                                login and they are not signed in to Microsoft
	 */
	private static List<Map<String, Object>> searchDirectory(User user, String term, int needed)
			throws IllegalAccessException {
		List<Map<String, Object>> matches = new ArrayList<>();
		String nextLink = null;
		try {
			do {
				MicrosoftGraphUserLookup.UserPage directoryPage = MicrosoftGraphUserLookup.searchUsers(user, term,
						nextLink);
				matches.addAll(directoryPage.getUsers());
				nextLink = directoryPage.getNextLink();
			} while (nextLink != null && matches.size() < needed);
		} catch (IllegalAccessException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to search the Microsoft directory for {}", User.getSingleLogginName(user), e);
			throw new IllegalStateException(SEARCH_FAILED_MESSAGE);
		}
		return matches;
	}

	/**
	 * One match as a people search returns it.
	 *
	 * @param match a user in the SEMOSS user shape
	 * @return the id, type, name and email
	 */
	private static Map<String, Object> personOf(Map<String, Object> match) {
		Map<String, Object> person = new LinkedHashMap<>();
		for (String key : PERSON_KEYS) {
			person.put(key, match.get(key));
		}
		return person;
	}
}
