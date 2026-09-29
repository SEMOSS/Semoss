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
package prerna.io.connector;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import prerna.auth.AuthProvider;
import prerna.util.SocialPropertiesUtil;

/**
 * Which connector apps each enabled sign in lets the connector reactors use,
 * judged from the scopes the sign in asks for in social.properties.
 * <p>
 * Callers only learn whether each app can work; the scopes themselves stay on
 * the server. An app can work when the sign in asks for at least one scope from
 * each of its groups. A Microsoft sign in that asks for {@code .default} takes
 * whatever the app registration grants, which the server cannot see, so every
 * Microsoft app counts as able to work.
 */
public final class ConnectorScopeAccess {

	/** The scope that stands for everything a Microsoft app registration grants. */
	private static final String DEFAULT_SCOPE = ".default";

	private static final String GRAPH_PREFIX = "https://graph.microsoft.com/";
	private static final String GOOGLE_AUTH_SEGMENT = "/auth/";
	private static final String GOOGLE_AUTH = "https://www.googleapis.com/auth/";

	/** The Drive scopes that can list the user's files. */
	private static final String[] DRIVE_LIST = { GOOGLE_AUTH + "drive.metadata.readonly",
			GOOGLE_AUTH + "drive.readonly", GOOGLE_AUTH + "drive", GOOGLE_AUTH + "drive.metadata" };

	/**
	 * A connector app, and the scopes its reactors read with: at least one scope
	 * from each group.
	 */
	private enum ConnectorApp {
		OUTLOOK(AuthProvider.MICROSOFT, "outlook", List.of(List.of("Mail.Read", "Mail.ReadWrite"))),
		MICROSOFT_CALENDAR(AuthProvider.MICROSOFT, "calendar",
				List.of(List.of("Calendars.Read", "Calendars.ReadWrite", "Calendars.Read.Shared",
						"Calendars.ReadWrite.Shared"))),
		ONEDRIVE(AuthProvider.MICROSOFT, "onedrive",
				List.of(List.of("Files.Read", "Files.ReadWrite", "Files.Read.All", "Files.ReadWrite.All"))),
		// teams, channels, and chats each have their own permission; any one of them
		// lets part of the app work
		TEAMS(AuthProvider.MICROSOFT, "teams",
				List.of(List.of("Team.ReadBasic.All", "Channel.ReadBasic.All", "ChannelMessage.Read.All", "Chat.Read",
						"Chat.ReadWrite"))),
		GMAIL(AuthProvider.GOOGLE, "gmail",
				List.of(List.of(GOOGLE_AUTH + "gmail.readonly", GOOGLE_AUTH + "gmail.modify",
						"https://mail.google.com/"))),
		GOOGLE_CALENDAR(AuthProvider.GOOGLE, "calendar",
				List.of(List.of(GOOGLE_AUTH + "calendar.readonly", GOOGLE_AUTH + "calendar",
						GOOGLE_AUTH + "calendar.events", GOOGLE_AUTH + "calendar.events.readonly"))),
		GOOGLE_DRIVE(AuthProvider.GOOGLE, "drive", List.of(List.of(DRIVE_LIST))),
		// documents are listed through Drive and read through the Docs API
		GOOGLE_DOCS(AuthProvider.GOOGLE, "docs",
				List.of(List.of(DRIVE_LIST), List.of(GOOGLE_AUTH + "documents.readonly", GOOGLE_AUTH + "documents",
						GOOGLE_AUTH + "drive.readonly", GOOGLE_AUTH + "drive")));

		private final AuthProvider provider;
		private final String key;
		private final List<List<String>> scopeGroups;

		ConnectorApp(AuthProvider provider, String key, List<List<String>> scopeGroups) {
			this.provider = provider;
			this.key = key;
			this.scopeGroups = scopeGroups;
		}

		/**
		 * @param requested the sign in's scopes, normalized
		 * @return whether they include a scope from every group
		 */
		private boolean isCoveredBy(Set<String> requested) {
			for (List<String> group : this.scopeGroups) {
				if (group.stream().map(ConnectorScopeAccess::normalize).noneMatch(requested::contains)) {
					return false;
				}
			}
			return true;
		}
	}

	private ConnectorScopeAccess() {

	}

	/**
	 * Which connector apps each enabled OAuth sign in can use, keyed by the
	 * provider's label (such as {@code MICROSOFT}) and then by app (such as
	 * {@code outlook}). Providers with no connector apps are left out.
	 *
	 * @return whether each app can work, by provider
	 */
	public static Map<String, Map<String, Boolean>> getConnectorAccess() {
		Map<String, AuthProvider> providersByKey = AuthProvider.getSocialPropKeysToEnum();
		Map<String, Map<String, Boolean>> access = new LinkedHashMap<>();
		for (Map<String, Object> entry : SocialPropertiesUtil.getInstance().getAvailableProviders()) {
			if (!Boolean.TRUE.equals(entry.get("isOauth"))) {
				continue;
			}
			String key = String.valueOf(entry.get("provider"));
			AuthProvider provider = providersByKey.get(key.toLowerCase(Locale.ROOT));
			if (provider == null || access.containsKey(provider.getLabel())) {
				continue;
			}

			Set<String> requested = null;
			Map<String, Boolean> apps = new LinkedHashMap<>();
			for (ConnectorApp app : ConnectorApp.values()) {
				if (app.provider != provider) {
					continue;
				}
				if (requested == null) {
					requested = getRequestedScopes(provider, key);
				}
				apps.put(app.key, requested.contains(DEFAULT_SCOPE) || app.isCoveredBy(requested));
			}
			if (!apps.isEmpty()) {
				access.put(provider.getLabel(), apps);
			}
		}
		return access;
	}

	/**
	 * Whether a provider's sign in asks for at least one of some scopes, for a
	 * feature that needs one of them, such as subscribing to a mailbox. A Microsoft
	 * sign in that asks for {@code .default} counts as asking for everything, since
	 * what it gets is the app registration's to say.
	 * <p>
	 * This is what the deployment asks for, not what the user's token was granted,
	 * so a true answer is necessary rather than sufficient: the provider has the
	 * last word.
	 *
	 * @param provider the provider
	 * @param scopes   the scopes, any one of which is enough, written the way the
	 *                 provider documents them
	 * @return whether the sign in asks for one of them
	 */
	public static boolean requestsAnyScope(AuthProvider provider, List<String> scopes) {
		Set<String> requested = getRequestedScopes(provider, provider.getSocialPrefix());
		if (requested.contains(DEFAULT_SCOPE)) {
			return true;
		}
		return scopes.stream().map(ConnectorScopeAccess::normalize).anyMatch(requested::contains);
	}

	/**
	 * The scopes a provider's sign in asks for, the same way the sign in reads
	 * them.
	 *
	 * @param provider the provider
	 * @param key      its key in social.properties, such as {@code ms}
	 * @return the scopes, normalized
	 */
	private static Set<String> getRequestedScopes(AuthProvider provider, String key) {
		String prefix = AuthProvider.getSocialPrefixForPath(key) + "_";
		IAccessTokenFiller filler = provider.newTokenFiller();
		String scope = filler instanceof AbstractOAuthTokenFiller
				? ((AbstractOAuthTokenFiller) filler).getRequestedScope(prefix)
				: SocialPropertiesUtil.getInstance().getProperty(prefix + "scope");

		Set<String> requested = new HashSet<>();
		if (scope == null) {
			return requested;
		}
		for (String value : scope.trim().split("[\\s,]+")) {
			if (!value.isEmpty()) {
				requested.add(normalize(value));
			}
		}
		return requested;
	}

	/**
	 * A scope in a short, comparable form, so {@code Mail.Read},
	 * {@code https://graph.microsoft.com/Mail.Read}, and {@code mail.read} match,
	 * as do {@code https://www.googleapis.com/auth/gmail.readonly} and
	 * {@code gmail.readonly}.
	 *
	 * @param scope a scope as a sign in writes it
	 * @return the short form, in lower case
	 */
	private static String normalize(String scope) {
		String normalized = scope.trim().toLowerCase(Locale.ROOT);
		while (normalized.endsWith("/")) {
			normalized = normalized.substring(0, normalized.length() - 1);
		}
		int authIndex = normalized.indexOf(GOOGLE_AUTH_SEGMENT);
		if (authIndex >= 0) {
			return normalized.substring(authIndex + GOOGLE_AUTH_SEGMENT.length());
		}
		if (normalized.startsWith(GRAPH_PREFIX)) {
			return normalized.substring(GRAPH_PREFIX.length());
		}
		// a scope that is a whole site, such as Gmail's https://mail.google.com/
		return normalized.replaceFirst("^https?://", "");
	}
}
