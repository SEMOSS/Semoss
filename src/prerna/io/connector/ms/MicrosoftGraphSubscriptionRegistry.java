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

import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityExternalConnectorsUtils;
import prerna.io.connector.AbstractOAuthTokenFiller;

/**
 * What this deployment knows about the Microsoft Graph subscriptions it
 * created, held in the security database.
 *
 * <p>
 * A notification arrives with no session and no token, carrying a subscription
 * id and a client state and nothing else. Behind a load balancer it also
 * arrives at whichever container the balancer picked, which is rarely the one
 * that created the subscription. So everything needed to answer it has to be
 * somewhere every container can read, which is what the
 * {@code MS_GRAPH_SUBSCRIPTION} table is.
 * </p>
 *
 * <p>
 * Two things are kept, and the second is the awkward one:
 * </p>
 * <ul>
 * <li>The client state, which is how a delivery is recognized as one this
 * deployment asked for.</li>
 * <li>The subscriber's Microsoft tokens, because acting on a notification means
 * reading the message or the event <em>as them</em>, and a container that never
 * saw them sign in has no other way to. A user id alone cannot be turned back
 * into a usable session. These are credentials at rest, kept the way the GitHub
 * app's client secret and private key already are in this database, and the
 * alternative to keeping them is an app-only identity with tenant-wide
 * application permissions, which is a larger grant rather than a smaller
 * one.</li>
 * </ul>
 *
 * <p>
 * The refresh token is single use in Microsoft Entra: spending it mints a new
 * one and retires the old. Whichever container refreshes therefore writes the
 * new pair back, or every other container would be holding a token that has
 * already been spent.
 * </p>
 */
public class MicrosoftGraphSubscriptionRegistry {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftGraphSubscriptionRegistry.class);

	/**
	 * The lifetime a rebuilt token is described as having. The real one is not
	 * recorded, and only the moment it runs out matters, so the start is worked
	 * back from the expiry against this.
	 */
	private static final int NOMINAL_LIFETIME_SECONDS = 3600;

	private MicrosoftGraphSubscriptionRegistry() {

	}

	/**
	 * One subscription this deployment made.
	 */
	public static class Subscription {

		private final String id;
		private final String resource;
		private final String changeType;
		private final String clientState;
		private final String notificationUrl;
		private final String userId;
		private final String userProvider;
		private final String userEmail;
		private final String accessToken;
		private final String refreshToken;
		private final Instant tokenExpiration;
		private Instant expiration;

		private Subscription(Map<String, Object> row) {
			this.id = asString(row.get("subscriptionId"));
			this.resource = asString(row.get("resource"));
			this.changeType = asString(row.get("changeType"));
			this.clientState = asString(row.get("clientState"));
			this.notificationUrl = asString(row.get("notificationUrl"));
			this.userId = asString(row.get("userId"));
			this.userProvider = asString(row.get("userProvider"));
			this.userEmail = asString(row.get("userEmail"));
			this.accessToken = asString(row.get("accessToken"));
			this.refreshToken = asString(row.get("refreshToken"));
			this.tokenExpiration = asInstant(row.get("tokenExpiration"));
			this.expiration = asInstant(row.get("expiration"));
		}

		public String getId() {
			return this.id;
		}

		public String getResource() {
			return this.resource;
		}

		public String getChangeType() {
			return this.changeType;
		}

		public String getNotificationUrl() {
			return this.notificationUrl;
		}

		public String getUserId() {
			return this.userId;
		}

		public Instant getExpiration() {
			return this.expiration;
		}

		public void setExpiration(Instant expiration) {
			this.expiration = expiration;
		}

		/**
		 * Whether a notification really came from Graph for this subscription.
		 *
		 * <p>
		 * The client state is a shared secret rather than a signature, so this is not
		 * proof of much on its own. It is what Graph offers for a notification without
		 * resource data, and it is enough to discard anything posted at the receiver by
		 * something that never saw the subscription being made.
		 * </p>
		 *
		 * @param candidate the client state the notification carried
		 * @return true when it matches what this subscription was created with
		 */
		public boolean matches(String candidate) {
			if (this.clientState == null || this.clientState.isEmpty()) {
				return true;
			}
			return this.clientState.equals(candidate);
		}

		/**
		 * The subscriber, rebuilt from what was recorded.
		 *
		 * <p>
		 * Enough of a user to act as: the Microsoft token is on it, which is all the
		 * Graph helpers read. It is not the session they signed in with and carries
		 * none of their other logins.
		 * </p>
		 *
		 * @return the user this subscription belongs to
		 */
		public User getUser() {
			AccessToken token = new AccessToken();
			token.setProvider(AuthProvider.MICROSOFT);
			token.setId(this.userId);
			token.setEmail(this.userEmail);
			token.setAccess_token(this.accessToken);
			if (this.refreshToken != null) {
				token.addMetaValue(AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY, this.refreshToken);
			}
			if (this.tokenExpiration != null) {
				// the start is worked back from the expiry, so the check that matters -
				// whether it has run out - lands exactly on the recorded moment
				token.setExpires_in(NOMINAL_LIFETIME_SECONDS);
				token.setStartTime(this.tokenExpiration.toEpochMilli() - (NOMINAL_LIFETIME_SECONDS * 1000L));
			}

			User user = new User();
			user.setAccessToken(token);
			return user;
		}

		/**
		 * A Microsoft token good to use now, refreshed and written back when the
		 * recorded one has run out.
		 *
		 * <p>
		 * This is the call a receiver wants rather than {@link #getUser()}: it is the
		 * one that keeps the stored pair current for every other container.
		 * </p>
		 *
		 * @return an access token for the subscriber
		 * @throws Exception if their Microsoft login can no longer be refreshed
		 */
		public String accessToken() throws Exception {
			User user = getUser();
			String token = MicrosoftLoginUtils.getValidAccessToken(user);

			AccessToken current = user.getAccessToken(AuthProvider.MICROSOFT);
			if (current != null && token != null && !token.equals(this.accessToken)) {
				String refreshed = firstMetaValue(current, AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY);
				Timestamp expiresAt = current.getExpires_in() > 0 && current.getStartTime() > 0
						? new Timestamp(current.getStartTime() + (current.getExpires_in() * 1000L))
						: null;
				SecurityExternalConnectorsUtils.updateMicrosoftGraphSubscriptionToken(this.id, token,
						refreshed == null ? this.refreshToken : refreshed, expiresAt);
			}
			return token;
		}

		/**
		 * @return the subscription as a map, without either secret
		 */
		public Map<String, Object> describe() {
			Map<String, Object> described = new LinkedHashMap<>();
			described.put("subscriptionId", this.id);
			described.put("resource", this.resource);
			described.put("changeType", this.changeType);
			described.put("notificationUrl", this.notificationUrl);
			described.put("userId", this.userId);
			described.put("userProvider", this.userProvider);
			if (this.expiration != null) {
				described.put("expirationDateTime", this.expiration.toString());
			}
			return described;
		}

	}

	/**
	 * Records a subscription that was just created, along with what a container
	 * that never saw the subscriber needs in order to act for them.
	 *
	 * @param subscriptionId  the id Graph gave it
	 * @param resource        what it watches
	 * @param changeType      which changes it hears about
	 * @param clientState     the secret every notification echoes back
	 * @param notificationUrl where Graph posts them
	 * @param expiration      when Graph stops sending
	 * @param user            the subscriber, whose Microsoft token is taken off
	 *                        them as they are now
	 */
	public static void register(String subscriptionId, String resource, String changeType, String clientState,
			String notificationUrl, Instant expiration, User user) {
		AccessToken token = user == null ? null : user.getAccessToken(AuthProvider.MICROSOFT);
		if (token == null) {
			throw new IllegalArgumentException(
					"A Microsoft login is required to record a Graph subscription for somebody.");
		}
		// the subscription is a Microsoft thing and is rebuilt as a Microsoft
		// identity, so it is keyed on the Microsoft login rather than on whichever
		// login the person happens to sign in with. Keying it on a native or Google
		// id would put that id on a Microsoft token when the row is read back
		String userId = token.getId();
		if (userId == null || userId.trim().isEmpty()) {
			throw new IllegalArgumentException(
					"The Microsoft login carries no id, so a Graph subscription cannot be recorded against it.");
		}

		Timestamp tokenExpiration = token.getExpires_in() > 0 && token.getStartTime() > 0
				? new Timestamp(token.getStartTime() + (token.getExpires_in() * 1000L))
				: null;

		SecurityExternalConnectorsUtils.upsertMicrosoftGraphSubscription(subscriptionId, userId,
				AuthProvider.MICROSOFT.toString(), token.getEmail(), clientState, resource, changeType, notificationUrl,
				expiration == null ? null : Timestamp.from(expiration), token.getAccess_token(),
				firstMetaValue(token, AbstractOAuthTokenFiller.REFRESH_TOKEN_KEY), tokenExpiration);
		classLogger.info("Recorded Microsoft Graph subscription {} on '{}' for user {}", subscriptionId, resource,
				userId);
	}

	/**
	 * @param subscriptionId the id a notification carried
	 * @return what is known about it, or null when this deployment did not create
	 *         it
	 */
	public static Subscription get(String subscriptionId) {
		if (subscriptionId == null) {
			return null;
		}
		Map<String, Object> row = SecurityExternalConnectorsUtils.getMicrosoftGraphSubscription(subscriptionId);
		return row == null ? null : new Subscription(row);
	}

	/**
	 * Forgets a subscription, which is what stopping one ends with.
	 *
	 * @param subscriptionId the subscription to forget
	 */
	public static void remove(String subscriptionId) {
		if (subscriptionId == null) {
			return;
		}
		try {
			SecurityExternalConnectorsUtils.deleteMicrosoftGraphSubscription(subscriptionId);
		} catch (Exception e) {
			classLogger.error("Failed to forget the Microsoft Graph subscription {}", subscriptionId, e);
		}
	}

	/**
	 * Records that a subscription now expires later than it did.
	 *
	 * @param subscriptionId the subscription that was renewed
	 * @param expiration     when it now expires
	 */
	public static void renewed(String subscriptionId, Instant expiration) {
		SecurityExternalConnectorsUtils.updateMicrosoftGraphSubscriptionExpiration(subscriptionId,
				expiration == null ? null : Timestamp.from(expiration));
	}

	/**
	 * The subscriptions held for one user.
	 *
	 * @param userId the user to look for
	 * @return their subscriptions, empty when they have none
	 */
	public static List<Subscription> forUser(String userId) {
		List<Subscription> found = new ArrayList<>();
		if (userId == null) {
			return found;
		}
		for (Map<String, Object> row : SecurityExternalConnectorsUtils.getMicrosoftGraphSubscriptionsByUser(userId)) {
			found.add(new Subscription(row));
		}
		return found;
	}

	/**
	 * @param token the token to read
	 * @param key   the metadata key to read
	 * @return the first value held under that key, or null when there is none
	 */
	private static String firstMetaValue(AccessToken token, String key) {
		if (token == null) {
			return null;
		}
		Collection<String> values = token.getMetaValues(key);
		if (values == null || values.isEmpty()) {
			return null;
		}
		return values.iterator().next();
	}

	/**
	 * @param value a column value
	 * @return it as a string, or null when there is nothing to read
	 */
	private static String asString(Object value) {
		if (value == null || value.toString().trim().isEmpty()) {
			return null;
		}
		return value.toString();
	}

	/**
	 * Reads a moment out of a column, however the driver handed it back.
	 *
	 * @param value a column value
	 * @return the moment, or null when there is none to read
	 */
	private static Instant asInstant(Object value) {
		if (value == null) {
			return null;
		}
		if (value instanceof Timestamp) {
			return ((Timestamp) value).toInstant();
		}
		if (value instanceof Date) {
			return ((Date) value).toInstant();
		}
		if (value instanceof Number) {
			return Instant.ofEpochMilli(((Number) value).longValue());
		}
		String text = value.toString().trim();
		if (text.isEmpty()) {
			return null;
		}
		try {
			return Instant.parse(text);
		} catch (RuntimeException e) {
			classLogger.debug("Reading the timestamp '{}' as a jdbc one", text, e);
		}
		try {
			return Timestamp.valueOf(text).toInstant();
		} catch (RuntimeException e) {
			classLogger.warn("Could not read the timestamp '{}' held against a Graph subscription", text, e);
			return null;
		}
	}

}
