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

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityUserUtils;
import prerna.io.connector.ms.MicrosoftGraphSubscriptionClient;
import prerna.io.connector.ms.MicrosoftGraphSubscriptionRegistry;
import prerna.io.connector.ms.MicrosoftGraphSubscriptionRegistry.Subscription;
import prerna.io.connector.ms.MicrosoftLoginUtils;

/**
 * New mail by Microsoft Graph webhook. Each owner's Inbox and Sent Items are subscribed to with their Microsoft
 * login when they sync or import; a notification imports the new message through the same gate as a sync, so the
 * chats that include its thread see it on their next turn. Subscriptions are renewed here before they expire. A
 * server Graph cannot reach (localhost, a .local name) makes none, and chats pull their threads instead
 * (BrainSync.pullThreads).
 */
public final class BrainMailWebhook {

	private static final Logger classLogger = LogManager.getLogger(BrainMailWebhook.class);

	// Collaboration's subscriptions: an owner's Inbox and Sent Items, the folders a sync reads
	static final List<String> RESOURCES = List.of("me/mailFolders('inbox')/messages",
			"me/mailFolders('sentitems')/messages");
	private static final String CREATED = "created";
	// Monolith's mail receiver (MicrosoftGraphSubscriptionService.MESSAGES_PATH)
	private static final String NOTIFICATION_PATH = "/msgraph/notifications/messages";
	// renewed with less than this left; a mail subscription lasts about a week
	private static final Duration RENEW_WITHIN = Duration.ofDays(2);
	// after a failed subscribe (usually Graph could not reach the url), the owner's syncs wait this long to try again
	private static final Duration RETRY_AFTER = Duration.ofHours(1);
	private static final Map<String, Instant> FAILED = new ConcurrentHashMap<>();
	private static final AtomicBoolean RENEWING = new AtomicBoolean();
	private static final ExecutorService SUBSCRIBING = Executors
			.newSingleThreadExecutor(runnable -> daemon(runnable, "collaboration-mail-subscribe"));
	private static final ScheduledExecutorService RENEWALS = Executors
			.newSingleThreadScheduledExecutor(runnable -> daemon(runnable, "collaboration-mail-renew"));

	private BrainMailWebhook() {
	}

	/** Whether a Graph subscription is one of Collaboration's. */
	public static boolean isMailResource(String resource) {
		return resource != null && RESOURCES.contains(resource);
	}

	// ---- notifications ----

	/**
	 * A notification for one of Collaboration's subscriptions: a created message goes into its owner's Brain. Called
	 * off the request thread; never throws.
	 */
	public static void onMail(Subscription subscription, String changeType, String messageId) {
		if (!CREATED.equalsIgnoreCase(changeType) || messageId == null || !CollaborationDbUtils.isInitalized()) {
			return;
		}
		try {
			List<String[]> owners = owners(subscription.getUserId());
			if (owners.isEmpty()) {
				return;
			}
			AccessToken microsoft = microsoftToken(subscription);
			BrainMailHeaderSource source = BrainMailHeaderSource.current();
			for (String[] owner : owners) {
				User user = ownerUser(owner[0], owner[1], microsoft);
				Map<String, Object> header = source.message(user, messageId);
				if (header != null) {
					Set<String> threads = BrainSync.importHeaders(user, owner[0], owner[1], source, List.of(header));
					classLogger.info("New mail from Microsoft Graph subscription {} reached {} thread(s)",
							subscription.getId(), threads.size());
				}
			}
		} catch (Exception e) {
			classLogger.error("Could not import new mail for the Microsoft Graph subscription {}", subscription.getId(),
					e);
		}
	}

	// the Brain owners this Microsoft login is the mailbox of (onboarding records it); usually one
	static List<String[]> owners(String msUserId) {
		if (msUserId == null) {
			return List.of();
		}
		return CollaborationDbUtils.query("SELECT OWNER_ID, OWNER_TYPE FROM COLLAB_OWNER WHERE MS_USER_ID = ?",
				rs -> new String[] { rs.getString("OWNER_ID"), rs.getString("OWNER_TYPE") }, msUserId);
	}

	/**
	 * The owner as the login their Brain belongs to (which also decides the models they may use), holding the
	 * subscriber's Microsoft token for Graph.
	 */
	static User ownerUser(String ownerId, String ownerType, AccessToken microsoft) {
		AuthProvider provider = AuthProvider.getProviderFromString(ownerType);
		User user = new User();
		if (provider != AuthProvider.MICROSOFT) {
			AccessToken login = new AccessToken();
			login.setProvider(provider);
			login.setId(ownerId);
			SecurityUserUtils.loadUserMetadata(login);
			user.setAccessToken(login);
		}
		user.setAccessToken(microsoft);
		user.setPrimaryLogin(provider);
		return user;
	}

	// refreshed through the subscription first, which writes the new pair back for every server, so the import
	// never spends the stored refresh token itself
	private static AccessToken microsoftToken(Subscription subscription) throws Exception {
		subscription.accessToken();
		Subscription current = MicrosoftGraphSubscriptionRegistry.get(subscription.getId());
		return (current == null ? subscription : current).getUser().getAccessToken(AuthProvider.MICROSOFT);
	}

	// ---- subscriptions ----

	/**
	 * Makes sure the owner's Inbox and Sent Items are subscribed to with their Microsoft login, renewing one close to
	 * expiring. In the background; skipped without a Microsoft login, when Graph cannot reach this server, and for an
	 * hour after a failed attempt.
	 */
	public static void ensureSubscribed(User user) {
		AccessToken microsoft = user == null ? null : user.getAccessToken(AuthProvider.MICROSOFT);
		if (microsoft == null || microsoft.getId() == null || !MicrosoftGraphSubscriptionClient.isPubliclyReachable()
				|| !MicrosoftGraphSubscriptionClient.hasScopeFor(RESOURCES.get(0))) {
			return;
		}
		String msUserId = microsoft.getId();
		Instant failed = FAILED.get(msUserId);
		if (failed != null && failed.plus(RETRY_AFTER).isAfter(Instant.now())) {
			return;
		}
		SUBSCRIBING.execute(() -> {
			try {
				subscribe(user, msUserId);
				FAILED.remove(msUserId);
			} catch (Exception e) {
				FAILED.put(msUserId, Instant.now());
				classLogger.warn("Could not subscribe to new mail for {}; chats pull their threads instead", msUserId, e);
			}
		});
	}

	// creates what is missing, renews what is close to expiring, and stops the ones made for another url
	private static void subscribe(User user, String msUserId) throws Exception {
		String notificationUrl = MicrosoftGraphSubscriptionClient.notificationUrl(NOTIFICATION_PATH);
		String token = MicrosoftLoginUtils.getValidAccessToken(user);
		Map<String, Subscription> held = new HashMap<>();
		for (Subscription subscription : MicrosoftGraphSubscriptionRegistry.forUser(msUserId)) {
			if (!isMailResource(subscription.getResource())) {
				continue;
			}
			boolean live = subscription.getExpiration() != null && subscription.getExpiration().isAfter(Instant.now());
			if (live && notificationUrl.equals(subscription.getNotificationUrl())
					&& !held.containsKey(subscription.getResource())) {
				held.put(subscription.getResource(), subscription);
			} else {
				stop(token, subscription);
			}
		}
		for (String resource : RESOURCES) {
			Subscription subscription = held.get(resource);
			if (subscription == null) {
				String clientState = UUID.randomUUID().toString();
				Map<String, Object> created = MicrosoftGraphSubscriptionClient.create(token, resource, CREATED,
						notificationUrl, clientState, 0);
				MicrosoftGraphSubscriptionRegistry.register(String.valueOf(created.get("id")), resource, CREATED,
						clientState, notificationUrl, expirationOf(created), user);
			} else if (subscription.getExpiration().isBefore(Instant.now().plus(RENEW_WITHIN))) {
				renew(token, subscription);
			}
		}
	}

	// one made for an old url, or a duplicate: stopped at Graph when it still exists there, then forgotten
	private static void stop(String token, Subscription subscription) {
		try {
			MicrosoftGraphSubscriptionClient.delete(token, subscription.getId());
		} catch (Exception e) {
			classLogger.debug("Graph had no subscription {} to stop", subscription.getId(), e);
		}
		MicrosoftGraphSubscriptionRegistry.remove(subscription.getId());
	}

	/** Renews Collaboration's subscriptions every hour with their stored tokens; once per server. */
	public static void startRenewals() {
		if (RENEWING.compareAndSet(false, true)) {
			RENEWALS.scheduleWithFixedDelay(BrainMailWebhook::renewAll, 10, 60, TimeUnit.MINUTES);
		}
	}

	// shortcut: every server renews, so with several servers two can refresh the same stored token at once; give
	// renewal one owner (a cluster lock) before running more than one server
	static void renewAll() {
		try {
			if (!CollaborationDbUtils.isInitalized()) {
				return;
			}
			for (String msUserId : CollaborationDbUtils.query(
					"SELECT DISTINCT MS_USER_ID FROM COLLAB_OWNER WHERE MS_USER_ID IS NOT NULL", rs -> rs.getString(1))) {
				for (Subscription subscription : MicrosoftGraphSubscriptionRegistry.forUser(msUserId)) {
					if (!isMailResource(subscription.getResource()) || subscription.getExpiration() == null
							|| subscription.getExpiration().isAfter(Instant.now().plus(RENEW_WITHIN))) {
						continue;
					}
					try {
						renew(subscription.accessToken(), subscription);
					} catch (Exception e) {
						classLogger.warn("Could not renew the Microsoft Graph subscription {}", subscription.getId(), e);
						// gone at Graph too; the owner's next sync makes a new one
						if (subscription.getExpiration().isBefore(Instant.now())) {
							MicrosoftGraphSubscriptionRegistry.remove(subscription.getId());
						}
					}
				}
			}
		} catch (RuntimeException e) {
			classLogger.error("Renewing Collaboration's mail subscriptions failed; trying again in an hour", e);
		}
	}

	private static void renew(String token, Subscription subscription) throws Exception {
		Map<String, Object> renewed = MicrosoftGraphSubscriptionClient.renew(token, subscription.getId(),
				subscription.getResource(), 0);
		MicrosoftGraphSubscriptionRegistry.renewed(subscription.getId(), expirationOf(renewed));
	}

	private static Instant expirationOf(Map<String, Object> subscription) {
		Object value = subscription == null ? null : subscription.get("expirationDateTime");
		try {
			return value == null ? null : Instant.parse(String.valueOf(value));
		} catch (RuntimeException e) {
			return null;
		}
	}

	private static Thread daemon(Runnable runnable, String name) {
		Thread thread = new Thread(runnable, name);
		thread.setDaemon(true);
		return thread;
	}
}
