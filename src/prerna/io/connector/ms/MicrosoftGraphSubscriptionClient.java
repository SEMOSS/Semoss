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

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.security.HttpHelperUtility;
import prerna.util.SocialPropertiesUtil;
import prerna.util.Utility;

/**
 * The change notification subscriptions of Microsoft Graph, as plain calls.
 *
 * <p>
 * A subscription is how Graph is asked to tell this instance when something
 * changes, rather than this instance asking Graph over and over. It names a
 * resource such as {@code me/messages}, the kinds of change to hear about, and
 * the url to post them to.
 * </p>
 *
 * <p>
 * Three rules shape everything here, and all three come from Graph:
 * </p>
 * <ul>
 * <li>The notification url has to be public https, and Graph proves that before
 * it agrees to anything: creating a subscription posts a validation token to
 * that url first and fails unless it is echoed back. A url on localhost cannot
 * be subscribed at all, which is what {@link #publicBaseUrl()} and its override
 * are for.</li>
 * <li>A subscription expires, soon. How soon depends on the resource, which is
 * what {@link #maxLifetimeMinutes(String)} knows, and nothing renews itself, so
 * whatever created one has to keep renewing it.</li>
 * <li>A notification says what changed and not what it changed to. Reading the
 * message or the event is a separate call afterwards, made with the token of
 * whoever the subscription belongs to.</li>
 * </ul>
 *
 * <p>
 * Nothing here holds a token. Every method takes the delegated token of the
 * user the subscription is for, and Graph records that user as the creator, so
 * a subscription can only ever be made for what its creator can already read.
 * </p>
 */
public class MicrosoftGraphSubscriptionClient {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftGraphSubscriptionClient.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final String GRAPH_BASE = MicrosoftTokenFiller.MS_GRAPH_BASE_API + "/v1.0";
	private static final String SUBSCRIPTIONS = GRAPH_BASE + "/subscriptions";

	/**
	 * Where Graph can actually reach this instance, when that is not where the
	 * application thinks it lives. Graph will not post to localhost, so local work
	 * puts a tunnel in front of it and names the tunnel here, the same way the
	 * GitHub integration does.
	 *
	 * <p>
	 * Example value: {@code https://elm-uselessly-laurel.ngrok-free.dev}
	 * </p>
	 */
	private static final String MS_PUBLIC_ORIGIN_OVERRIDE = "MS_PUBLIC_ORIGIN_OVERRIDE";

	/** Where the Microsoft scopes this deployment asks for are configured. */
	private static final String MS_SCOPE_PROPERTY = "ms_scope";

	/** Leave room for clock skew between this process and Graph. */
	private static final int SAFETY_BUFFER_MINUTES = 5;

	/** Graph currently enforces 10,070 minutes for Outlook subscriptions. */
	private static final int OUTLOOK_MAX_MINUTES = 10_070 - SAFETY_BUFFER_MINUTES;

	/** The documented maximum for Teams chat/channel subscriptions. */
	private static final int TEAMS_MAX_MINUTES = 4_320 - SAFETY_BUFFER_MINUTES;

	/** The maximum for Teams approvals subscriptions. */
	private static final int TEAMS_APPROVALS_MAX_MINUTES = 43_200 - SAFETY_BUFFER_MINUTES;

	/** The maximum for Microsoft Graph security alert subscriptions. */
	private static final int SECURITY_ALERT_MAX_MINUTES = 43_200 - SAFETY_BUFFER_MINUTES;

	/** The maximum for Teams Shifts subscriptions. */
	private static final int TEAMS_SHIFTS_MAX_MINUTES = 360 - SAFETY_BUFFER_MINUTES;

	/** The maximum for OneDrive and SharePoint list subscriptions. */
	private static final int DRIVE_MAX_MINUTES = 42_300 - SAFETY_BUFFER_MINUTES;

	/** The maximum for directory resource subscriptions. */
	private static final int DIRECTORY_MAX_MINUTES = 41_760 - SAFETY_BUFFER_MINUTES;

	/** The longest a subscription on somebody's presence may live, in minutes. */
	private static final int PRESENCE_MAX_MINUTES = 60 - SAFETY_BUFFER_MINUTES;

	/** The maximum for resources whose documented limit is 4,230 minutes. */
	private static final int STANDARD_MAX_MINUTES = 4_230 - SAFETY_BUFFER_MINUTES;

	/**
	 * Graph rounds anything sooner than this up to it, so asking for less is only a
	 * way of being surprised later.
	 */
	private static final int MIN_MINUTES = 45;

	private MicrosoftGraphSubscriptionClient() {

	}

	/**
	 * Asks Graph to start telling this instance about changes.
	 *
	 * <p>
	 * Graph posts a validation token to the notification url before answering, so
	 * this call only succeeds while that url is reachable and echoing it.
	 * </p>
	 *
	 * @param accessToken     delegated Microsoft Graph token of the user the
	 *                        subscription is for
	 * @param resource        what to watch, such as {@code me/messages} or
	 *                        {@code me/events}, with no leading slash and no base
	 *                        url
	 * @param changeType      which changes to hear about, one or more of
	 *                        {@code created}, {@code updated} and {@code deleted},
	 *                        comma separated
	 * @param notificationUrl the public https url Graph posts the notifications to
	 * @param clientState     the secret echoed back in every notification, which is
	 *                        how a receiver tells a real one from anything else;
	 *                        128 characters at most
	 * @param minutes         how long to ask for, held between what Graph rounds up
	 *                        to and what it allows for this resource
	 * @return the subscription as Graph created it, carrying the id it was given
	 *         and when it expires
	 * @throws IllegalArgumentException if required inputs are missing or invalid
	 * @throws Exception                if the create fails
	 */
	public static Map<String, Object> create(String accessToken, String resource, String changeType,
			String notificationUrl, String clientState, int minutes) throws Exception {
		try {
			requireValue(resource, "A resource is required to subscribe to Microsoft Graph notifications.");
			requireValue(changeType, "A change type is required to subscribe to Microsoft Graph notifications.");
			requireValue(notificationUrl,
					"A notification url is required to subscribe to Microsoft Graph notifications.");
			if (!notificationUrl.trim().toLowerCase(Locale.ROOT).startsWith("https://")) {
				throw new IllegalArgumentException(
						"Microsoft Graph only posts notifications to an https url, and will not reach: "
								+ notificationUrl);
			}
			if (clientState != null && clientState.length() > 128) {
				throw new IllegalArgumentException("The client state cannot be longer than 128 characters.");
			}

			Map<String, Object> body = new LinkedHashMap<>();
			body.put("changeType", changeType.trim());
			body.put("notificationUrl", notificationUrl.trim());
			body.put("resource", resource.trim());
			body.put("expirationDateTime", expiration(resource, minutes));
			if (clientState != null && !clientState.trim().isEmpty()) {
				body.put("clientState", clientState.trim());
			}
			body.put("latestSupportedTlsVersion", "v1_2");

			String response = HttpHelperUtility.postRequestStringBody(SUBSCRIPTIONS, headers(accessToken),
					GSON.toJson(body), ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> subscription = readMap(response);
			if (subscription == null || subscription.get("id") == null) {
				throw new IllegalStateException("Microsoft Graph returned no subscription for resource: " + resource);
			}
			classLogger.info("Subscribed to Microsoft Graph '{}' as subscription {} until {}", resource,
					subscription.get("id"), subscription.get("expirationDateTime"));
			return subscription;
		} catch (Exception e) {
			classLogger.error("Failed to subscribe to Microsoft Graph notifications for '{}'.", resource, e);
			throw e;
		}
	}

	/**
	 * Pushes an existing subscription's expiry back out.
	 *
	 * @param accessToken delegated Microsoft Graph token of the user the
	 *                    subscription belongs to
	 * @param id          the subscription to renew
	 * @param resource    what it watches, which is what decides how far out it can
	 *                    be pushed
	 * @param minutes     how long to ask for
	 * @return the subscription as Graph left it
	 * @throws IllegalArgumentException if required inputs are missing
	 * @throws Exception                if the renew fails
	 */
	public static Map<String, Object> renew(String accessToken, String id, String resource, int minutes)
			throws Exception {
		try {
			requireValue(id, "A subscription id is required to renew a Microsoft Graph subscription.");
			requireValue(resource, "A resource is required to renew a Microsoft Graph subscription.");

			Map<String, Object> body = new LinkedHashMap<>();
			body.put("expirationDateTime", expiration(resource, minutes));

			String url = SUBSCRIPTIONS + "/" + id.trim();
			String response = HttpHelperUtility.patchRequestStringBody(url, headers(accessToken), GSON.toJson(body),
					ContentType.APPLICATION_JSON, null, null, null);
			Map<String, Object> subscription = readMap(response);
			if (subscription == null) {
				throw new IllegalStateException("Microsoft Graph returned nothing for subscription id = " + id);
			}
			classLogger.info("Renewed Microsoft Graph subscription {} until {}", id,
					subscription.get("expirationDateTime"));
			return subscription;
		} catch (Exception e) {
			classLogger.error("Failed to renew the Microsoft Graph subscription '{}'.", id, e);
			throw e;
		}
	}

	/**
	 * Stops a subscription.
	 *
	 * @param accessToken delegated Microsoft Graph token of the user the
	 *                    subscription belongs to
	 * @param id          the subscription to stop
	 * @throws IllegalArgumentException if required inputs are missing
	 * @throws Exception                if the delete fails
	 */
	public static void delete(String accessToken, String id) throws Exception {
		try {
			requireValue(id, "A subscription id is required to stop a Microsoft Graph subscription.");

			// a successful delete answers 204 with no body
			HttpHelperUtility.deleteRequestStringBody(SUBSCRIPTIONS + "/" + id.trim(), headers(accessToken), null, null,
					null);
			classLogger.info("Stopped Microsoft Graph subscription {}", id);
		} catch (Exception e) {
			classLogger.error("Failed to stop the Microsoft Graph subscription '{}'.", id, e);
			throw e;
		}
	}

	/**
	 * Lists the subscriptions Graph holds for whoever the token belongs to.
	 *
	 * <p>
	 * Worth reading against this instance's own record of them, since Graph is the
	 * one that counts: a subscription that expired, or that another instance
	 * created, shows up here and nowhere else.
	 * </p>
	 *
	 * @param accessToken delegated Microsoft Graph token of the user
	 * @return the subscriptions as Graph returned them
	 * @throws Exception if the read fails
	 */
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> list(String accessToken) throws Exception {
		try {
			String response = HttpHelperUtility.getRequest(SUBSCRIPTIONS, headers(accessToken), null, null, null);
			Map<String, Object> json = readMap(response);
			List<Map<String, Object>> subscriptions = new ArrayList<>();
			if (json == null || !(json.get("value") instanceof List)) {
				return subscriptions;
			}
			for (Object entry : (List<?>) json.get("value")) {
				if (entry instanceof Map) {
					subscriptions.add((Map<String, Object>) entry);
				}
			}
			return subscriptions;
		} catch (Exception e) {
			classLogger.error("Failed to list the Microsoft Graph subscriptions.", e);
			throw e;
		}
	}

	/**
	 * The url Graph posts notifications to for one receiver.
	 *
	 * @param path the path of the receiver, such as
	 *             {@code /msgraph/notifications/messages}
	 * @return the public url of it
	 */
	public static String notificationUrl(String path) {
		return publicBaseUrl() + path;
	}

	/**
	 * The delegated scopes Graph requires before it will let somebody subscribe to
	 * a resource.
	 *
	 * <p>
	 * Subscribing needs the same permission as reading, so these are the read
	 * scopes for the resource. Either of the pair satisfies it, which is why each
	 * entry is a set rather than a single name.
	 * </p>
	 *
	 * @param resource what is being subscribed to
	 * @return the acceptable scopes, any one of which is enough, empty when this
	 *         class has not been taught about the resource
	 */
	public static List<String> acceptableScopes(String resource) {
		if (resource == null) {
			return new ArrayList<>();
		}
		String wanted = resource.trim().toLowerCase(Locale.ROOT);
		if (wanted.contains("/messages") || wanted.endsWith("messages")) {
			return Arrays.asList("Mail.Read", "Mail.ReadWrite");
		}
		if (wanted.contains("/events") || wanted.endsWith("events") || wanted.contains("/calendar")) {
			return Arrays.asList("Calendars.Read", "Calendars.ReadWrite");
		}
		return new ArrayList<>();
	}

	/**
	 * Whether this deployment asked Microsoft for a scope that allows subscribing
	 * to a resource.
	 *
	 * <p>
	 * This reads what the deployment <em>requests</em> at sign in, which is the
	 * thing an administrator controls and the thing that is usually wrong. It
	 * cannot see what the tenant actually consented to, so a true answer here is
	 * necessary rather than sufficient: Graph still has the last word, and its
	 * refusal is what a caller should report.
	 * </p>
	 *
	 * @param resource what is being subscribed to
	 * @return true when one of the acceptable scopes is configured
	 */
	public static boolean hasScopeFor(String resource) {
		List<String> acceptable = acceptableScopes(resource);
		if (acceptable.isEmpty()) {
			// nothing known about the resource, so nothing to object to
			return true;
		}
		String configured = configuredScopes();
		if (configured == null || configured.trim().isEmpty()) {
			return false;
		}
		String haystack = " " + configured.toLowerCase(Locale.ROOT).replace(',', ' ') + " ";
		for (String scope : acceptable) {
			if (haystack.contains(" " + scope.toLowerCase(Locale.ROOT) + " ")) {
				return true;
			}
		}
		return false;
	}

	/**
	 * @return the Microsoft scopes this deployment requests at sign in, as
	 *         configured, or null when none are
	 */
	public static String configuredScopes() {
		return SocialPropertiesUtil.getInstance().getProperty(MS_SCOPE_PROPERTY);
	}

	/**
	 * Whether Graph could reach this deployment at all.
	 *
	 * <p>
	 * Creating a subscription makes Graph post a validation token to the
	 * notification url before it agrees to anything, so a deployment that is not
	 * publicly reachable over https cannot subscribe to anything however its scopes
	 * are set. Worth answering before somebody presses a button rather than after
	 * Graph refuses.
	 * </p>
	 *
	 * @return true when the notification url is one Graph could post to
	 */
	public static boolean isPubliclyReachable() {
		String base = publicBaseUrl();
		if (base == null || base.trim().isEmpty()) {
			return false;
		}
		String url = base.toLowerCase(Locale.ROOT);
		if (!url.startsWith("https://")) {
			return false;
		}
		return !url.contains("://localhost") && !url.contains("://127.0.0.1") && !url.contains("://0.0.0.0")
				&& !url.contains("://[::1]");
	}

	/**
	 * Where Graph can reach this instance.
	 *
	 * <p>
	 * The application url, unless an override names the tunnel standing in front of
	 * it. The path of the application url is kept either way, so a deployment under
	 * {@code /Monolith} stays under it.
	 * </p>
	 *
	 * @return the base url, with no trailing slash
	 */
	public static String publicBaseUrl() {
		String appUrl = Utility.getApplicationUrl();
		String override = Utility.getDIHelperProperty(MS_PUBLIC_ORIGIN_OVERRIDE);
		if (override != null && !override.trim().isEmpty()) {
			int schemeEnd = appUrl == null ? -1 : appUrl.indexOf("://");
			int pathStart = schemeEnd < 0 ? -1 : appUrl.indexOf('/', schemeEnd + 3);
			String path = pathStart < 0 ? "" : appUrl.substring(pathStart);
			return trimTrailingSlash(override.trim() + path);
		}
		return trimTrailingSlash(appUrl);
	}

	/**
	 * The longest this client requests for a subscription on a resource.
	 *
	 * <p>
	 * The limits differ by an order of magnitude between one resource and another,
	 * and each configured limit includes a five-minute buffer. Unknown resources
	 * are rejected so they cannot accidentally be sent with an invalid lifetime.
	 * </p>
	 *
	 * @param resource what the subscription watches
	 * @return how many minutes it may live for
	 * @throws IllegalArgumentException when the resource is blank or unsupported
	 */
	public static int maxLifetimeMinutes(String resource) {
		requireValue(resource, "A resource is required to determine the Microsoft Graph subscription lifetime.");
		String wanted = resource.trim().toLowerCase(Locale.ROOT);

		// Check the special Teams families before checking generic message paths.
		// Teams channel messages also contain a `messages` path segment.
		if (hasSegment(wanted, "offershiftrequests") || hasSegment(wanted, "openshiftchangerequests")
				|| hasSegment(wanted, "shiftswaprequests") || hasSegment(wanted, "shifts")
				|| hasSegment(wanted, "timeoffrequests")) {
			return TEAMS_SHIFTS_MAX_MINUTES;
		}
		if (hasSegment(wanted, "approvals") || hasSegment(wanted, "approval") || hasSegment(wanted, "approvalitems")) {
			return TEAMS_APPROVALS_MAX_MINUTES;
		}
		if (hasSegment(wanted, "recordings") || hasSegment(wanted, "transcripts")
				|| hasSegment(wanted, "callrecordings") || hasSegment(wanted, "calltranscripts")
				|| hasSegment(wanted, "installedapps") || hasSegment(wanted, "chats") || hasSegment(wanted, "channels")
				|| hasSegment(wanted, "teams")) {
			return TEAMS_MAX_MINUTES;
		}
		if (hasSegment(wanted, "presence") || hasSegment(wanted, "presences")) {
			return PRESENCE_MAX_MINUTES;
		}
		if (hasSegment(wanted, "drive") || hasSegment(wanted, "drives") || hasSegment(wanted, "driveitems")
				|| hasSegment(wanted, "lists")) {
			return DRIVE_MAX_MINUTES;
		}
		if (hasSegment(wanted, "messages") || hasSegment(wanted, "events") || hasSegment(wanted, "calendar")
				|| hasSegment(wanted, "calendarview") || hasSegment(wanted, "contacts")) {
			return OUTLOOK_MAX_MINUTES;
		}
		if (hasSegment(wanted, "conversations") || hasSegment(wanted, "threads") || hasSegment(wanted, "callrecords")
				|| hasSegment(wanted, "onlinemeetings") || hasSegment(wanted, "print") || hasSegment(wanted, "printer")
				|| hasSegment(wanted, "printtaskdefinitions") || hasSegment(wanted, "todo")
				|| hasSegment(wanted, "tasks") || hasSegment(wanted, "basetask")
				|| hasSegment(wanted, "aiinteraction")) {
			return STANDARD_MAX_MINUTES;
		}
		if (hasSegment(wanted, "users") || hasSegment(wanted, "groups")) {
			return DIRECTORY_MAX_MINUTES;
		}
		if (hasSegment(wanted, "alerts") && wanted.contains("health")) {
			return DRIVE_MAX_MINUTES;
		}
		if (hasSegment(wanted, "alerts") && wanted.contains("security")) {
			return SECURITY_ALERT_MAX_MINUTES;
		}
		throw new IllegalArgumentException(
				"No Microsoft Graph subscription lifetime is configured for resource: " + resource);
	}

	private static boolean hasSegment(String resource, String segment) {
		for (String part : resource.split("/")) {
			String pathSegment = part;
			int queryStart = pathSegment.indexOf('(');
			if (queryStart >= 0) {
				pathSegment = pathSegment.substring(0, queryStart);
			}
			if (segment.equals(pathSegment)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * When a subscription asked for now should expire, held inside what Graph
	 * allows at both ends.
	 *
	 * @param resource what the subscription watches
	 * @param minutes  how long was asked for; anything at or below 0 asks for the
	 *                 longest allowed
	 * @return the moment it expires, as Graph reads it
	 */
	private static String expiration(String resource, int minutes) {
		int max = maxLifetimeMinutes(resource);
		int wanted = minutes <= 0 ? max : Math.min(Math.max(minutes, MIN_MINUTES), max);
		return DateTimeFormatter.ISO_INSTANT.format(Instant.now().plusSeconds(wanted * 60L));
	}

	private static Map<String, String> headers(String accessToken) {
		return MicrosoftLoginUtils.getBearerHeader(accessToken);
	}

	private static Map<String, Object> readMap(String response) {
		if (response == null || response.trim().isEmpty()) {
			return null;
		}
		return GSON.fromJson(response, new TypeToken<Map<String, Object>>() {
		}.getType());
	}

	private static String trimTrailingSlash(String url) {
		if (url == null) {
			return "";
		}
		String trimmed = url.trim();
		while (trimmed.endsWith("/")) {
			trimmed = trimmed.substring(0, trimmed.length() - 1);
		}
		return trimmed;
	}

	private static void requireValue(String value, String message) {
		if (value == null || value.trim().isEmpty()) {
			throw new IllegalArgumentException(message);
		}
	}

}
