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
package prerna.io.connector.ms.calendar;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.jsoup.Jsoup;

/**
 * Turns the json Graph returns for a calendar into the maps the calendar
 * reactors answer with.
 *
 * <p>
 * Kept apart from {@link MicrosoftCalendarHelper} for the same reason the mail
 * mapper is kept apart from its helper: what an event looks like on the way out
 * is a decision about this codebase's shape, not about how Graph is called, and
 * a reactor that reads an event back after writing one should see it described
 * the same way a listing describes it.
 * </p>
 * 
 * <p>
 * A moment comes back as Graph gave it, a naive date and time under
 * {@code start} with the zone it should be read in alongside it under
 * {@code startTimeZone}. That zone is whatever the request asked to be answered
 * in, so a caller that named one sees its own times rather than UTC.
 * </p>
 */
public class MicrosoftCalendarEventMapper {

	private static final String ADDRESS = "address";
	private static final String NAME = "name";
	private static final String DATE_TIME = "dateTime";
	private static final String TIME_ZONE = "timeZone";
	private static final String EMAIL_ADDRESS = "emailAddress";
	private static final String DISPLAY_NAME = "displayName";

	private MicrosoftCalendarEventMapper() {

	}

	/**
	 * Describe one event.
	 *
	 * @param event        the event as Graph returned it
	 * @param includeBody  whether the body text comes back
	 * @param maxBodyChars the longest body to return before truncating it, or 0 to
	 *                     return whatever length it is
	 * @return the event as a map
	 */
	public static Map<String, Object> toEvent(Map<String, Object> event, boolean includeBody, int maxBodyChars) {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("id", event.get("id"));
		putIfPresent(output, "subject", event.get("subject"));
		putMoment(output, "start", event.get("start"));
		putMoment(output, "end", event.get("end"));
		output.put("isAllDay", Boolean.TRUE.equals(event.get("isAllDay")));
		putIfPresent(output, "location", displayNameOf(event.get("location")));
		putIfPresent(output, "organizer", addressOf(event.get("organizer")));
		putIfPresent(output, "organizerName", nameOf(event.get("organizer")));

		List<Map<String, Object>> attendees = attendees(event.get("attendees"));
		if (!attendees.isEmpty()) {
			output.put("attendees", attendees);
		}

		putIfPresent(output, "webLink", event.get("webLink"));
		putIfPresent(output, "joinUrl", joinUrlOf(event.get("onlineMeeting")));
		output.put("isOnlineMeeting", Boolean.TRUE.equals(event.get("isOnlineMeeting")));
		output.put("isCancelled", Boolean.TRUE.equals(event.get("isCancelled")));
		putIfPresent(output, "showAs", event.get("showAs"));
		putIfPresent(output, "importance", event.get("importance"));
		putIfPresent(output, "responseStatus", responseOf(event.get("responseStatus")));
		putIfPresent(output, "reminderMinutesBeforeStart", event.get("reminderMinutesBeforeStart"));
		putIfPresent(output, "categories", event.get("categories"));
		// a series master is the rule, and an occurrence or an exception is one
		// sitting of it, so this says whether the event repeats at all
		output.put("isRecurring", event.get("seriesMasterId") != null || "seriesMaster".equals(event.get("type")));

		if (includeBody) {
			String body = bodyOf(event);
			if (maxBodyChars > 0 && body.length() > maxBodyChars) {
				body = body.substring(0, maxBodyChars) + " ... [truncated]";
				output.put("bodyTruncated", true);
			}
			output.put("body", body);
		}
		return output;
	}

	/**
	 * Describe one calendar.
	 *
	 * @param calendar the calendar as Graph returned it
	 * @return the calendar as a map
	 */
	public static Map<String, Object> toCalendar(Map<String, Object> calendar) {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("id", calendar.get("id"));
		putIfPresent(output, "name", calendar.get("name"));
		putIfPresent(output, "color", calendar.get("color"));
		putIfPresent(output, "owner", addressOfEmail(calendar.get("owner")));
		putIfPresent(output, "ownerName", nameOfEmail(calendar.get("owner")));
		output.put("canEdit", Boolean.TRUE.equals(calendar.get("canEdit")));
		output.put("canShare", Boolean.TRUE.equals(calendar.get("canShare")));
		output.put("isDefaultCalendar", Boolean.TRUE.equals(calendar.get("isDefaultCalendar")));
		return output;
	}

	/**
	 * Describe the free and busy view of one mailbox.
	 *
	 * <p>
	 * The availability view is a string of digits, one for each slot of the window,
	 * where 0 is free and anything else is some degree of busy. It comes through as
	 * it is because reading it that way is cheaper than reading the items.
	 * </p>
	 *
	 * @param schedule the entry as Graph returned it
	 * @return the entry as a map
	 */
	public static Map<String, Object> toSchedule(Map<String, Object> schedule) {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("scheduleId", schedule.get("scheduleId"));
		putIfPresent(output, "availabilityView", schedule.get("availabilityView"));
		putIfPresent(output, "error", errorOf(schedule.get("error")));

		List<Map<String, Object>> items = new ArrayList<>();
		Object scheduleItems = schedule.get("scheduleItems");
		if (scheduleItems instanceof List) {
			for (Object scheduleItem : (List<?>) scheduleItems) {
				if (!(scheduleItem instanceof Map)) {
					continue;
				}
				Map<?, ?> item = (Map<?, ?>) scheduleItem;
				Map<String, Object> described = new LinkedHashMap<>();
				putMoment(described, "start", item.get("start"));
				putMoment(described, "end", item.get("end"));
				putIfPresent(described, "status", item.get("status"));
				putIfPresent(described, "subject", item.get("subject"));
				putIfPresent(described, "location", item.get("location"));
				items.add(described);
			}
		}
		output.put("scheduleItems", items);
		return output;
	}

	/**
	 * The readable text of an event, preferring what Graph says is plain over
	 * markup, the same way the mail mapper does.
	 *
	 * @param event the event as Graph returned it
	 * @return the body text, empty when there is none
	 */
	public static String bodyOf(Map<String, Object> event) {
		Object body = event.get("body");
		if (!(body instanceof Map)) {
			Object preview = event.get("bodyPreview");
			return preview == null ? "" : preview.toString().trim();
		}
		Map<?, ?> bodyMap = (Map<?, ?>) body;
		String content = bodyMap.get("content") == null ? "" : bodyMap.get("content").toString();
		if ("html".equalsIgnoreCase(String.valueOf(bodyMap.get("contentType")))) {
			// the markup is noise to whoever asked what the meeting is about
			return Jsoup.parse(content).text().trim();
		}
		return content.trim();
	}

	/**
	 * Set a moment and the zone it is to be read in, out of the pair Graph answers
	 * with.
	 *
	 * @param output the map being built
	 * @param key    the key the moment is set under, with the zone alongside it
	 * @param moment the {@code dateTimeTimeZone} as Graph returned it
	 */
	private static void putMoment(Map<String, Object> output, String key, Object moment) {
		if (!(moment instanceof Map)) {
			return;
		}
		Map<?, ?> momentMap = (Map<?, ?>) moment;
		putIfPresent(output, key, momentMap.get(DATE_TIME));
		putIfPresent(output, key + "TimeZone", momentMap.get(TIME_ZONE));
	}

	/**
	 * The attendees of an event, each with how they replied.
	 *
	 * @param attendees the collection as Graph returned it
	 * @return the attendees, empty when there are none
	 */
	private static List<Map<String, Object>> attendees(Object attendees) {
		List<Map<String, Object>> described = new ArrayList<>();
		if (!(attendees instanceof List)) {
			return described;
		}
		for (Object entry : (List<?>) attendees) {
			if (!(entry instanceof Map)) {
				continue;
			}
			Map<?, ?> attendee = (Map<?, ?>) entry;
			Map<String, Object> output = new LinkedHashMap<>();
			putIfPresent(output, ADDRESS, addressOfEmail(attendee.get(EMAIL_ADDRESS)));
			putIfPresent(output, NAME, nameOfEmail(attendee.get(EMAIL_ADDRESS)));
			putIfPresent(output, "type", attendee.get("type"));
			putIfPresent(output, "response", responseOf(attendee.get("status")));
			if (!output.isEmpty()) {
				described.add(output);
			}
		}
		return described;
	}

	/**
	 * The address out of something carrying an {@code emailAddress}, such as an
	 * organizer.
	 *
	 * @param holder the object as Graph returned it
	 * @return the address, or null when there is none
	 */
	private static String addressOf(Object holder) {
		if (!(holder instanceof Map)) {
			return null;
		}
		return addressOfEmail(((Map<?, ?>) holder).get(EMAIL_ADDRESS));
	}

	/**
	 * The display name out of something carrying an {@code emailAddress}.
	 *
	 * @param holder the object as Graph returned it
	 * @return the name, or null when there is none
	 */
	private static String nameOf(Object holder) {
		if (!(holder instanceof Map)) {
			return null;
		}
		return nameOfEmail(((Map<?, ?>) holder).get(EMAIL_ADDRESS));
	}

	/**
	 * @param emailAddress the {@code emailAddress} as Graph returned it
	 * @return the address, or null when there is none
	 */
	private static String addressOfEmail(Object emailAddress) {
		if (!(emailAddress instanceof Map)) {
			return null;
		}
		Object address = ((Map<?, ?>) emailAddress).get(ADDRESS);
		return address == null ? null : address.toString();
	}

	/**
	 * @param emailAddress the {@code emailAddress} as Graph returned it
	 * @return the display name, or null when there is none
	 */
	private static String nameOfEmail(Object emailAddress) {
		if (!(emailAddress instanceof Map)) {
			return null;
		}
		Object name = ((Map<?, ?>) emailAddress).get(NAME);
		return name == null ? null : name.toString();
	}

	/**
	 * @param location the {@code location} as Graph returned it
	 * @return where it is, or null when nowhere was set
	 */
	private static String displayNameOf(Object location) {
		if (!(location instanceof Map)) {
			return null;
		}
		Object displayName = ((Map<?, ?>) location).get(DISPLAY_NAME);
		if (displayName == null || displayName.toString().trim().isEmpty()) {
			return null;
		}
		return displayName.toString();
	}

	/**
	 * @param onlineMeeting the {@code onlineMeeting} as Graph returned it
	 * @return the link somebody joins by, or null when there is no meeting
	 */
	private static String joinUrlOf(Object onlineMeeting) {
		if (!(onlineMeeting instanceof Map)) {
			return null;
		}
		Object joinUrl = ((Map<?, ?>) onlineMeeting).get("joinUrl");
		return joinUrl == null ? null : joinUrl.toString();
	}

	/**
	 * @param status the {@code responseStatus} as Graph returned it
	 * @return how it was replied to, or null when there is no reply
	 */
	private static String responseOf(Object status) {
		if (!(status instanceof Map)) {
			return null;
		}
		Object response = ((Map<?, ?>) status).get("response");
		return response == null ? null : response.toString();
	}

	/**
	 * @param error the {@code error} as Graph returned it against one mailbox
	 * @return what went wrong reading that mailbox, or null when nothing did
	 */
	private static String errorOf(Object error) {
		if (!(error instanceof Map)) {
			return null;
		}
		Map<?, ?> errorMap = (Map<?, ?>) error;
		return errorMap.get("responseCode") + ": " + errorMap.get("message");
	}

	/**
	 * Set a value, and only when there is one.
	 *
	 * @param output the map being built
	 * @param key    the key to set
	 * @param value  the value, which is left out when it is null
	 */
	private static void putIfPresent(Map<String, Object> output, String key, Object value) {
		if (value != null) {
			output.put(key, value);
		}
	}

}
