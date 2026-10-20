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

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.jsoup.Jsoup;

import prerna.io.connector.ConnectorTimes;
import prerna.io.connector.calendar.Availability;
import prerna.io.connector.calendar.CalendarEvent;
import prerna.io.connector.calendar.CalendarInfo;
import prerna.io.connector.calendar.CalendarPermission;
import prerna.io.connector.calendar.EventAttendee;
import prerna.io.connector.calendar.EventTime;
import prerna.util.ValueUtils;

/**
 * Turns the json Graph returns for a calendar, an event, a permission or a
 * schedule into the records every calendar reactor answers with.
 *
 * <p>
 * Every read asks Graph for its times in UTC, so a time here without an offset
 * is a UTC time.
 * </p>
 */
public class MicrosoftCalendarEventMapper {

	private static final String ADDRESS = "address";
	private static final String NAME = "name";
	private static final String DATE_TIME = "dateTime";
	private static final String EMAIL_ADDRESS = "emailAddress";
	private static final String DISPLAY_NAME = "displayName";

	private MicrosoftCalendarEventMapper() {

	}

	/**
	 * Describe one event.
	 *
	 * @param event the event as Graph returned it
	 * @return the event
	 */
	public static CalendarEvent toEvent(Map<String, Object> event) {
		boolean isAllDay = Boolean.TRUE.equals(event.get("isAllDay"));
		Object categories = event.get("categories");
		Object reminder = event.get("reminderMinutesBeforeStart");
		return new CalendarEvent(ValueUtils.toStringOrNull(event.get("id")),
				ValueUtils.toStringOrNull(event.get("subject")),
				timeOf(event.get("start"), isAllDay, event.get("originalStartTimeZone")),
				timeOf(event.get("end"), isAllDay, event.get("originalEndTimeZone")),
				ValueUtils.toStringOrNull(event.get("originalStartTimeZone")), isAllDay,
				displayNameOf(event.get("location")), addressOf(event.get("organizer")), nameOf(event.get("organizer")),
				attendees(event.get("attendees")), ValueUtils.toStringOrNull(event.get("webLink")),
				joinUrlOf(event.get("onlineMeeting")), Boolean.TRUE.equals(event.get("isOnlineMeeting")),
				Boolean.TRUE.equals(event.get("isCancelled")), ValueUtils.toStringOrNull(event.get("showAs")),
				ValueUtils.toStringOrNull(event.get("importance")), responseOf(event.get("responseStatus")),
				reminder instanceof Number ? ((Number) reminder).intValue() : null, stringList(categories),
				// a series master is the rule, and an occurrence or an exception is one
				// sitting of it, so this says whether the event repeats at all
				event.get("seriesMasterId") != null || "seriesMaster".equals(event.get("type")), bodyOf(event));
	}

	/**
	 * Describe one calendar.
	 *
	 * <p>
	 * A calendar in the signed in user's list that somebody else owns is one they
	 * were shared, so {@code isSharedWithMe} is the owner's address not being
	 * theirs. What they may do with it is already in {@code canEdit} and
	 * {@code canViewPrivateItems}, which Outlook sets from the permission the owner
	 * granted.
	 * </p>
	 *
	 * @param calendar  the calendar as Graph returned it
	 * @param userEmail optional address of the signed in user; without it the owner
	 *                  is still reported and only {@code isSharedWithMe} is left
	 *                  out
	 * @return the calendar
	 */
	public static CalendarInfo toCalendar(Map<String, Object> calendar, String userEmail) {
		String owner = addressOfEmail(calendar.get("owner"));
		Boolean isSharedWithMe = userEmail != null && !userEmail.trim().isEmpty() && owner != null
				? !owner.equalsIgnoreCase(userEmail.trim())
				: null;
		return new CalendarInfo(ValueUtils.toStringOrNull(calendar.get("id")),
				ValueUtils.toStringOrNull(calendar.get("name")), ValueUtils.toStringOrNull(calendar.get("color")),
				owner, nameOfEmail(calendar.get("owner")), Boolean.TRUE.equals(calendar.get("canEdit")),
				Boolean.TRUE.equals(calendar.get("canShare")), Boolean.TRUE.equals(calendar.get("canViewPrivateItems")),
				Boolean.TRUE.equals(calendar.get("isDefaultCalendar")), isSharedWithMe);
	}

	/**
	 * Describe one permission on a calendar.
	 *
	 * <p>
	 * The {@code role} is the part worth reading. A {@code freeBusyRead},
	 * {@code limitedRead}, {@code read} or {@code write} role is a calendar that
	 * was shared, while a {@code delegateWithoutPrivateEventAccess} or
	 * {@code delegateWithPrivateEventAccess} role is a delegation: that person may
	 * also answer meeting requests on the owner's behalf.
	 * </p>
	 *
	 * @param permission the permission as Graph returned it
	 * @return the permission
	 */
	public static CalendarPermission toPermission(Map<String, Object> permission) {
		String role = ValueUtils.toStringOrNull(permission.get("role"));
		return new CalendarPermission(ValueUtils.toStringOrNull(permission.get("id")), role,
				addressOfEmail(permission.get("emailAddress")), nameOfEmail(permission.get("emailAddress")),
				stringList(permission.get("allowedRoles")), Boolean.TRUE.equals(permission.get("isInsideOrganization")),
				Boolean.TRUE.equals(permission.get("isRemovable")),
				// a delegate is a share plus the right to act for the owner, and the role is
				// the only thing that says which of the two this is
				role != null && role.toLowerCase(Locale.ROOT).startsWith("delegate"));
	}

	/**
	 * Describe when one mailbox is busy.
	 *
	 * <p>
	 * Graph reports every item in the window, free ones included, and says what the
	 * mailbox is busy with only where its owner shares that much. Only what is not
	 * free is kept.
	 * </p>
	 *
	 * @param schedule the entry as Graph returned it
	 * @return the availability
	 */
	public static Availability toAvailability(Map<String, Object> schedule) {
		List<Availability.Busy> busy = new ArrayList<>();
		if (schedule.get("scheduleItems") instanceof List<?> items) {
			for (Object entry : items) {
				if (!(entry instanceof Map<?, ?> item)) {
					continue;
				}
				String status = ValueUtils.toStringOrNull(item.get("status"));
				if ("free".equalsIgnoreCase(status)) {
					continue;
				}
				EventTime start = timeOf(item.get("start"), false, null);
				EventTime end = timeOf(item.get("end"), false, null);
				if (start == null || end == null) {
					continue;
				}
				busy.add(new Availability.Busy(start.instant(), end.instant(), status,
						ValueUtils.toStringOrNull(item.get("subject")),
						ValueUtils.toStringOrNull(item.get("location"))));
			}
		}
		return new Availability(ValueUtils.toStringOrNull(schedule.get("scheduleId")), busy,
				errorOf(schedule.get("error")));
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
	 * Read a moment out of the {@code dateTimeTimeZone} Graph answers with.
	 *
	 * <p>
	 * A whole day event is kept from midnight to midnight in its own zone, so read
	 * in UTC it lands a few hours either side of midnight. When the event's zone is
	 * one Java knows, the day is read in it. Outlook usually names it the Windows
	 * way, which Java does not know, and then the day is the one the UTC time is
	 * nearest to: midnight anywhere west of UTC falls early on the day in UTC, and
	 * anywhere from one to thirteen hours east of it falls at 11:00 or later the
	 * day before. Only zones more than ten hours west of UTC or fourteen hours east
	 * of it, which are a few islands, would read as the wrong day.
	 * </p>
	 *
	 * @param moment   the {@code dateTimeTimeZone} as Graph returned it
	 * @param wholeDay whether the event covers whole days
	 * @param zone     the zone the event was created in, as Graph names it, or null
	 * @return the time, or null when there is none
	 */
	private static EventTime timeOf(Object moment, boolean wholeDay, Object zone) {
		if (!(moment instanceof Map<?, ?> momentMap)) {
			return null;
		}
		Instant instant = ConnectorTimes.parseProviderTime(ValueUtils.toStringOrNull(momentMap.get(DATE_TIME)));
		if (instant == null) {
			return null;
		}
		if (!wholeDay) {
			return EventTime.of(instant);
		}
		ZoneId eventZone = knownZone(zone);
		if (eventZone != null) {
			return EventTime.ofDate(LocalDateTime.ofInstant(instant, eventZone).plusHours(12).toLocalDate());
		}
		LocalDateTime utc = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
		LocalDate date = utc.getHour() >= 11 ? utc.toLocalDate().plusDays(1) : utc.toLocalDate();
		return EventTime.ofDate(date);
	}

	/**
	 * @param zone a zone as Graph names it
	 * @return the zone, when it is UTC or an IANA zone, or null for a Windows name
	 */
	private static ZoneId knownZone(Object zone) {
		if (zone == null) {
			return null;
		}
		String name = zone.toString().trim();
		if (name.equalsIgnoreCase("UTC") || name.equalsIgnoreCase("tzone://Microsoft/Utc")) {
			return ZoneOffset.UTC;
		}
		try {
			return ZoneId.of(name);
		} catch (DateTimeException e) {
			return null;
		}
	}

	private static List<EventAttendee> attendees(Object attendees) {
		List<EventAttendee> described = new ArrayList<>();
		if (!(attendees instanceof List<?> entries)) {
			return described;
		}
		for (Object entry : entries) {
			if (!(entry instanceof Map<?, ?> attendee)) {
				continue;
			}
			described.add(new EventAttendee(addressOfEmail(attendee.get(EMAIL_ADDRESS)),
					nameOfEmail(attendee.get(EMAIL_ADDRESS)), ValueUtils.toStringOrNull(attendee.get("type")),
					responseOf(attendee.get("status"))));
		}
		return described;
	}

	private static String addressOf(Object holder) {
		return holder instanceof Map<?, ?> map ? addressOfEmail(map.get(EMAIL_ADDRESS)) : null;
	}

	private static String nameOf(Object holder) {
		return holder instanceof Map<?, ?> map ? nameOfEmail(map.get(EMAIL_ADDRESS)) : null;
	}

	private static String addressOfEmail(Object emailAddress) {
		return emailAddress instanceof Map<?, ?> map ? ValueUtils.toStringOrNull(map.get(ADDRESS)) : null;
	}

	private static String nameOfEmail(Object emailAddress) {
		return emailAddress instanceof Map<?, ?> map ? ValueUtils.toStringOrNull(map.get(NAME)) : null;
	}

	private static String displayNameOf(Object location) {
		if (!(location instanceof Map<?, ?> map)) {
			return null;
		}
		String displayName = ValueUtils.toStringOrNull(map.get(DISPLAY_NAME));
		return displayName == null || displayName.trim().isEmpty() ? null : displayName;
	}

	private static String joinUrlOf(Object onlineMeeting) {
		return onlineMeeting instanceof Map<?, ?> map ? ValueUtils.toStringOrNull(map.get("joinUrl")) : null;
	}

	private static String responseOf(Object status) {
		return status instanceof Map<?, ?> map ? ValueUtils.toStringOrNull(map.get("response")) : null;
	}

	private static String errorOf(Object error) {
		if (!(error instanceof Map<?, ?> map)) {
			return null;
		}
		return map.get("responseCode") + ": " + map.get("message");
	}

	private static List<String> stringList(Object values) {
		if (!(values instanceof List<?> list)) {
			return null;
		}
		return list.stream().filter(value -> value != null).map(Object::toString).toList();
	}

}
