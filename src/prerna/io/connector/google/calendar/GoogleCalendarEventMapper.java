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
package prerna.io.connector.google.calendar;

import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import prerna.io.connector.ConnectorTimes;
import prerna.io.connector.calendar.Availability;
import prerna.io.connector.calendar.CalendarEvent;
import prerna.io.connector.calendar.CalendarInfo;
import prerna.io.connector.calendar.CalendarPermission;
import prerna.io.connector.calendar.EventAttendee;
import prerna.io.connector.calendar.EventRequest;
import prerna.io.connector.calendar.EventTime;
import prerna.io.connector.calendar.Recurrence;
import prerna.io.connector.ms.MicrosoftMessageDisplay;
import prerna.util.ValueUtils;

/**
 * Turns the json Google Calendar returns into the records every calendar
 * reactor answers with, and an event a caller wrote into the json Google reads.
 *
 * <p>
 * Where both calendars keep the same thing under different names, the record
 * uses Outlook's: an answer of {@code needsAction} is {@code notResponded}, an
 * event that does not block time shows as {@code free}, and the one color an
 * event can carry is its category.
 * </p>
 */
public final class GoogleCalendarEventMapper {

	/**
	 * What an event can show as on a Google calendar: it blocks time, or it does
	 * not.
	 */
	public static final List<String> SHOW_AS_VALUES = List.of("free", "busy");

	/** The ids of the colors a Google Calendar event can carry. */
	public static final int MAX_COLOR_ID = 11;

	private static final DateTimeFormatter RRULE_DATE = DateTimeFormatter.ofPattern("yyyyMMdd");
	private static final DateTimeFormatter RRULE_TIME = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'");

	private GoogleCalendarEventMapper() {

	}

	/**
	 * Check the values a Google Calendar event accepts that another calendar may
	 * not.
	 *
	 * @param request the event the caller wrote
	 * @throws IllegalArgumentException when showAs is not free or busy, or the
	 *                                  categories are not one color id
	 */
	public static void checkEventValues(EventRequest request) {
		String showAs = request.showAs();
		if (showAs != null && SHOW_AS_VALUES.stream().noneMatch(value -> value.equalsIgnoreCase(showAs.trim()))) {
			throw new IllegalArgumentException("showAs must be one of " + SHOW_AS_VALUES
					+ " on a Google Calendar, which only records whether an event blocks time, but received: "
					+ showAs);
		}
		List<String> categories = request.categories();
		if (categories == null || categories.isEmpty()) {
			return;
		}
		if (categories.size() > 1) {
			throw new IllegalArgumentException("A Google Calendar event carries one color, so pass a single category.");
		}
		String color = categories.get(0).trim();
		int colorId;
		try {
			colorId = Integer.parseInt(color);
		} catch (NumberFormatException e) {
			colorId = -1;
		}
		if (colorId < 1 || colorId > MAX_COLOR_ID) {
			throw new IllegalArgumentException("categories must be an event color id from 1 to " + MAX_COLOR_ID
					+ " on a Google Calendar, but received: " + color);
		}
	}

	/**
	 * @param key showAs or categories
	 * @return what a Google Calendar event accepts for it
	 */
	public static String describeEventValues(String key) {
		if ("showAs".equals(key)) {
			return "Either free or busy, since a Google Calendar only records whether an event blocks time.";
		}
		return "A single event color, as its id from 1 to " + MAX_COLOR_ID
				+ ", which is how a Google Calendar tags an event.";
	}

	/**
	 * Describe one event.
	 *
	 * @param event        the event as Google returned it
	 * @param calendarZone the calendar's own zone, for an event that names none
	 * @return the event
	 */
	public static CalendarEvent toEvent(Map<String, Object> event, String calendarZone) {
		Map<?, ?> start = event.get("start") instanceof Map<?, ?> map ? map : Map.of();
		Map<?, ?> end = event.get("end") instanceof Map<?, ?> map ? map : Map.of();
		boolean isAllDay = start.get("date") != null;
		String joinUrl = joinUrlOf(event);
		Map<?, ?> organizer = event.get("organizer") instanceof Map<?, ?> map ? map : Map.of();
		Object colorId = event.get("colorId");
		String zone = start.get("timeZone") == null ? calendarZone : start.get("timeZone").toString();
		return new CalendarEvent(ValueUtils.toStringOrNull(event.get("id")),
				ValueUtils.toStringOrNull(event.get("summary")), timeOf(start), timeOf(end), zone, isAllDay,
				ValueUtils.toStringOrNull(event.get("location")), ValueUtils.toStringOrNull(organizer.get("email")),
				ValueUtils.toStringOrNull(organizer.get("displayName")), attendees(event.get("attendees")),
				ValueUtils.toStringOrNull(event.get("htmlLink")), joinUrl, joinUrl != null,
				"cancelled".equals(event.get("status")),
				"transparent".equals(event.get("transparency")) ? "free" : "busy", null, ownResponse(event, organizer),
				reminderOf(event.get("reminders")), colorId == null ? List.of() : List.of(colorId.toString()),
				event.get("recurringEventId") != null || event.get("recurrence") != null,
				descriptionOf(event.get("description")));
	}

	/**
	 * Describe one calendar in the user's list.
	 *
	 * @param entry   the calendar list entry as Google returned it
	 * @param account the signed in user's address
	 * @return the calendar
	 */
	public static CalendarInfo toCalendar(Map<String, Object> entry, String account) {
		String id = ValueUtils.toStringOrNull(entry.get("id"));
		String role = ValueUtils.toStringOrNull(entry.get("accessRole"));
		boolean primary = Boolean.TRUE.equals(entry.get("primary"));
		boolean owns = "owner".equals(role);
		boolean writes = owns || "writer".equals(role);
		// a calendar of the user's own is theirs, and somebody else's primary calendar
		// is named by their address
		String owner = primary || owns ? account
				: id != null && id.contains("@") && !id.endsWith("calendar.google.com") ? id : null;
		String name = entry.get("summaryOverride") != null ? ValueUtils.toStringOrNull(entry.get("summaryOverride"))
				: ValueUtils.toStringOrNull(entry.get("summary"));
		return new CalendarInfo(id, name, ValueUtils.toStringOrNull(entry.get("backgroundColor")), owner, null, writes,
				owns, writes, primary, !owns);
	}

	/**
	 * Describe somebody else's calendar the user can reach but has not added to
	 * their list.
	 *
	 * @param calendar the calendar as Google returned it
	 * @return the calendar
	 */
	public static CalendarInfo toSharedCalendar(Map<String, Object> calendar) {
		String id = ValueUtils.toStringOrNull(calendar.get("id"));
		return new CalendarInfo(id, ValueUtils.toStringOrNull(calendar.get("summary")), null, id, null, false, false,
				false, false, true);
	}

	/**
	 * Describe one rule of who a calendar is shared with.
	 *
	 * @param rule the rule as Google returned it
	 * @return the permission
	 */
	public static CalendarPermission toPermission(Map<String, Object> rule) {
		Map<?, ?> scope = rule.get("scope") instanceof Map<?, ?> map ? map : Map.of();
		String role = roleOf(ValueUtils.toStringOrNull(rule.get("role")));
		return new CalendarPermission(ValueUtils.toStringOrNull(rule.get("id")), role,
				ValueUtils.toStringOrNull(scope.get("value")), null, null, "domain".equals(scope.get("type")),
				!"owner".equals(role), false);
	}

	/**
	 * Describe when each person is busy.
	 *
	 * @param response  the free and busy answer as Google returned it
	 * @param schedules the people asked about, in the order they were asked
	 * @return one entry for each of them
	 */
	public static List<Availability> toAvailability(Map<String, Object> response, List<String> schedules) {
		Map<?, ?> calendars = response != null && response.get("calendars") instanceof Map<?, ?> map ? map : Map.of();
		List<Availability> entries = new ArrayList<>();
		for (String address : schedules) {
			Map<?, ?> calendar = calendars.get(address) instanceof Map<?, ?> map ? map : Map.of();
			List<Availability.Busy> busy = new ArrayList<>();
			if (calendar.get("busy") instanceof List<?> periods) {
				for (Object period : periods) {
					if (period instanceof Map<?, ?> times) {
						busy.add(new Availability.Busy(
								ConnectorTimes.parseProviderTime(ValueUtils.toStringOrNull(times.get("start"))),
								ConnectorTimes.parseProviderTime(ValueUtils.toStringOrNull(times.get("end"))), "busy",
								null, null));
					}
				}
			}
			String error = null;
			if (calendar.get("errors") instanceof List<?> errors && !errors.isEmpty()
					&& errors.get(0) instanceof Map<?, ?> first) {
				error = ValueUtils.toStringOrNull(first.get("reason"));
			}
			entries.add(new Availability(address, busy, error));
		}
		return entries;
	}

	/**
	 * Build an event in the shape Google reads, setting only what the request sets,
	 * which is what lets the same method build a change as well as a create.
	 *
	 * <p>
	 * Times are written with the event's own zone, so the calendar keeps the event
	 * in it and a series keeps its local time across daylight saving changes.
	 * </p>
	 *
	 * @param request the event
	 * @return the event in the shape Google reads
	 */
	public static Map<String, Object> buildEvent(EventRequest request) {
		ZoneId zone = request.zone() == null ? ZoneOffset.UTC : request.zone();
		Map<String, Object> event = new LinkedHashMap<>();
		if (request.subject() != null) {
			event.put("summary", request.subject());
		}
		if (request.body() != null) {
			event.put("description", request.body());
		}
		if (request.start() != null) {
			event.put("start", moment(request.start(), zone));
		}
		if (request.end() != null) {
			event.put("end", moment(request.end(), zone));
		}
		if (request.location() != null) {
			event.put("location", request.location());
		}
		if (request.setsAttendees()) {
			List<Map<String, Object>> invited = new ArrayList<>();
			addAttendees(invited, request.attendees(), false);
			addAttendees(invited, request.optionalAttendees(), true);
			event.put("attendees", invited);
		}
		if (Boolean.TRUE.equals(request.isOnlineMeeting())) {
			event.put("conferenceData", Map.of("createRequest", Map.of("requestId", UUID.randomUUID().toString(),
					"conferenceSolutionKey", Map.of("type", "hangoutsMeet"))));
		}
		if (request.reminderMinutes() != null) {
			event.put("reminders", Map.of("useDefault", false, "overrides",
					List.of(Map.of("method", "popup", "minutes", request.reminderMinutes()))));
		}
		if (request.showAs() != null) {
			event.put("transparency", "free".equalsIgnoreCase(request.showAs().trim()) ? "transparent" : "opaque");
		}
		if (request.categories() != null && !request.categories().isEmpty()) {
			event.put("colorId", request.categories().get(0).trim());
		}
		if (request.recurrence() != null) {
			event.put("recurrence", List.of(rrule(request.recurrence(), request.start(), zone)));
		}
		return event;
	}

	/**
	 * Mark the user's own answer on an event's guest list.
	 *
	 * @param event    the event as Google returned it
	 * @param address  the address answering
	 * @param response accept, decline or tentative
	 * @param comment  a note to the organizer, or null
	 * @return the whole guest list, with the answer marked, which is what Google
	 *         takes to change it
	 * @throws IllegalArgumentException when the address is not on the guest list
	 */
	public static List<Map<String, Object>> answer(Map<String, Object> event, String address, String response,
			String comment) {
		List<Map<String, Object>> attendees = new ArrayList<>();
		if (event.get("attendees") instanceof List<?> guests) {
			for (Object guest : guests) {
				if (guest instanceof Map<?, ?> map) {
					Map<String, Object> attendee = new LinkedHashMap<>();
					map.forEach((key, value) -> attendee.put(String.valueOf(key), value));
					attendees.add(attendee);
				}
			}
		}
		// the address answering wins over whoever Google marks as the user, since on
		// a calendar shared with the user the two are different people
		Map<String, Object> answering = null;
		for (Map<String, Object> attendee : attendees) {
			if (address != null && address.equalsIgnoreCase(String.valueOf(attendee.get("email")))) {
				answering = attendee;
				break;
			}
		}
		if (answering == null) {
			answering = attendees.stream().filter(attendee -> Boolean.TRUE.equals(attendee.get("self"))).findFirst()
					.orElse(null);
		}
		if (answering == null) {
			throw new IllegalArgumentException("Only somebody invited to the event can answer it.");
		}
		answering.put("responseStatus",
				"tentative".equals(response) ? "tentative" : "accept".equals(response) ? "accepted" : "declined");
		if (comment != null) {
			answering.put("comment", comment);
		}
		return attendees;
	}

	/**
	 * The rule Google repeats a series by, on the weekday, the day of the month, or
	 * the date its first sitting falls on, which is what a rule with no day in it
	 * repeats on.
	 */
	private static String rrule(Recurrence recurrence, EventTime start, ZoneId zone) {
		StringBuilder rule = new StringBuilder("RRULE:FREQ=").append(recurrence.frequency().toUpperCase(Locale.ROOT));
		LocalDate until = recurrence.until();
		if (until != null) {
			// a series of whole days ends on a date, and any other ends at the last
			// moment of that day in its own zone, written in UTC
			rule.append(";UNTIL=")
					.append(start != null && start.isDate() ? until.format(RRULE_DATE)
							: until.plusDays(1).atStartOfDay(zone).minusSeconds(1).withZoneSameInstant(ZoneOffset.UTC)
									.format(RRULE_TIME));
		}
		return rule.toString();
	}

	private static Map<String, Object> moment(EventTime time, ZoneId zone) {
		Map<String, Object> moment = new LinkedHashMap<>();
		if (time.isDate()) {
			moment.put("date", time.date().toString());
		} else {
			moment.put("dateTime",
					time.instant().atZone(zone).toOffsetDateTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
			moment.put("timeZone", ConnectorTimes.providerZoneId(zone));
		}
		return moment;
	}

	private static void addAttendees(List<Map<String, Object>> invited, List<String> addresses, boolean optional) {
		if (addresses == null) {
			return;
		}
		for (String address : addresses) {
			Map<String, Object> attendee = new LinkedHashMap<>();
			attendee.put("email", address);
			if (optional) {
				attendee.put("optional", true);
			}
			invited.add(attendee);
		}
	}

	private static EventTime timeOf(Map<?, ?> moment) {
		if (moment.get("date") != null) {
			try {
				return EventTime.ofDate(LocalDate.parse(moment.get("date").toString()));
			} catch (DateTimeParseException e) {
				return null;
			}
		}
		if (moment.get("dateTime") != null) {
			try {
				return EventTime.of(OffsetDateTime.parse(moment.get("dateTime").toString()).toInstant());
			} catch (DateTimeParseException e) {
				return null;
			}
		}
		return null;
	}

	private static List<EventAttendee> attendees(Object attendees) {
		List<EventAttendee> described = new ArrayList<>();
		if (!(attendees instanceof List<?> guests)) {
			return described;
		}
		for (Object guest : guests) {
			if (!(guest instanceof Map<?, ?> attendee)) {
				continue;
			}
			String type = Boolean.TRUE.equals(attendee.get("resource")) ? "resource"
					: Boolean.TRUE.equals(attendee.get("optional")) ? "optional" : "required";
			described.add(new EventAttendee(ValueUtils.toStringOrNull(attendee.get("email")),
					ValueUtils.toStringOrNull(attendee.get("displayName")), type,
					responseOf(ValueUtils.toStringOrNull(attendee.get("responseStatus")))));
		}
		return described;
	}

	private static String ownResponse(Map<String, Object> event, Map<?, ?> organizer) {
		if (Boolean.TRUE.equals(organizer.get("self"))) {
			return "organizer";
		}
		if (event.get("attendees") instanceof List<?> guests) {
			for (Object guest : guests) {
				if (guest instanceof Map<?, ?> attendee && Boolean.TRUE.equals(attendee.get("self"))) {
					return responseOf(ValueUtils.toStringOrNull(attendee.get("responseStatus")));
				}
			}
		}
		return null;
	}

	private static String responseOf(String status) {
		if (status == null) {
			return null;
		}
		switch (status) {
		case "needsAction":
			return "notResponded";
		case "tentative":
			return "tentativelyAccepted";
		default:
			return status;
		}
	}

	private static String roleOf(String role) {
		if (role == null) {
			return null;
		}
		switch (role) {
		case "freeBusyReader":
			return "freeBusyRead";
		case "reader":
			return "read";
		case "writer":
			return "write";
		default:
			return role;
		}
	}

	private static String joinUrlOf(Map<String, Object> event) {
		if (event.get("hangoutLink") != null) {
			return event.get("hangoutLink").toString();
		}
		if (event.get("conferenceData") instanceof Map<?, ?> conference
				&& conference.get("entryPoints") instanceof List<?> entryPoints) {
			for (Object entry : entryPoints) {
				if (entry instanceof Map<?, ?> point && "video".equals(point.get("entryPointType"))
						&& point.get("uri") != null) {
					return point.get("uri").toString();
				}
			}
		}
		return null;
	}

	private static Integer reminderOf(Object reminders) {
		if (!(reminders instanceof Map<?, ?> map) || !(map.get("overrides") instanceof List<?> overrides)) {
			return null;
		}
		Integer earliest = null;
		for (Object override : overrides) {
			if (override instanceof Map<?, ?> reminder && reminder.get("minutes") instanceof Number minutes) {
				earliest = earliest == null ? minutes.intValue() : Math.max(earliest, minutes.intValue());
			}
		}
		return earliest;
	}

	private static String descriptionOf(Object description) {
		if (description == null) {
			return "";
		}
		String text = description.toString();
		// google keeps a description written in its own editor as html
		if (text.contains("<") && text.contains(">")) {
			return MicrosoftMessageDisplay.text(Map.of("body", Map.of("contentType", "html", "content", text)));
		}
		return text.trim();
	}

}
