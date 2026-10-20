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
package prerna.io.connector.calendar;

import java.time.ZoneId;
import java.util.List;

import prerna.io.connector.ConnectorTimes;

/**
 * What the reactors that write an event have in common.
 *
 * <p>
 * Creating an event and changing one differ only in the provider call at the
 * end and in what has to be there: a create needs a start and an end, and a
 * change needs whichever fields are being changed and nothing else. Reading the
 * attendees, checking the times and assembling the event is the same, and is
 * here so the two cannot drift apart.
 * </p>
 *
 * <p>
 * Two keys every calendar takes accept different values in each: {@code showAs}
 * and {@code categories}. Each provider reactor checks them in
 * {@link #checkProviderValues(EventRequest)} and says what it accepts in
 * {@link #describeProviderValues(String)}, and neither can be left out.
 * </p>
 */
public abstract class AbstractEventWriteReactor extends AbstractCalendarReactor {

	/** The keys every event writing reactor takes, in the order it takes them. */
	protected static final String[] EVENT_KEYS = { SUBJECT, START, END, TIME_ZONE, IS_ALL_DAY, LOCATION, ATTENDEES,
			OPTIONAL_ATTENDEES, BODY, HTML, IS_ONLINE_MEETING, REMINDER_MINUTES, SHOW_AS, CATEGORIES, RECURRENCE,
			RECURRENCE_UNTIL, CALENDAR_ID, MAILBOX };

	/**
	 * Check the values only this provider decides, before anything is written.
	 *
	 * @param request the event the caller wrote
	 * @throws IllegalArgumentException when a value is one this calendar does not
	 *                                  accept
	 */
	protected abstract void checkProviderValues(EventRequest request);

	/**
	 * @param key {@code showAs} or {@code categories}
	 * @return what this calendar accepts for it, for the key's description
	 */
	protected abstract String describeProviderValues(String key);

	/**
	 * Read the event the caller wrote.
	 *
	 * @param id           the event being changed, or null for one being created
	 * @param requireTimes whether a start and an end have to be there, which they
	 *                     do to create an event and do not to change one
	 * @param toDo         what this is being read for, used in the errors
	 * @return the event
	 */
	protected final EventRequest readEvent(String id, boolean requireTimes, String toDo) {
		ZoneId zone = readZone();
		boolean allDay = Boolean.TRUE.equals(readOptionalBoolean(IS_ALL_DAY));
		String startValue = readString(START);
		String endValue = readString(END);
		if (requireTimes && startValue == null) {
			throw new IllegalArgumentException("A " + START + " is required to " + toDo + ".");
		}
		if (requireTimes && endValue == null) {
			throw new IllegalArgumentException("An " + END + " is required to " + toDo + ".");
		}
		EventTime start = readTime(startValue, allDay, zone);
		EventTime end = readTime(endValue, allDay, zone);
		if (start != null && end != null && !isBefore(start, end)) {
			throw new IllegalArgumentException("The " + END + " of an event has to be after its " + START + ".");
		}

		Integer reminderMinutes = readOptionalInt(REMINDER_MINUTES);
		if (reminderMinutes != null && reminderMinutes < 0) {
			throw new IllegalArgumentException(REMINDER_MINUTES + " cannot be negative.");
		}

		Recurrence recurrence = null;
		String frequency = readString(RECURRENCE);
		if (frequency != null) {
			if (start == null) {
				// the series repeats on the weekday or the date of its first sitting, so
				// there is nothing to repeat without one
				throw new IllegalArgumentException("A " + START + " is required to say how an event repeats.");
			}
			String until = readString(RECURRENCE_UNTIL);
			recurrence = Recurrence.of(frequency, until == null ? null : ConnectorTimes.readDate(until));
		}

		EventRequest request = new EventRequest(id, readString(CALENDAR_ID), readString(MAILBOX), readString(SUBJECT),
				this.keyValue.get(BODY), readBoolean(HTML, false), start, end, zone, readOptionalBoolean(IS_ALL_DAY),
				readString(LOCATION), readValues(ATTENDEES), readValues(OPTIONAL_ATTENDEES),
				readOptionalBoolean(IS_ONLINE_MEETING), reminderMinutes, readString(SHOW_AS), readString(IMPORTANCE),
				readValues(CATEGORIES), recurrence);
		if (request.isEmpty()) {
			throw new IllegalArgumentException("Nothing was passed to " + toDo + ".");
		}
		checkProviderValues(request);
		return request;
	}

	private static EventTime readTime(String value, boolean allDay, ZoneId zone) {
		if (value == null) {
			return null;
		}
		return allDay ? EventTime.ofDate(ConnectorTimes.readDate(value))
				: EventTime.of(ConnectorTimes.readMoment(value, zone));
	}

	private static boolean isBefore(EventTime start, EventTime end) {
		if (start.isDate() && end.isDate()) {
			return start.date().isBefore(end.date());
		}
		if (!start.isDate() && !end.isDate()) {
			return start.instant().isBefore(end.instant());
		}
		return true;
	}

	/**
	 * Check that a value is one of a fixed set of words, matched however the caller
	 * happened to capitalize it.
	 *
	 * @param key      the key the value came from
	 * @param value    the value, or null when it was left out
	 * @param accepted the words accepted, in the capitalization the provider uses
	 * @return the accepted word, or null when the value was left out
	 */
	protected static String requireOneOf(String key, String value, List<String> accepted) {
		if (value == null) {
			return null;
		}
		for (String candidate : accepted) {
			if (candidate.equalsIgnoreCase(value.trim())) {
				return candidate;
			}
		}
		throw new IllegalArgumentException(key + " must be one of " + accepted + " but received: " + value);
	}

	@Override
	protected String describeKey(String key) {
		switch (key) {
		case SUBJECT:
			return "Subject line of the event.";
		case BODY:
			return "Description of the event, describing what it is about.";
		case HTML:
			return "Optional boolean for whether the body is html rather than plain text. Defaults to false.";
		case IS_ALL_DAY:
			return "Optional boolean for whether the event covers whole days. The start and end are read as dates "
					+ "when it is true, and the end is the day after the last day.";
		case LOCATION:
			return "Optional place the event is held.";
		case ATTENDEES:
			return "Optional email addresses of the required attendees, as a list.";
		case OPTIONAL_ATTENDEES:
			return "Optional email addresses of the optional attendees, as a list.";
		case IS_ONLINE_MEETING:
			return "Optional boolean for whether an online meeting link is created for the event.";
		case REMINDER_MINUTES:
			return "Optional number of minutes before the start that the reminder fires.";
		case SHOW_AS:
			return "Optional way the time reads on the calendar. " + describeProviderValues(SHOW_AS);
		case CATEGORIES:
			return "Optional categories to tag the event with, as a list. " + describeProviderValues(CATEGORIES);
		case IMPORTANCE:
			return "Optional importance of the event, one of low, normal or high.";
		case RECURRENCE:
			return "Optional way the event repeats, one of " + Recurrence.FREQUENCIES + ", on the weekday or the "
					+ "date of its start. Pass timeZone with it, so the series keeps its local time across daylight "
					+ "saving changes.";
		case RECURRENCE_UNTIL:
			return "Optional last date the event repeats on, as an ISO 8601 date. It repeats with no end when omitted.";
		default:
			return super.describeKey(key);
		}
	}
}
