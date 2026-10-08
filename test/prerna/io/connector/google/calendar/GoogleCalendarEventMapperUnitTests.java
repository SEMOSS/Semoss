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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.io.connector.calendar.CalendarEvent;
import prerna.io.connector.calendar.EventRequest;
import prerna.io.connector.calendar.EventTime;
import prerna.io.connector.calendar.Recurrence;

class GoogleCalendarEventMapperUnitTests {

	private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");

	@Test
	void readsAnEventIntoTheSharedShape() {
		CalendarEvent event = GoogleCalendarEventMapper.toEvent(Map.of("id", "e1", "summary", "Planning", "start",
				Map.of("dateTime", "2026-10-05T09:00:00-04:00", "timeZone", "America/New_York"), "end",
				Map.of("dateTime", "2026-10-05T10:00:00-04:00", "timeZone", "America/New_York"), "attendees",
				List.of(Map.of("email", "me@example.com", "self", true, "responseStatus", "tentative"),
						Map.of("email", "bob@example.com", "optional", true, "responseStatus", "needsAction")),
				"organizer", Map.of("email", "ada@example.com"), "transparency", "transparent", "colorId", "5",
				"hangoutLink", "https://meet.google.com/abc", "recurringEventId", "r1"), null);

		Map<String, Object> output = event.toMap(false, 0);
		assertEquals("2026-10-05T13:00:00Z", output.get("start"));
		assertEquals("2026-10-05T14:00:00Z", output.get("end"));
		assertEquals("America/New_York", output.get("timeZone"));
		assertEquals("free", output.get("showAs"));
		assertEquals(List.of("5"), output.get("categories"));
		assertEquals("tentativelyAccepted", output.get("responseStatus"));
		assertEquals("https://meet.google.com/abc", output.get("joinUrl"));
		assertEquals(true, output.get("isOnlineMeeting"));
		assertEquals(true, output.get("isRecurring"));
		assertEquals("optional", event.attendees().get(1).type());
		assertEquals("notResponded", event.attendees().get(1).response());
	}

	@Test
	void aWholeDayEventIsWrittenAsDates() {
		CalendarEvent event = GoogleCalendarEventMapper.toEvent(
				Map.of("id", "e2", "start", Map.of("date", "2026-10-05"), "end", Map.of("date", "2026-10-06")),
				"America/New_York");
		assertTrue(event.isAllDay());
		assertEquals("2026-10-05", event.toMap(false, 0).get("start"));
		assertEquals("America/New_York", event.timeZone());
	}

	@Test
	void writesTimesInTheEventsZoneAndASeriesThatEndsAtTheLastMomentOfItsLastDay() {
		EventRequest request = new EventRequest(null, null, null, "Planning", null, false,
				EventTime.of(Instant.parse("2026-10-05T13:00:00Z")),
				EventTime.of(Instant.parse("2026-10-05T14:00:00Z")), NEW_YORK, null, null, null, null, null, null,
				"free", null, List.of("5"), new Recurrence(Recurrence.WEEKLY, LocalDate.parse("2026-12-31")));
		Map<String, Object> event = GoogleCalendarEventMapper.buildEvent(request);
		assertEquals(Map.of("dateTime", "2026-10-05T09:00:00-04:00", "timeZone", "America/New_York"),
				event.get("start"));
		assertEquals("transparent", event.get("transparency"));
		assertEquals("5", event.get("colorId"));
		assertEquals(List.of("RRULE:FREQ=WEEKLY;UNTIL=20270101T045959Z"), event.get("recurrence"));
	}

	@Test
	void acceptsOnlyWhatAGoogleCalendarKeeps() {
		assertThrows(IllegalArgumentException.class,
				() -> GoogleCalendarEventMapper.checkEventValues(request("tentative", null)));
		assertThrows(IllegalArgumentException.class,
				() -> GoogleCalendarEventMapper.checkEventValues(request(null, List.of("Red Category"))));
		assertThrows(IllegalArgumentException.class,
				() -> GoogleCalendarEventMapper.checkEventValues(request(null, List.of("1", "2"))));
		GoogleCalendarEventMapper.checkEventValues(request("Busy", List.of("11")));
	}

	@Test
	void anAnswerMarksOnlyTheUsersOwnEntry() {
		List<Map<String, Object>> attendees = GoogleCalendarEventMapper.answer(
				Map.of("attendees",
						List.of(Map.of("email", "me@example.com", "responseStatus", "needsAction"),
								Map.of("email", "bob@example.com", "responseStatus", "accepted"))),
				"ME@example.com", "decline", "Out that day");
		assertEquals("declined", attendees.get(0).get("responseStatus"));
		assertEquals("Out that day", attendees.get(0).get("comment"));
		assertEquals("accepted", attendees.get(1).get("responseStatus"));
		assertThrows(IllegalArgumentException.class, () -> GoogleCalendarEventMapper
				.answer(Map.of("attendees", List.of()), "me@example.com", "accept", null));
	}

	private static EventRequest request(String showAs, List<String> categories) {
		return new EventRequest("e1", null, null, null, null, false, null, null, NEW_YORK, null, null, null, null, null,
				null, showAs, null, categories, null);
	}
}
