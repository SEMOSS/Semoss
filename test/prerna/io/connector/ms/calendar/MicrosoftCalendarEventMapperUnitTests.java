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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.io.connector.calendar.Availability;
import prerna.io.connector.calendar.CalendarEvent;
import prerna.io.connector.calendar.EventRequest;
import prerna.io.connector.calendar.EventTime;
import prerna.io.connector.calendar.Recurrence;

class MicrosoftCalendarEventMapperUnitTests {

	@Test
	void readsAnEventIntoTheSharedShapeInUtc() {
		CalendarEvent event = MicrosoftCalendarEventMapper
				.toEvent(Map.ofEntries(Map.entry("id", "e1"), Map.entry("subject", "Planning"),
						Map.entry("start", Map.of("dateTime", "2026-10-05T13:00:00.0000000", "timeZone", "UTC")),
						Map.entry("end", Map.of("dateTime", "2026-10-05T14:00:00.0000000", "timeZone", "UTC")),
						Map.entry("originalStartTimeZone", "Eastern Standard Time"),
						Map.entry("attendees",
								List.of(Map.of("emailAddress", Map.of("address", "bob@example.com", "name", "Bob"),
										"type", "required", "status", Map.of("response", "accepted")))),
						Map.entry("responseStatus", Map.of("response", "organizer")), Map.entry("importance", "high"),
						Map.entry("onlineMeeting", Map.of("joinUrl", "https://teams.example.com/j")),
						Map.entry("isOnlineMeeting", true), Map.entry("seriesMasterId", "s1")));

		Map<String, Object> output = event.toMap(false, 0);
		assertEquals("2026-10-05T13:00:00Z", output.get("start"));
		assertEquals("Eastern Standard Time", output.get("timeZone"));
		assertEquals("organizer", output.get("responseStatus"));
		assertEquals("high", output.get("importance"));
		assertEquals("https://teams.example.com/j", output.get("joinUrl"));
		assertEquals(true, output.get("isRecurring"));
		assertEquals("accepted", event.attendees().get(0).response());
	}

	@Test
	void aWholeDayEventIsTheDateItsUtcTimeIsNearest() {
		// midnight in a zone east of UTC reads as the evening before, and one west of
		// it as the morning of
		assertEquals("2026-10-05", allDayStart("2026-10-04T22:00:00.0000000"));
		assertEquals("2026-10-05", allDayStart("2026-10-05T04:00:00.0000000"));
		assertEquals("2026-10-05", allDayStart("2026-10-05T00:00:00.0000000"));
	}

	@Test
	void aWholeDayEventInAZoneJavaKnowsIsReadInThatZone() {
		// midnight in Auckland is late morning the day before in UTC, which the UTC
		// fallback alone could not place
		CalendarEvent event = MicrosoftCalendarEventMapper.toEvent(Map.of("id", "e", "isAllDay", true, "start",
				Map.of("dateTime", "2026-10-04T11:00:00.0000000", "timeZone", "UTC"), "end",
				Map.of("dateTime", "2026-10-05T11:00:00.0000000", "timeZone", "UTC"), "originalStartTimeZone",
				"Pacific/Auckland", "originalEndTimeZone", "Pacific/Auckland"));
		assertEquals("2026-10-05", event.toMap(false, 0).get("start"));
		assertEquals("2026-10-06", event.toMap(false, 0).get("end"));
	}

	@Test
	void writesTimesAsTheClockInTheEventsZoneReadsThem() {
		ZoneId newYork = ZoneId.of("America/New_York");
		EventRequest request = new EventRequest(null, null, null, "Planning", null, false,
				EventTime.of(Instant.parse("2026-10-05T13:00:00Z")),
				EventTime.of(Instant.parse("2026-10-05T14:00:00Z")), newYork, null, null, List.of("bob@example.com"),
				null, null, null, "Busy", null, null, new Recurrence(Recurrence.WEEKLY, LocalDate.parse("2026-12-31")));
		Map<String, Object> event = MicrosoftCalendarHelper.buildEvent(request);
		assertEquals(Map.of("dateTime", "2026-10-05T09:00:00", "timeZone", "America/New_York"), event.get("start"));
		assertEquals("busy", event.get("showAs"));
		@SuppressWarnings("unchecked")
		Map<String, Object> recurrence = (Map<String, Object>) event.get("recurrence");
		@SuppressWarnings("unchecked")
		Map<String, Object> pattern = (Map<String, Object>) recurrence.get("pattern");
		assertEquals(List.of("monday"), pattern.get("daysOfWeek"));
	}

	@Test
	void acceptsOnlyWhatOutlookKeeps() {
		assertThrows(IllegalArgumentException.class,
				() -> MicrosoftCalendarHelper.checkEventValues(new EventRequest("e1", null, null, null, null, false,
						null, null, null, null, null, null, null, null, null, "maybe", null, null, null)));
	}

	@Test
	void keepsOnlyWhatIsNotFree() {
		Availability availability = MicrosoftCalendarEventMapper.toAvailability(Map.of("scheduleId", "bob@example.com",
				"scheduleItems",
				List.of(item("busy", "2026-10-05T13:00:00.0000000"), item("free", "2026-10-05T15:00:00.0000000"))));
		assertEquals(1, availability.busy().size());
		assertEquals(Instant.parse("2026-10-05T13:00:00Z"), availability.busy().get(0).start());
		assertEquals("busy", availability.busy().get(0).status());
	}

	private static Map<String, Object> item(String status, String start) {
		return Map.of("status", status, "start", Map.of("dateTime", start, "timeZone", "UTC"), "end",
				Map.of("dateTime", start.replace("T13", "T14").replace("T15", "T16"), "timeZone", "UTC"));
	}

	private static String allDayStart(String dateTime) {
		return MicrosoftCalendarEventMapper
				.toEvent(Map.of("id", "e", "isAllDay", true, "start", Map.of("dateTime", dateTime, "timeZone", "UTC"),
						"end", Map.of("dateTime", dateTime, "timeZone", "UTC")))
				.toMap(false, 0).get("start").toString();
	}
}
