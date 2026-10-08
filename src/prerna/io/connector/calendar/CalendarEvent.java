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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import prerna.io.connector.ConnectorOutput;

/**
 * One event, the same whichever calendar it was read from.
 *
 * <p>
 * Times are written in UTC, and a whole day event as dates, with an end that is
 * the day after the last day, the way both providers keep them. The event's own
 * zone is reported alongside, so its times can be shown the way its organizer
 * set them.
 * </p>
 *
 * @param id                         the provider's id for the event
 * @param subject                    its title
 * @param start                      when it starts
 * @param end                        when it ends
 * @param timeZone                   the zone the calendar keeps the event in
 * @param isAllDay                   whether it covers whole days
 * @param location                   where it is held
 * @param organizer                  the organizer's address
 * @param organizerName              the organizer's display name
 * @param attendees                  who is invited, and how they answered
 * @param webLink                    where it opens in the provider's own app
 * @param joinUrl                    the link to join its online meeting
 * @param isOnlineMeeting            whether it has an online meeting
 * @param isCancelled                whether it was canceled
 * @param showAs                     how its time reads on the calendar
 * @param importance                 how urgent it is, which only Outlook keeps
 * @param responseStatus             how the signed in user answered
 * @param reminderMinutesBeforeStart how long before the start the reminder
 *                                   fires
 * @param categories                 what it is tagged with
 * @param isRecurring                whether it is one sitting of a series
 * @param body                       the readable text of its description
 */
public record CalendarEvent(String id, String subject, EventTime start, EventTime end, String timeZone,
		boolean isAllDay, String location, String organizer, String organizerName, List<EventAttendee> attendees,
		String webLink, String joinUrl, boolean isOnlineMeeting, boolean isCancelled, String showAs, String importance,
		String responseStatus, Integer reminderMinutesBeforeStart, List<String> categories, boolean isRecurring,
		String body) {

	public CalendarEvent {
		attendees = attendees == null ? List.of() : List.copyOf(attendees);
		categories = categories == null ? List.of() : List.copyOf(categories);
	}

	/**
	 * @param includeBody  whether the body comes back
	 * @param maxBodyChars the longest body to return before cutting it short, or 0
	 *                     to return whatever length it is
	 * @return the event as a reactor answers with it
	 */
	public Map<String, Object> toMap(boolean includeBody, int maxBodyChars) {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("id", this.id);
		ConnectorOutput.putIfPresent(output, "subject", this.subject);
		ConnectorOutput.putIfPresent(output, "start", this.start == null ? null : this.start.format());
		ConnectorOutput.putIfPresent(output, "end", this.end == null ? null : this.end.format());
		ConnectorOutput.putIfPresent(output, "timeZone", this.timeZone);
		output.put("isAllDay", this.isAllDay);
		ConnectorOutput.putIfPresent(output, "location", this.location);
		ConnectorOutput.putIfPresent(output, "organizer", this.organizer);
		ConnectorOutput.putIfPresent(output, "organizerName", this.organizerName);
		output.put("attendees", this.attendees.stream().map(EventAttendee::toMap).toList());
		ConnectorOutput.putIfPresent(output, "webLink", this.webLink);
		ConnectorOutput.putIfPresent(output, "joinUrl", this.joinUrl);
		output.put("isOnlineMeeting", this.isOnlineMeeting);
		output.put("isCancelled", this.isCancelled);
		ConnectorOutput.putIfPresent(output, "showAs", this.showAs);
		ConnectorOutput.putIfPresent(output, "importance", this.importance);
		ConnectorOutput.putIfPresent(output, "responseStatus", this.responseStatus);
		ConnectorOutput.putIfPresent(output, "reminderMinutesBeforeStart", this.reminderMinutesBeforeStart);
		output.put("categories", this.categories);
		output.put("isRecurring", this.isRecurring);
		if (includeBody) {
			ConnectorOutput.putText(output, "body", this.body, maxBodyChars);
		}
		return output;
	}
}
