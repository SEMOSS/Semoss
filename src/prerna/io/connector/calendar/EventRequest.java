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

/**
 * An event a caller wrote, to create or to change.
 *
 * <p>
 * Anything the caller left out is null, which is what lets a change touch one
 * thing without quietly clearing the rest: a provider writes only what is set.
 * </p>
 *
 * @param id                the event to change, or null to create one
 * @param calendarId        the calendar, or null for the default one
 * @param mailbox           whose calendar, or null for the user's own
 * @param subject           its title
 * @param body              its description
 * @param html              whether the description is html rather than plain
 *                          text
 * @param start             when it starts
 * @param end               when it ends
 * @param zone              the zone the event is kept in, which is UTC when the
 *                          caller named none
 * @param isAllDay          whether it covers whole days
 * @param location          where it is held
 * @param attendees         who is required, which replaces the whole guest list
 *                          when set
 * @param optionalAttendees who is optional, which replaces the whole guest list
 *                          when set
 * @param isOnlineMeeting   whether it gets an online meeting link
 * @param reminderMinutes   how long before the start the reminder fires
 * @param showAs            how its time reads on the calendar
 * @param importance        how urgent it is, which only Outlook keeps
 * @param categories        what it is tagged with
 * @param recurrence        how it repeats
 */
public record EventRequest(String id, String calendarId, String mailbox, String subject, String body, boolean html,
		EventTime start, EventTime end, ZoneId zone, Boolean isAllDay, String location, List<String> attendees,
		List<String> optionalAttendees, Boolean isOnlineMeeting, Integer reminderMinutes, String showAs,
		String importance, List<String> categories, Recurrence recurrence) {

	/**
	 * @return whether the guest list is being set
	 */
	public boolean setsAttendees() {
		return this.attendees != null || this.optionalAttendees != null;
	}

	/**
	 * @return whether nothing about the event is being set
	 */
	public boolean isEmpty() {
		return this.subject == null && this.body == null && this.start == null && this.end == null
				&& this.isAllDay == null && this.location == null && !setsAttendees() && this.isOnlineMeeting == null
				&& this.reminderMinutes == null && this.showAs == null && this.importance == null
				&& this.categories == null && this.recurrence == null;
	}
}
