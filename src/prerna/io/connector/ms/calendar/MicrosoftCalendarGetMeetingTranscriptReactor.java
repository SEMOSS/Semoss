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

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import prerna.io.connector.ConnectorOutput;
import prerna.io.connector.ConnectorTimes;
import prerna.io.connector.calendar.AbstractCalendarReactor;
import prerna.io.connector.calendar.CalendarApp;
import prerna.io.connector.calendar.CalendarEvent;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.calendar.MicrosoftCalendarHelper.EventTranscript;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reads what was said in the Teams meeting of an event on a Microsoft 365
 * calendar.
 *
 * <p>
 * Required delegated Microsoft Graph scopes, beside {@code Calendars.Read} for
 * the event: {@code OnlineMeetings.Read} to find its meeting, and
 * {@code OnlineMeetingTranscript.Read.All}, which an administrator consents
 * to, to read the transcript.
 * </p>
 *
 * <p>
 * Google Calendar has nothing to read a transcript from, so this stands on its
 * own instead of under an operation both calendars share.
 * </p>
 */
public class MicrosoftCalendarGetMeetingTranscriptReactor extends AbstractCalendarReactor {

	/**
	 * How much of a transcript one call returns, about three quarters of an hour
	 * of talk, so that a long meeting is read a page at a time rather than filling
	 * a model's context.
	 */
	static final int PAGE_CHARS = 40_000;

	/** How many of a series' transcripts a sitting without one names. */
	private static final int ELSEWHERE_NAMED = 5;

	public MicrosoftCalendarGetMeetingTranscriptReactor() {
		this.keysToGet = new String[] { ID, OFFSET, CALENDAR_ID, MAILBOX };
		this.keyRequired = requiredKeys(this.keysToGet, ID);
	}

	@Override
	protected CalendarApp getCalendarApp() {
		return CalendarApp.MICROSOFT_CALENDAR;
	}

	@Override
	protected NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("read the meeting transcript", () -> {
			String id = requireEventId("read a meeting transcript");
			int offset = readOffset();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(this.insight.getUser());
			CalendarEvent event = MicrosoftCalendarHelper.getEvent(accessToken, readString(MAILBOX),
					readString(CALENDAR_ID), id);
			EventTranscript transcript = MicrosoftCalendarHelper.getEventTranscript(accessToken, event);

			Map<String, Object> output = new LinkedHashMap<>();
			ConnectorOutput.putIfPresent(output, "subject", event.subject());
			List<String> lines = transcript.lines();
			if (lines.isEmpty()) {
				output.put("message", nothingToRead(transcript.elsewhere()));
				return output;
			}
			// whole lines up to the size of a page, and at least one, so that a page
			// always moves on
			int start = Math.min(offset, lines.size());
			int end = start;
			int chars = 0;
			while (end < lines.size() && (end == start || chars + lines.get(end).length() <= PAGE_CHARS)) {
				chars += lines.get(end).length() + 1;
				end++;
			}
			output.put("transcript", String.join("\n", lines.subList(start, end)));
			output.put("hasMore", end < lines.size());
			if (end < lines.size()) {
				output.put("nextOffset", end);
			}
			return output;
		});
	}

	/**
	 * @param elsewhere when the meeting's other transcripts started, oldest first
	 * @return why there is no transcript to return
	 */
	private static String nothingToRead(List<Instant> elsewhere) {
		if (elsewhere.isEmpty()) {
			return "This meeting has no transcript: it was not transcribed, or it has not been held yet.";
		}
		return "No transcript of this meeting started around this event's time. The series was transcribed on other "
				+ "days, the latest starting at "
				+ elsewhere.subList(Math.max(0, elsewhere.size() - ELSEWHERE_NAMED), elsewhere.size()).stream()
						.map(ConnectorTimes::format).collect(Collectors.joining(", "))
				+ " (UTC). Pass the event of one of those days to read it.";
	}

	@Override
	protected MCPExecution getExecution() {
		return MCPExecution.AUTO;
	}

	@Override
	protected String getView() {
		return null;
	}

	@Override
	protected String describe() {
		return "Read what was said in the Teams meeting of an event on a " + calendar()
				+ ": its transcript, a line for each turn that starts with the speaker's name. Only a meeting that "
				+ "was transcribed has one, and only its organizer and the people invited can read it. For a recurring "
				+ "meeting, pass the event of the day wanted and that day's transcript is returned. A long transcript "
				+ "comes back a page at a time: while hasMore is true, call again with offset set to nextOffset.";
	}

	@Override
	protected String describeKey(String key) {
		if (OFFSET.equals(key)) {
			return "Optional number of transcript lines to skip, to read on after a page: the nextOffset of the "
					+ "result before. Defaults to 0.";
		}
		return super.describeKey(key);
	}
}
