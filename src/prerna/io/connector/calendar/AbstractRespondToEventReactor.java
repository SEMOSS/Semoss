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
import java.util.Locale;
import java.util.Map;

import prerna.auth.User;
import prerna.reactor.agent.mcp.MCPUtility.MCPExecution;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Accepts, declines or tentatively accepts an invitation.
 */
public abstract class AbstractRespondToEventReactor extends AbstractCalendarReactor {

	public static final String ACCEPT = "accept";
	public static final String DECLINE = "decline";
	public static final String TENTATIVE = "tentative";

	/** Every answer an invitation can be given. */
	public static final List<String> RESPONSES = List.of(ACCEPT, DECLINE, TENTATIVE);

	private static final String[] KEYS = { ID, RESPONSE, COMMENT, SEND_RESPONSE, CALENDAR_ID, MAILBOX };

	/**
	 * An answer to an invitation.
	 *
	 * @param id           the event
	 * @param calendarId   the calendar holding it, or null for the default one
	 * @param mailbox      whose invitation, or null for the user's own
	 * @param response     one of {@link #RESPONSES}
	 * @param comment      a note to the organizer, or null
	 * @param sendResponse whether the organizer is told
	 */
	public record RespondRequest(String id, String calendarId, String mailbox, String response, String comment,
			boolean sendResponse) {
	}

	protected AbstractRespondToEventReactor(String... extraKeys) {
		this.keysToGet = withExtraKeys(KEYS, extraKeys);
		this.keyRequired = requiredKeys(this.keysToGet, ID, RESPONSE);
	}

	/**
	 * Record the answer.
	 *
	 * @param user    the signed in user
	 * @param request the answer
	 * @throws Exception when the answer cannot be recorded
	 */
	protected abstract void respondToEvent(User user, RespondRequest request) throws Exception;

	@Override
	protected final NounMetadata executeAuthenticated() {
		this.organizeKeys();
		return run("reply to the invitation", () -> {
			String id = requireEventId("reply to an invitation");
			String response = normalizeResponse(
					requireString(RESPONSE, "A " + RESPONSE + " of accept, decline or tentative is required."));
			// telling the organizer is the default, since a reply nobody sees is not
			// what accepting or declining usually means
			boolean sendResponse = readBoolean(SEND_RESPONSE, true);
			respondToEvent(this.insight.getUser(), new RespondRequest(id, readString(CALENDAR_ID), readString(MAILBOX),
					response, readString(COMMENT), sendResponse));

			Map<String, Object> output = new LinkedHashMap<>();
			output.put(ID, id);
			output.put(RESPONSE, response);
			output.put(SEND_RESPONSE, sendResponse);
			return output;
		});
	}

	private static String normalizeResponse(String response) {
		String normalized = response.trim().toLowerCase(Locale.ROOT);
		if ("tentativelyaccept".equals(normalized) || "maybe".equals(normalized)) {
			return TENTATIVE;
		}
		if (!RESPONSES.contains(normalized)) {
			throw new IllegalArgumentException(
					RESPONSE + " must be one of " + RESPONSES + " but received: " + response);
		}
		return normalized;
	}

	@Override
	protected final MCPExecution getExecution() {
		return MCPExecution.ASK;
	}

	@Override
	protected final String getView() {
		return "calendar/event?intent=respond";
	}

	@Override
	protected final String describe() {
		return "Accept, decline or tentatively accept a meeting invitation on a " + calendar()
				+ ", as the signed in user or on behalf of somebody they are a delegate of.";
	}

	@Override
	protected final String describeKey(String key) {
		switch (key) {
		case ID:
			return "Id of the event to reply to, as returned by " + reactorName("ListEvents") + ".";
		case RESPONSE:
			return "The reply, one of " + RESPONSES + ".";
		case COMMENT:
			return "Optional note sent to the organizer along with the reply.";
		case SEND_RESPONSE:
			return "Optional boolean for whether the organizer is told of the reply. Defaults to true.";
		default:
			return super.describeKey(key);
		}
	}
}
