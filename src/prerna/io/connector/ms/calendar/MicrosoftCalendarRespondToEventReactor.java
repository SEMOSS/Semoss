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

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Replies to a meeting invitation on behalf of whoever is signed in.
 *
 * <p>
 * Required delegated Microsoft Graph scope:
 * </p>
 * <ul>
 * <li>{@code Calendars.ReadWrite} for {@code POST /me/events/{id}/accept},
 * {@code /decline} and {@code /tentativelyAccept}</li>
 * </ul>
 *
 * <p>
 * The reply is the signed in user's own, since the token says who that is. By
 * default the organizer is told, which is what accepting or declining an
 * invitation ordinarily means; passing {@code sendResponse} as false records
 * the reply on the user's own calendar and leaves the organizer unaware.
 * </p>
 */
public class MicrosoftCalendarRespondToEventReactor extends AbstractMicrosoftCalendarReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftCalendarRespondToEventReactor.class);

	private static final String RESPONSE = "response";
	private static final String COMMENT = "comment";
	private static final String SEND_RESPONSE = "sendResponse";

	public MicrosoftCalendarRespondToEventReactor() {
		this.keysToGet = new String[] { EVENT_ID, RESPONSE, COMMENT, SEND_RESPONSE };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();

		String eventId = trimToNull(this.keyValue.get(EVENT_ID));
		if (eventId == null) {
			throw new SemossPixelException("An " + EVENT_ID + " is required to reply to a calendar invitation.");
		}
		String response = trimToNull(this.keyValue.get(RESPONSE));
		if (response == null) {
			throw new SemossPixelException("A " + RESPONSE + " of accept, decline or tentative is required.");
		}
		String comment = trimToNull(this.keyValue.get(COMMENT));
		// telling the organizer is the default, since a reply nobody sees is not
		// what accepting or declining usually means
		Boolean sendResponse = optionalBoolean(SEND_RESPONSE);
		boolean tellOrganizer = sendResponse == null || sendResponse;

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getMicrosoftAccessToken(user);
			String replied = MicrosoftCalendarHelper.respondToEvent(accessToken, eventId, response, comment,
					tellOrganizer);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("id", eventId);
			output.put(RESPONSE, replied);
			output.put(SEND_RESPONSE, tellOrganizer);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while replying to calendar event '{}'", eventId, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid reply '{}' passed for calendar event '{}'", response, eventId, e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to reply to calendar event '{}'", eventId, e);
			throw new SemossPixelException(
					"An error occurred replying to the calendar invitation. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Accept, decline or tentatively accept a Microsoft 365 meeting invitation as the signed in user.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(EVENT_ID)) {
			return "Id of the event to reply to, as returned by MicrosoftCalendarListEvents.";
		} else if (key.equals(RESPONSE)) {
			return "The reply, one of accept, decline or tentative.";
		} else if (key.equals(COMMENT)) {
			return "Optional note sent to the organizer along with the reply.";
		} else if (key.equals(SEND_RESPONSE)) {
			return "Optional boolean for whether the organizer is told of the reply. Defaults to true.";
		}
		return super.getDescriptionForKey(key);
	}
}
