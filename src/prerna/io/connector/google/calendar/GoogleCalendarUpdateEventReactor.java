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

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GoogleCalendarUpdateEventReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleCalendarUpdateEventReactor.class);

	private static final String SUMMARY = "summary";
	private static final String LOCATION = "location";
	private static final String START_DATE = "startDate";
	private static final String END_DATE = "endDate";
	private static final String VIDEO = "video";
	private static final String EMAIL = "email";
	private static final String FREQUENCY = "frequency";
	private static final String UNTIL = "until";
	private static final String STATUS_KEY = "status";

	public GoogleCalendarUpdateEventReactor() {
		this.keysToGet = new String[] { SUMMARY, LOCATION, ReactorKeysEnum.DESCRIPTION.getKey(), START_DATE, END_DATE,
				VIDEO, EMAIL, ReactorKeysEnum.ID.getKey(), FREQUENCY, UNTIL };
		this.keyRequired = new int[] { 0, 0, 0, 0, 0, 0, 0, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String id = this.keyValue.get(this.keysToGet[7]);
		if (id == null || id.trim().isEmpty()) {
			throw new SemossPixelException("Event ID is required.");
		}
		String summary = null;
		String location = null;
		String desc = null;
		String startdatetime = null;
		String enddatetime = null;
		String enablevideo = null;
		String emailsInput = null;
		String frequency = null;
		String until = null;
		if (this.keyValue.get(this.keysToGet[0]) != null && !this.keyValue.get(this.keysToGet[0]).isEmpty()) {
			summary = this.keyValue.get(this.keysToGet[0]);
		}
		if (this.keyValue.get(this.keysToGet[1]) != null && !this.keyValue.get(this.keysToGet[1]).isEmpty()) {
			location = this.keyValue.get(this.keysToGet[1]);
		}
		if (this.keyValue.get(this.keysToGet[2]) != null && !this.keyValue.get(this.keysToGet[2]).isEmpty()) {
			desc = this.keyValue.get(this.keysToGet[2]);
		}
		if (this.keyValue.get(this.keysToGet[3]) != null && !this.keyValue.get(this.keysToGet[3]).isEmpty()) {
			startdatetime = this.keyValue.get(this.keysToGet[3]);
		}
		if (this.keyValue.get(this.keysToGet[4]) != null && !this.keyValue.get(this.keysToGet[4]).isEmpty()) {
			enddatetime = this.keyValue.get(this.keysToGet[4]);
		}
		if (this.keyValue.get(this.keysToGet[5]) != null && !this.keyValue.get(this.keysToGet[5]).isEmpty()) {
			enablevideo = this.keyValue.get(this.keysToGet[5]);
		}
		if (this.keyValue.get(this.keysToGet[6]) != null && !this.keyValue.get(this.keysToGet[6]).isEmpty()) {
			emailsInput = this.keyValue.get(this.keysToGet[6]);
		}
		if (this.keyValue.get(this.keysToGet[8]) != null && !this.keyValue.get(this.keysToGet[8]).isEmpty()) {
			frequency = this.keyValue.get(this.keysToGet[8]).trim().toUpperCase();
		}
		if (this.keyValue.get(this.keysToGet[9]) != null && !this.keyValue.get(this.keysToGet[9]).isEmpty()) {
			until = this.keyValue.get(this.keysToGet[9]).trim();
		}
		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			List<String> attendeeEmails = null;
			if (emailsInput != null && !emailsInput.isEmpty()) {
				attendeeEmails = new ArrayList<>();
				String[] emailArray = emailsInput.split(",");
				for (String email : emailArray) {
					email = email.trim();
					if (!email.isEmpty()) {
						attendeeEmails.add(email);
					}
				}
			}

			ZoneId zoneId = user.getZoneId();
			if (zoneId == null) {
				zoneId = Utility.getApplicationZoneIdObj();
			}

			Boolean video = null;
			if (enablevideo != null && !enablevideo.trim().isEmpty()) {
				String normalizedVideo = enablevideo.trim().toLowerCase();
				if ("true".equals(normalizedVideo) || "false".equals(normalizedVideo)) {
					video = Boolean.parseBoolean(normalizedVideo);
				} else {
					throw new SemossPixelException("Video must be set to true or false.");
				}
			}
			boolean result = GoogleCalendarHelper.updateEvent(accessToken, id, summary, location, desc, startdatetime,
					enddatetime, zoneId, attendeeEmails, frequency, until, video);
			Map<String, Object> map = new HashMap<>();
			map.put(STATUS_KEY, result);
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (SemossPixelException e) {
			classLogger.error("Error while updating a Google Calendar event", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to update a Google Calendar event", e);
			throw new SemossPixelException("An error occurred updating the event. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Update an existing event in Google Calendar.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SUMMARY)) {
			return "Updated title or summary for the event.";
		} else if (key.equals(LOCATION)) {
			return "Updated location for the event.";
		} else if (key.equals(ReactorKeysEnum.DESCRIPTION.getKey())) {
			return "Updated detailed description for the event.";
		} else if (key.equals(START_DATE)) {
			return "Updated start date and time for the event.";
		} else if (key.equals(END_DATE)) {
			return "Updated end date and time for the event.";
		} else if (key.equals(VIDEO)) {
			return "Whether to include Google Meet video conferencing.";
		} else if (key.equals(EMAIL)) {
			return "Updated comma-separated attendee email addresses.";
		} else if (key.equals(ReactorKeysEnum.ID.getKey())) {
			return "Unique identifier of the event to update.";
		} else if (key.equals(FREQUENCY)) {
			return "Updated recurrence frequency (DAILY, WEEKLY, or NONE).";
		} else if (key.equals(UNTIL)) {
			return "Optional updated end date and time for recurring events; leave blank for no end date.";
		}
		return super.getDescriptionForKey(key);
	}

}
