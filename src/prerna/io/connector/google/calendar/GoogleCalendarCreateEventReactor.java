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
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.io.connector.google.GoogleLoginUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GoogleCalendarCreateEventReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GoogleCalendarCreateEventReactor.class);

	private static final String SUMMARY = "summary";
	private static final String LOCATION = "location";
	private static final String START_DATE = "startDate";
	private static final String END_DATE = "endDate";
	private static final String EMAIL = "email";
	private static final String FREQUENCY = "frequency";
	private static final String UNTIL = "until";
	private static final String VIDEO = "video";
	private static final String NONE = "NONE";
	private static final String NO_TITLE = "No title";

	public GoogleCalendarCreateEventReactor() {
		this.keysToGet = new String[] { SUMMARY, LOCATION, ReactorKeysEnum.DESCRIPTION.getKey(), START_DATE, END_DATE,
				EMAIL, FREQUENCY, UNTIL, VIDEO };
		this.keyRequired = new int[] { 0, 0, 0, 1, 1, 0, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String summary = "";
		String location = "";
		String desc = "";
		String startdatetime = this.keyValue.get(this.keysToGet[3]);
		String enddatetime = this.keyValue.get(this.keysToGet[4]);
		String emailsInput = "";
		String frequency = "";
		String until = "";
		String enablevideo = "";
		if (startdatetime == null || startdatetime.trim().isEmpty()) {
			throw new SemossPixelException("Start date and time are required.");
		}
		if (enddatetime == null || enddatetime.trim().isEmpty()) {
			throw new SemossPixelException("End date and time are required.");
		}

		if (this.keyValue.get(this.keysToGet[0]) != null && !this.keyValue.get(this.keysToGet[0]).isEmpty()) {
			summary = this.keyValue.get(this.keysToGet[0]);
		} else {
			summary = NO_TITLE;
		}
		if (this.keyValue.get(this.keysToGet[1]) != null && !this.keyValue.get(this.keysToGet[1]).isEmpty()) {
			location = this.keyValue.get(this.keysToGet[1]);
		}
		if (this.keyValue.get(this.keysToGet[2]) != null && !this.keyValue.get(this.keysToGet[2]).isEmpty()) {
			desc = this.keyValue.get(this.keysToGet[2]);
		}
		if (this.keyValue.get(this.keysToGet[5]) != null && !this.keyValue.get(this.keysToGet[5]).isEmpty()) {
			emailsInput = this.keyValue.get(this.keysToGet[5]);
		}
		if (this.keyValue.get(this.keysToGet[6]) != null && !this.keyValue.get(this.keysToGet[6]).isEmpty()) {
			frequency = this.keyValue.get(this.keysToGet[6]).trim().toUpperCase();
		}
		if (this.keyValue.get(this.keysToGet[7]) != null && !this.keyValue.get(this.keysToGet[7]).isEmpty()) {
			until = this.keyValue.get(this.keysToGet[7]).trim();
		}
		if (this.keyValue.get(this.keysToGet[8]) != null && !this.keyValue.get(this.keysToGet[8]).isEmpty()) {
			enablevideo = this.keyValue.get(this.keysToGet[8]);
		}

		try {
			User user = this.insight.getUser();
			String accessToken = GoogleLoginUtils.getGoogleAccessToken(user);
			List<String> attendeeEmails = new ArrayList<>();
			if (emailsInput != null && !emailsInput.isEmpty()) {
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
			boolean video = false;
			if (enablevideo != null && !enablevideo.trim().isEmpty()) {
				String normalizedVideo = enablevideo.trim().toLowerCase();
				if ("true".equals(normalizedVideo) || "false".equals(normalizedVideo)) {
					video = Boolean.parseBoolean(normalizedVideo);
				} else {
					throw new SemossPixelException("Video must be set to true or false.");
				}
			}
			boolean isRecurring = frequency != null && !frequency.isEmpty() && !frequency.equals(NONE);
			if (!isRecurring) {
				Map<String, Object> result = GoogleCalendarHelper.createEvent(accessToken, summary, location, desc,
						startdatetime, enddatetime, zoneId, attendeeEmails, video);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
			} else {
				Map<String, Object> result = GoogleCalendarHelper.recurringEvent(accessToken, summary, location, desc,
						startdatetime, enddatetime, zoneId, attendeeEmails, frequency, until, video);
				return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE);
			}
		} catch (SemossPixelException e) {
			classLogger.error("Error while creating a Google Calendar event", e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to create a Google Calendar event", e);
			throw new SemossPixelException("An error occurred creating the event. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Create a Google Calendar event (single or recurring).";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SUMMARY)) {
			return "Title or summary for the event.";
		} else if (key.equals(LOCATION)) {
			return "Location where the event takes place.";
		} else if (key.equals(ReactorKeysEnum.DESCRIPTION.getKey())) {
			return "Detailed description of the event (" + ReactorKeysEnum.DESCRIPTION.getKey() + ").";
		} else if (key.equals(START_DATE)) {
			return "Start date and time of the event.";
		} else if (key.equals(END_DATE)) {
			return "End date and time of the event.";
		} else if (key.equals(EMAIL)) {
			return "Comma-separated attendee email addresses.";
		} else if (key.equals(FREQUENCY)) {
			return "Recurrence frequency for recurring events (DAILY, WEEKLY, or NONE).";
		} else if (key.equals(UNTIL)) {
			return "Optional end date and time for a recurring event; leave blank for no end date.";
		} else if (key.equals(VIDEO)) {
			return "Whether to include Google Meet video conferencing.";
		}
		return super.getDescriptionForKey(key);
	}
}
