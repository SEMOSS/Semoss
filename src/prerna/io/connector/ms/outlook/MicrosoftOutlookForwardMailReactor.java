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
package prerna.io.connector.ms.outlook;

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
 * Passes a message in the signed in user's mailbox on to somebody else.
 *
 * <p>
 * Required delegated Microsoft Graph scopes:
 * </p>
 * <ul>
 * <li>{@code Mail.Send} for {@code POST /me/messages/{id}/forward}</li>
 * <li>{@code Mail.ReadWrite} instead, when {@code asDraft} asks for
 * {@code POST /me/messages/{id}/createForward}</li>
 * </ul>
 *
 * <p>
 * The whole message goes, attachments included, which is the point of
 * forwarding rather than quoting. That is also the reason to think before
 * calling it: whoever it goes to sees everything on the thread, including
 * anything further down it.
 * </p>
 */
public class MicrosoftOutlookForwardMailReactor extends AbstractMicrosoftOutlookMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookForwardMailReactor.class);

	private static final String TO = "to";

	public MicrosoftOutlookForwardMailReactor() {
		this.keysToGet = new String[] { UID, TO, COMMENT, AS_DRAFT };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String uid = requiredUid("forward a message");
		String[] to = values(TO);
		String comment = this.keyValue.get(COMMENT);
		boolean asDraft = Boolean.parseBoolean(this.keyValue.get(AS_DRAFT));

		if (to == null) {
			throw new SemossPixelException("At least one recipient in " + TO + " is required to forward a message.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			Map<String, Object> draft = new MicrosoftOutlookMailHelper().forward(accessToken, null, uid, to, comment,
					asDraft);

			Map<String, Object> output = new LinkedHashMap<>();
			output.put("forwarded", uid);
			output.put(TO, String.join(", ", to));
			output.put("sent", !asDraft);
			if (draft != null) {
				output.put(UID, draft.get("id"));
				MicrosoftOutlookMessageMapper.putIfPresent(output, "webLink", draft.get("webLink"));
			}
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while forwarding the message '{}'", uid, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to forward a message", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to forward the message '{}'", uid, e);
			throw new SemossPixelException(
					"An error occurred forwarding the message. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Forward a message from the signed in user's own Microsoft 365 mailbox, attachments and all.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(TO)) {
			return "Who to forward the message to, passed as several values or as one comma separated value.";
		} else if (key.equals(COMMENT)) {
			return "Optional note added above the message being forwarded.";
		}
		return super.getDescriptionForKey(key);
	}
}
