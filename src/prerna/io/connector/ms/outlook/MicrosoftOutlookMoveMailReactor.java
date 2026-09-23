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
 * Moves a message into another folder of the signed in user's mailbox.
 *
 * <p>
 * Required delegated Microsoft Graph scopes:
 * </p>
 * <ul>
 * <li>{@code Mail.ReadWrite} for {@code POST /me/messages/{id}/move}</li>
 * <li>{@code Mail.Read} for {@code GET /me/mailFolders}, which is read to turn
 * a folder somebody named into the id Graph moves to</li>
 * </ul>
 *
 * <p>
 * The folder can be a well known name such as {@code archive}, the name of a
 * folder in the mailbox, or a folder id. A folder nested inside another is not
 * found by name, since only the top level is searched, so name those by id.
 * </p>
 *
 * <p>
 * A move is a copy and a delete on the server, so the message comes out of it
 * with a different id. The new one is what comes back here, and the old one
 * stops working the moment the move lands.
 * </p>
 */
public class MicrosoftOutlookMoveMailReactor extends AbstractMicrosoftOutlookMessageReactor {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookMoveMailReactor.class);

	private static final String FOLDER = "folder";

	public MicrosoftOutlookMoveMailReactor() {
		this.keysToGet = new String[] { UID, FOLDER };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String uid = requiredUid("move a message");
		String folder = trimToNull(this.keyValue.get(FOLDER));

		if (folder == null) {
			throw new SemossPixelException("A " + FOLDER + " to move the message into is required.");
		}

		try {
			User user = this.insight.getUser();
			String accessToken = MicrosoftLoginUtils.getValidAccessToken(user);
			MicrosoftOutlookMailHelper helper = new MicrosoftOutlookMailHelper();

			String destination = helper.resolveDestination(accessToken, null, folder);
			String movedUid = helper.moveMessage(accessToken, null, uid, destination);

			Map<String, Object> output = new LinkedHashMap<>();
			// the id it had, and the one it has now, since a caller holding the old one
			// has nothing to read with it
			output.put("previousUid", uid);
			MicrosoftOutlookMessageMapper.putIfPresent(output, UID, movedUid);
			output.put(FOLDER, folder);
			output.put("moved", true);
			return new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE);
		} catch (SemossPixelException e) {
			classLogger.error("Error while moving the message '{}' to '{}'", uid, folder, e);
			throw e;
		} catch (IllegalArgumentException e) {
			classLogger.error("Invalid input passed to move a message", e);
			throw new SemossPixelException(e.getMessage());
		} catch (Exception e) {
			classLogger.error("Failed to move the message '{}' to '{}'", uid, folder, e);
			throw new SemossPixelException("An error occurred moving the message. Error message: " + e.getMessage());
		}
	}

	@Override
	public String getReactorDescription() {
		return "Move a message into another folder of the signed in user's own Microsoft 365 mailbox.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(FOLDER)) {
			return "Where to move the message: a well known name such as archive, deleteditems or inbox, the name of a folder in the mailbox as returned by MicrosoftOutlookListMailFolders, or a folder id.";
		}
		return super.getDescriptionForKey(key);
	}
}
