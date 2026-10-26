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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

import prerna.auth.User;
import prerna.collaboration.BrainProfileUtils;
import prerna.io.connector.mail.ComposedMail;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.util.ValueUtils;

/**
 * Moves a finished draft into the folder the user picked in their Collaboration
 * settings, so drafts the agent wrote can be worked through in one place in the
 * Outlook app instead of being mixed into Drafts.
 */
public final class MicrosoftOutlookDraftFolder {

	private static final Logger classLogger = LogManager.getLogger(MicrosoftOutlookDraftFolder.class);

	private MicrosoftOutlookDraftFolder() {
	}

	/**
	 * Leave the draft in the user's chosen folder.
	 *
	 * <p>
	 * A move gives the message a new id, so the draft that comes back carries it.
	 * With no folder chosen, or when the move fails, the draft stays in Drafts and
	 * is returned as it was; a draft that was written is never lost to a folder
	 * problem.
	 * </p>
	 */
	public static ComposedMail place(User user, ComposedMail draft) {
		if (draft == null || draft.id() == null) {
			return draft;
		}
		String folderId = BrainProfileUtils.draftFolderId(user);
		if (folderId == null) {
			return draft;
		}
		try {
			String token = MicrosoftLoginUtils.getValidAccessToken(user);
			MicrosoftOutlookMailHelper mail = new MicrosoftOutlookMailHelper();
			Map<String, Object> moved = mail.moveMessageDetails(token, null, draft.id(), folderId);
			String id = moved == null ? null : ValueUtils.toStringOrNull(moved.get("id"));
			if (id == null) {
				return draft;
			}
			// unread, so Outlook counts it on the folder and the owner can see a new one landed
			try {
				mail.setRead(token, null, id, false);
			} catch (Exception e) {
				classLogger.warn("Could not mark the moved draft unread: {}", e.getMessage());
			}
			String webLink = ValueUtils.toStringOrNull(moved.get("webLink"));
			return draft.withIds(id, draft.conversationId(), webLink == null ? draft.webLink() : webLink);
		} catch (Exception e) {
			// most often the folder was deleted in Outlook
			classLogger.warn("Left a draft in Drafts; the chosen folder could not be used: {}", e.getMessage());
			return draft;
		}
	}
}
