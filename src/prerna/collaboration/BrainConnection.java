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
package prerna.collaboration;

import java.util.LinkedHashMap;
import java.util.Map;

import org.javatuples.Pair;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.SecurityExternalConnectorsUtils;
import prerna.io.connector.ms.MicrosoftGraphCredentialStore;

/** The owner's Microsoft connection as Settings shows it: sign-in, linked mailbox, background access. */
public final class BrainConnection {

	private BrainConnection() {
	}

	/**
	 * @return signedIn/account for this session, mailbox linked by the import, background access
	 *         (saved, renewable, whether this sign-in can save it) and per-source status. Never tokens.
	 */
	public static Map<String, Object> status(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		Map<String, String> mailbox = mailbox(owner);
		String mailboxId = mailbox.get("id");
		AccessToken session = user.getAccessToken(AuthProvider.MICROSOFT);

		Map<String, Object> microsoft = new LinkedHashMap<>();
		microsoft.put("signedIn", session != null);
		microsoft.put("account", session == null ? null : first(session.getEmail(), session.getName()));
		// a refresh token in this session is what background access is saved from
		microsoft.put("renewable", MicrosoftGraphCredentialStore.isRefreshable(session));

		AccessToken saved = MicrosoftGraphCredentialStore.get(mailboxId);
		Map<String, Object> background = new LinkedHashMap<>();
		background.put("saved", saved != null);
		background.put("account", saved == null ? null : saved.getEmail());
		background.put("renewable", MicrosoftGraphCredentialStore.isRefreshable(saved));
		background.put("canSave", canSave(session, mailboxId));
		// signed in to a different Microsoft account than the one the Brain reads
		background.put("otherAccount", session != null && mailboxId != null && !mailboxId.equals(session.getId()));

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("microsoft", microsoft);
		result.put("mailbox", mailbox.get("upn"));
		result.put("background", background);
		result.put("sources", CollaborationSourceUtils.getSourceStatuses(owner.getValue0(), owner.getValue1()));
		// a finished import, so turning mail off reads as paused rather than never set up
		result.put("imported", BrainSync.lastRun(owner.getValue0(), owner.getValue1()) != null);
		// until the owner opens Work at the end of setup or skips it, Today sends them back to setup
		result.put("setupDone", CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_SETTINGS WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND SETUP_DONE_AT IS NOT NULL", owner.getValue0(), owner.getValue1()));
		// which Outlook folders mail is read from, and how many topics stand for one
		result.put("folders", BrainMailFolders.summary(owner.getValue0(), owner.getValue1()));
		return result;
	}

	/** Records that the owner opened Work at the end of setup or skipped the rest of it; Reset clears it. */
	public static Map<String, Object> finishSetup(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		// creates the settings row a new owner does not have yet
		BrainProfileUtils.getSettings(owner.getValue0(), owner.getValue1());
		CollaborationDbUtils.update("UPDATE BRAIN_SETTINGS SET SETUP_DONE_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				CollaborationDbUtils.now(), owner.getValue0(), owner.getValue1());
		return status(user);
	}

	/** Keeps this session's renewable Microsoft sign-in so routines and refresh work without the page open. */
	public static Map<String, Object> saveBackground(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		AccessToken session = user.getAccessToken(AuthProvider.MICROSOFT);
		String mailboxId = mailbox(owner).get("id");
		if (session != null && mailboxId != null && !mailboxId.equals(session.getId())) {
			throw new IllegalArgumentException("Sign in with the Microsoft account your mail is read from.");
		}
		if (!canSave(session, mailboxId)) {
			throw new IllegalArgumentException("Reconnect Microsoft to enable background inbox access.");
		}
		MicrosoftGraphCredentialStore.save(session);
		return status(user);
	}

	/** Removes the saved background access; scheduled routines stop until it is saved again. */
	public static Map<String, Object> forgetBackground(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String mailboxId = mailbox(owner).get("id");
		SecurityExternalConnectorsUtils.deleteMicrosoftGraphCredential(mailboxId);
		// the new-mail subscription keeps a sign-in of its own, and a routine would restore background access from it
		BrainMailWebhook.unsubscribe(mailboxId);
		return status(user);
	}

	private static boolean canSave(AccessToken session, String mailboxId) {
		return MicrosoftGraphCredentialStore.isRefreshable(session)
				&& (mailboxId == null || mailboxId.equals(session.getId()));
	}

	private static Map<String, String> mailbox(Pair<String, String> owner) {
		Map<String, String> row = CollaborationDbUtils.queryOne(
				"SELECT MS_USER_ID, MS_UPN FROM COLLAB_OWNER WHERE OWNER_ID = ? AND OWNER_TYPE = ?", rs -> {
					Map<String, String> value = new LinkedHashMap<>();
					value.put("id", blankToNull(rs.getString(1)));
					value.put("upn", blankToNull(rs.getString(2)));
					return value;
				}, owner.getValue0(), owner.getValue1());
		return row == null ? new LinkedHashMap<>() : row;
	}

	private static String blankToNull(String value) {
		return value == null || value.isBlank() ? null : value;
	}

	private static String first(String a, String b) {
		return a != null && !a.isBlank() ? a : b;
	}
}
