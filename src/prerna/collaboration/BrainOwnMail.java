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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.javatuples.Pair;

import prerna.auth.User;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMailHelper;

// Mail Collaboration sends the owner (routine results, notices). Each one is recorded by its internetMessageId and
// carries a header, so mail import never reads it back as the owner's own mail.
public final class BrainOwnMail {

	// on the received copy; Graph usually returns no headers on the Sent Items copy, which the recorded id covers
	public static final String HEADER = "X-Collaboration-Sent";

	private BrainOwnMail() {
	}

	/**
	 * Send a message Collaboration wrote to its owner and record it first.
	 *
	 * @param kind what sent it, such as routine or notice
	 * @return the sent message's internetMessageId
	 */
	public static String send(User user, String accessToken, Map<String, Object> message, String kind) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		Map<String, Object> marked = new LinkedHashMap<>(message);
		List<Object> headers = new ArrayList<>();
		if (message.get("internetMessageHeaders") instanceof List<?> existing) {
			headers.addAll(existing);
		}
		headers.add(Map.of("name", HEADER, "value", kind));
		marked.put("internetMessageHeaders", headers);
		return new MicrosoftOutlookMailHelper().sendThroughDraft(accessToken, null, marked,
				internetId -> record(owner.getValue0(), owner.getValue1(), internetId, kind));
	}

	/**
	 * Send an email Collaboration wrote to the signed in owner. It must be addressed to them alone; copies to
	 * others the owner chose may go on cc.
	 */
	public static String sendToOwner(User user, String accessToken, String subject, String body, boolean html,
			String[] to, String[] cc, String[] bcc, String[] attachmentPaths, String kind) throws Exception {
		String me = BrainMailImport.first(BrainMailHeaderSource.current().me(user), "mail", "userPrincipalName");
		if (me == null || to == null || to.length != 1 || !me.equalsIgnoreCase(to[0].trim())
				|| (bcc != null && bcc.length > 0)) {
			throw new IllegalArgumentException("Collaboration only sends this to you: put your own address alone in to");
		}
		return send(user, accessToken, MicrosoftOutlookMailHelper.buildMessage(subject, body, html, to, cc, null,
				null, null, attachmentPaths), kind);
	}

	static void record(String ownerId, String ownerType, String internetMessageId, String kind) {
		CollaborationDbUtils.update(
				"INSERT INTO BRAIN_OWN_MAIL (OWNER_ID, OWNER_TYPE, MESSAGE_KEY, KIND, SENT_AT) VALUES (?, ?, ?, ?, ?)",
				ownerId, ownerType, key(ownerId, ownerType, internetMessageId), kind, CollaborationDbUtils.now());
	}

	// the same key mail import gives the message
	static String key(String ownerId, String ownerType, String internetMessageId) {
		return CollaborationDbUtils.deterministicId(ownerId, ownerType, "email", internetMessageId);
	}

	// keys of every message sent to this owner by Collaboration
	static Set<String> keys(String ownerId, String ownerType) {
		return new HashSet<>(CollaborationDbUtils.query(
				"SELECT MESSAGE_KEY FROM BRAIN_OWN_MAIL WHERE OWNER_ID = ? AND OWNER_TYPE = ?", rs -> rs.getString(1),
				ownerId, ownerType));
	}

	// the one key, when Collaboration sent it; for mail sent after a run loaded the keys
	static Set<String> keys(String ownerId, String ownerType, String messageKey) {
		return new HashSet<>(CollaborationDbUtils.query(
				"SELECT MESSAGE_KEY FROM BRAIN_OWN_MAIL WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND MESSAGE_KEY = ?",
				rs -> rs.getString(1), ownerId, ownerType, messageKey));
	}

	// the header we put on, when Graph returned the headers
	static boolean marked(Map<String, Object> header) {
		if (header.get("internetMessageHeaders") instanceof List<?> list) {
			String name = HEADER.toLowerCase(Locale.ROOT);
			for (Object o : list) {
				if (o instanceof Map<?, ?> m && m.get("name") instanceof String n
						&& name.equals(n.trim().toLowerCase(Locale.ROOT))) {
					return true;
				}
			}
		}
		return false;
	}
}
