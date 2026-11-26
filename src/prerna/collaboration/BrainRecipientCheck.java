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
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.io.connector.ms.MicrosoftLoginUtils;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMailHelper;
import prerna.io.connector.ms.outlook.MicrosoftOutlookMessageMapper;

/**
 * Tells the owner whether an address an email is going to is one they already write to. An
 * address is known when it is the owner's own, a person the owner confirmed or follows in Brain, or
 * someone on a thread the owner has written in. Mail that only arrived from an address does not
 * make it known, so an address an injected email names stays new.
 */
public final class BrainRecipientCheck {

	private static final Logger classLogger = LogManager.getLogger(BrainRecipientCheck.class);

	public static final String KNOWN = "known";
	public static final String NEW = "new";

	// tools that send mail; the arguments that name who gets it
	private static final String SEND_EMAIL = "SendEmail";

	private BrainRecipientCheck() {
	}

	/**
	 * Each address with its status (known or new) and, when known, why: owner, confirmed or
	 * correspondence. Blank and repeated addresses are dropped.
	 */
	public static List<Map<String, Object>> classify(User user, Collection<String> addresses) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownAddress = CollaborationToolResults.ownEmail(user);
		Set<String> unique = new LinkedHashSet<>();
		for (String address : addresses) {
			String normalized = normalize(address);
			if (!normalized.isEmpty()) {
				unique.add(normalized);
			}
		}
		List<Map<String, Object>> out = new ArrayList<>();
		for (String address : unique) {
			Map<String, Object> entry = new LinkedHashMap<>();
			entry.put("address", address);
			Map<String, Object> person = person(owner, address);
			String reason = reason(owner, address, ownAddress, person);
			if (person != null && person.get("name") != null) {
				entry.put("name", person.get("name"));
			}
			entry.put("status", reason == null ? NEW : KNOWN);
			if (reason != null) {
				entry.put("reason", reason);
			}
			out.add(entry);
		}
		return out;
	}

	/**
	 * Who an approval for this tool call would send to, each with its status, as
	 * {recipients, hasNewRecipient, resolved}; null for a tool that sends no email. resolved is false
	 * when the recipients could not be read, which the approval should not present as all known.
	 */
	public static Map<String, Object> forToolCall(User user, String toolName, Map<String, Object> args) {
		if (!SEND_EMAIL.equals(toolName)) {
			return null;
		}
		Map<String, Object> params = args == null ? Map.of() : args;
		List<String> addresses = new ArrayList<>();
		boolean resolved = true;
		try {
			String draftId = text(params.get("draftId"));
			String replyTo = text(params.get("replyTo"));
			if (draftId != null || replyTo != null && addressesIn(params).isEmpty()) {
				MicrosoftOutlookMailHelper mail = new MicrosoftOutlookMailHelper();
				String token = MicrosoftLoginUtils.getValidAccessToken(user);
				if (draftId != null) {
					Map<String, Object> draft = mail.getMessage(token, null, draftId);
					for (String key : List.of("toRecipients", "ccRecipients", "bccRecipients")) {
						addAll(addresses, MicrosoftOutlookMessageMapper.addressArray(draft == null ? null : draft.get(key)));
					}
					resolved = draft != null;
				} else {
					// a reply with no recipients named goes to whoever sent the email it answers
					Map<String, Object> source = mail.getMessage(token, null, replyTo);
					addAll(addresses, MicrosoftOutlookMessageMapper
							.addressArray(source == null || source.get("from") == null ? null : List.of(source.get("from"))));
					resolved = source != null;
				}
			}
			addresses.addAll(addressesIn(params));
		} catch (Exception e) {
			classLogger.warn("Could not read the recipients of the {} call", toolName, e);
			resolved = false;
		}
		List<Map<String, Object>> recipients = classify(user, addresses);
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("recipients", recipients);
		out.put("hasNewRecipient", recipients.stream().anyMatch(r -> NEW.equals(r.get("status"))));
		out.put("resolved", resolved);
		return out;
	}

	private static List<String> addressesIn(Map<String, Object> params) {
		List<String> addresses = new ArrayList<>();
		for (String key : List.of("to", "cc", "bcc")) {
			String value = text(params.get(key));
			if (value != null) {
				for (String part : value.split("[,;]")) {
					addresses.add(part);
				}
			}
		}
		return addresses;
	}

	private static void addAll(List<String> into, String[] values) {
		if (values != null) {
			into.addAll(List.of(values));
		}
	}

	private static String text(Object value) {
		return value instanceof String s && !s.isBlank() ? s.trim() : null;
	}

	// "Name <a@x.com>" and "A@X.com" both come to a@x.com
	static String normalize(String address) {
		if (address == null) {
			return "";
		}
		String value = address.trim();
		int open = value.lastIndexOf('<');
		int close = value.lastIndexOf('>');
		if (open >= 0 && close > open) {
			value = value.substring(open + 1, close);
		}
		return value.trim().toLowerCase(Locale.ROOT);
	}

	private static Map<String, Object> person(Pair<String, String> owner, String address) {
		return CollaborationDbUtils.queryOne(
				"SELECT p.PERSON_ID, p.DISPLAY_NAME, p.RELATIONSHIP, p.RELATIONSHIP_STATE, p.FOLLOW_STATE "
						+ "FROM BRAIN_PERSON_ADDRESS a JOIN BRAIN_PERSON p ON p.OWNER_ID = a.OWNER_ID "
						+ "AND p.OWNER_TYPE = a.OWNER_TYPE AND p.PERSON_ID = a.PERSON_ID "
						+ "WHERE a.OWNER_ID = ? AND a.OWNER_TYPE = ? AND a.VALUE_NORM = ?",
				rs -> {
					Map<String, Object> row = new LinkedHashMap<>();
					row.put("id", CollaborationDbUtils.getString(rs, "PERSON_ID"));
					row.put("name", CollaborationDbUtils.getString(rs, "DISPLAY_NAME"));
					row.put("relationship", CollaborationDbUtils.getString(rs, "RELATIONSHIP"));
					row.put("relationshipState", CollaborationDbUtils.getString(rs, "RELATIONSHIP_STATE"));
					row.put("followState", CollaborationDbUtils.getString(rs, "FOLLOW_STATE"));
					return row;
				}, owner.getValue0(), owner.getValue1(), address);
	}

	// null when the address is new
	private static String reason(Pair<String, String> owner, String address, String ownAddress,
			Map<String, Object> person) {
		if (!ownAddress.isEmpty() && ownAddress.equals(address)
				|| person != null && "self".equals(person.get("relationship"))) {
			return "owner";
		}
		if (person == null) {
			return null;
		}
		if ("confirmed".equals(person.get("relationshipState")) || "following".equals(person.get("followState"))) {
			return "confirmed";
		}
		// on a thread where the owner has sent a message
		boolean wroteTo = CollaborationDbUtils.exists("SELECT 1 FROM BRAIN_THREAD_PARTICIPANT tp "
				+ "WHERE tp.OWNER_ID = ? AND tp.OWNER_TYPE = ? AND tp.PERSON_ID = ? AND EXISTS ("
				+ "SELECT 1 FROM BRAIN_MESSAGE m JOIN BRAIN_PERSON s ON s.OWNER_ID = m.OWNER_ID "
				+ "AND s.OWNER_TYPE = m.OWNER_TYPE AND s.PERSON_ID = m.SENDER_PERSON_ID "
				+ "WHERE m.OWNER_ID = tp.OWNER_ID AND m.OWNER_TYPE = tp.OWNER_TYPE "
				+ "AND m.THREAD_ID = tp.THREAD_ID AND s.RELATIONSHIP = 'self')", owner.getValue0(), owner.getValue1(),
				person.get("id"));
		return wroteTo ? "correspondence" : null;
	}
}
