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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

// person, or automated / list sender, from stored headers; rules only, no model. Written as RELATIONSHIP
// automated (state suggested), so what the owner set is never overwritten. Then threads that only automated
// senders wrote, and calendar replies, are marked AUTOMATED so People, Topics and Work leave them out.
public final class BrainSenderTyping {

	public static final String AUTOMATED = "automated";
	private static final String SUGGESTED = "suggested";

	// local parts, as a whole word anywhere: noreply1, dte-reminder, weekly-digest, us-va-team, buildbot
	private static final Pattern ADDRESS = Pattern.compile("(^|[-_.])(no-?reply|do-?not-?reply|notifications?|"
			+ "newsletters?|digest|mailer-daemon|postmaster|bounces?|alerts?|updates|news|marketing|info|support|"
			+ "promos?|promotions|offers|deals|blast|campaigns?|reminders?|approvals?|automated|automation|appreg|"
			+ "workflow|system|admin|helpdesk|servicedesk|announcements?|events|surveys?|comms|communications|team|"
			+ "teams|dl|mailbox|\\w*bot)\\d*([-_.+]|$)", Pattern.CASE_INSENSITIVE);
	// display names: "DTE-REMINDER", "DTTL IAM AppReg", "US-VA Team", "Deloitte Security Office", "... Daily"
	private static final Pattern NAME = Pattern.compile("(?<![\\p{L}\\p{N}])(no-?reply|do not reply|automated|"
			+ "automation|reminders?|approvals?|notifications?|alerts?|digest|newsletters?|daily|weekly|monthly|team|"
			+ "teams|office|services|service ?desk|help ?desk|support|admin|administrator|security|communications|"
			+ "comms|announcements?|events|survey|mailbox|system|portal|workflow|appreg|calendar|updates|news|"
			+ "insights|marketing|program office)(?![\\p{L}\\p{N}])|\\s&\\s", Pattern.CASE_INSENSITIVE);
	// "Last, First" and "Last, First (NIH/NIAID) [C]"
	private static final Pattern PERSON_NAME = Pattern.compile("^[\\p{L}'. -]+,\\s*[\\p{L}'. -]+(\\s*[(\\[].*)?$");
	// calendar replies, auto-replies and delivery reports; invitations stay, they may need an answer
	private static final Pattern SYSTEM_SUBJECT = Pattern.compile("^\\s*(accepted|declined|tentative|"
			+ "tentatively accepted|canceled|cancelled|automatic reply|auto[- ]?reply|out of office|undeliverable|"
			+ "delivery status notification|read)\\s*:", Pattern.CASE_INSENSITIVE);

	private BrainSenderTyping() {
	}

	/** An automated address by its local part. */
	public static boolean automatedAddress(String address) {
		int at = address == null ? -1 : address.indexOf('@');
		return at > 0 && ADDRESS.matcher(address.substring(0, at)).find();
	}

	/** A display name that is a team, list or system, not a person. */
	public static boolean automatedName(String name) {
		return name != null && NAME.matcher(name).find();
	}

	static boolean personName(String name) {
		return name != null && PERSON_NAME.matcher(name.trim()).matches();
	}

	static boolean systemSubject(String subject) {
		return subject != null && SYSTEM_SUBJECT.matcher(subject).find();
	}

	private record Person(String id, String email, String name, String relationship, String state) {
	}

	/** Types everyone the owner has not typed, then marks automated threads. Returns counts. */
	public static Map<String, Object> run(String ownerId, String ownerType) {
		String selfId = CollaborationDbUtils.queryOne("SELECT PERSON_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? AND RELATIONSHIP = ?", rs -> rs.getString(1), ownerId, ownerType, "self");
		List<Person> people = CollaborationDbUtils.query("SELECT PERSON_ID, EMAIL_NORM, DISPLAY_NAME, RELATIONSHIP, "
				+ "RELATIONSHIP_STATE FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> new Person(rs.getString(1), rs.getString(2), CollaborationDbUtils.getString(rs, "DISPLAY_NAME"),
						rs.getString(4), rs.getString(5)),
				ownerId, ownerType);

		// per sender: messages, not addressed to the owner, bulk
		Map<String, int[]> sent = new HashMap<>();
		CollaborationDbUtils.query("SELECT SENDER_PERSON_ID, COUNT(*) AS N, SUM(CASE WHEN TO_ME = ? THEN 1 ELSE 0 END) "
				+ "AS NOT_ME, SUM(CASE WHEN BULK = ? THEN 1 ELSE 0 END) AS BULK_N FROM BRAIN_MESSAGE WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND THREAD_ID IS NOT NULL AND SENDER_PERSON_ID IS NOT NULL GROUP BY SENDER_PERSON_ID",
				rs -> sent.put(rs.getString(1), new int[] { rs.getInt("N"), rs.getInt("NOT_ME"), rs.getInt("BULK_N") }),
				false, true, ownerId, ownerType);
		// senders per thread; a hidden sender counts as unknown ("")
		Map<String, Set<String>> senders = new HashMap<>();
		CollaborationDbUtils.query("SELECT THREAD_ID, SENDER_PERSON_ID FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? AND THREAD_ID IS NOT NULL", rs -> {
					String s = rs.getString(2);
					senders.computeIfAbsent(rs.getString(1), k -> new HashSet<>()).add(s == null ? "" : s);
					return null;
				}, ownerId, ownerType);
		// the owner wrote to them: on To or Cc of a thread the owner sent on
		Set<String> youWrote = new HashSet<>();
		Map<String, int[]> threadsOf = new HashMap<>();
		CollaborationDbUtils.query("SELECT THREAD_ID, PERSON_ID, ROLES_JSON FROM BRAIN_THREAD_PARTICIPANT WHERE "
				+ "OWNER_ID = ? AND OWNER_TYPE = ?", rs -> {
					String thread = rs.getString(1);
					String person = rs.getString(2);
					String roles = String.valueOf(CollaborationDbUtils.getString(rs, "ROLES_JSON"));
					Set<String> s = senders.getOrDefault(thread, Set.of());
					if (selfId != null && s.contains(selfId) && (roles.contains("\"to\"") || roles.contains("\"cc\""))) {
						youWrote.add(person);
					}
					// [threads they wrote on, threads where they were the only sender]
					int[] t = threadsOf.computeIfAbsent(person, k -> new int[2]);
					if (s.contains(person)) {
						t[0]++;
						if (s.size() == 1) {
							t[1]++;
						}
					}
					return null;
				}, ownerId, ownerType);

		// share of "Last, First" names per organisation; a team mailbox stands out among them
		Map<String, int[]> nameStyle = new HashMap<>();
		for (Person p : people) {
			String org = BrainOrgDomains.org(BrainMailImport.domain(p.email()));
			if (org != null && p.name() != null) {
				int[] c = nameStyle.computeIfAbsent(org, k -> new int[2]);
				c[0]++;
				c[1] += personName(p.name()) ? 1 : 0;
			}
		}

		Set<String> automated = new HashSet<>();
		List<Object[]> changes = new ArrayList<>();
		for (Person p : people) {
			if (p.id().equals(selfId)) {
				continue;
			}
			boolean isAutomated = isAutomated(p, sent.get(p.id()), threadsOf.get(p.id()), youWrote.contains(p.id()),
					nameStyle.get(BrainOrgDomains.org(BrainMailImport.domain(p.email()))));
			boolean ownerSet = p.state() != null && !SUGGESTED.equals(p.state());
			if (ownerSet) {
				if (AUTOMATED.equals(p.relationship())) {
					automated.add(p.id());
				}
				continue;
			}
			if (isAutomated) {
				automated.add(p.id());
				if (!AUTOMATED.equals(p.relationship())) {
					changes.add(new Object[] { AUTOMATED, SUGGESTED, p.id() });
				}
			} else if (AUTOMATED.equals(p.relationship())) {
				// ranking sets colleague or external again
				changes.add(new Object[] { null, null, p.id() });
			}
		}

		// threads: only automated senders, or a calendar reply / auto-reply
		List<String> threads = new ArrayList<>();
		CollaborationDbUtils.query("SELECT THREAD_ID, SUBJECT, AUTOMATED FROM BRAIN_THREAD WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ?", rs -> {
					if (Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "AUTOMATED"))) {
						return null;
					}
					String id = rs.getString(1);
					Set<String> s = senders.getOrDefault(id, Set.of());
					if (systemSubject(CollaborationDbUtils.getString(rs, "SUBJECT"))
							|| (!s.isEmpty() && automated.containsAll(s))) {
						threads.add(id);
					}
					return null;
				}, ownerId, ownerType);

		CollaborationDbUtils.batch(conn -> {
			for (Object[] c : changes) {
				CollaborationDbUtils.update(conn, "UPDATE BRAIN_PERSON SET RELATIONSHIP = ?, RELATIONSHIP_STATE = ?, "
						+ "UPDATED_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ?", c[0], c[1],
						CollaborationDbUtils.now(), ownerId, ownerType, c[2]);
			}
			for (String id : threads) {
				CollaborationDbUtils.update(conn, "UPDATE BRAIN_THREAD SET AUTOMATED = ? WHERE OWNER_ID = ? AND "
						+ "OWNER_TYPE = ? AND THREAD_ID = ?", true, ownerId, ownerType, id);
			}
		});

		Map<String, Object> out = new HashMap<>();
		out.put("automatedPeople", automated.size());
		out.put("automatedThreads", threads.size());
		return out;
	}

	// sent: [messages, not to me, bulk]; threads: [wrote on, only sender]; style: [names, "Last, First" names]
	private static boolean isAutomated(Person p, int[] sent, int[] threads, boolean youWrote, int[] style) {
		if (automatedAddress(p.email())) {
			return true;
		}
		boolean person = personName(p.name());
		if (!person && automatedName(p.name())) {
			return true;
		}
		int messages = sent == null ? 0 : sent[0];
		if (youWrote || messages < 2) {
			return false;
		}
		int weak = 1;
		if (sent[1] >= 2 && 2 * sent[1] >= messages) {
			weak++;
		}
		if (sent[2] >= 2 && 2 * sent[2] >= messages) {
			weak++;
		}
		if (threads != null && threads[1] >= 2 && 5 * threads[1] >= 4 * threads[0]) {
			weak++;
		}
		// most people here are "Last, First" and this one is not
		if (!person && style != null && style[0] >= 5 && 10 * style[1] >= 6 * style[0]) {
			weak++;
		}
		// one-way (counted as the first) plus two more
		return weak >= 3;
	}
}
