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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// STRENGTH (0-100, log scale relative to your strongest contact) and LAST_CONTACT_AT from stored headers, plus a suggested
// relationship (same domain as you: colleague, else external) where you have not set one. Rules only, no model.
public final class BrainPeopleRanking {

	private static final double TWO_WAY = 4;
	private static final double I_WROTE_TO = 2;
	private static final double I_CC = 0.5;
	private static final double THEY_WROTE = 1;
	private static final int THEIR_CAP_PER_THREAD = 3;
	private static final double RECENCY_DAYS = 30;

	private BrainPeopleRanking() {
	}

	public static void rank(String ownerId, String ownerType, String selfId, String myDomain) {
		BrainOrgDomains.Org org = BrainOrgDomains.load(ownerId, ownerType, myDomain);
		// automated and list senders are not ranked
		Set<String> automated = new HashSet<>(CollaborationDbUtils.query(
				"SELECT PERSON_ID FROM BRAIN_PERSON WHERE " + "OWNER_ID = ? AND OWNER_TYPE = ? AND RELATIONSHIP = ?",
				rs -> rs.getString(1), ownerId, ownerType, BrainSenderTyping.AUTOMATED));
		// messages per thread and sender; calendar replies are not writing to someone
		Map<String, Map<String, Integer>> sent = new HashMap<>();
		CollaborationDbUtils.query("SELECT THREAD_ID, SENDER_PERSON_ID, COUNT(*) AS N FROM BRAIN_MESSAGE "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND THREAD_ID IS NOT NULL AND SENDER_PERSON_ID IS NOT NULL "
				+ "AND (MEETING IS NULL OR MEETING = ?) GROUP BY THREAD_ID, SENDER_PERSON_ID", rs -> {
					sent.computeIfAbsent(rs.getString("THREAD_ID"), k -> new HashMap<>())
							.put(rs.getString("SENDER_PERSON_ID"), rs.getInt("N"));
					return null;
				}, ownerId, ownerType, false);

		Map<String, int[]> twoWayAndTheirs = new HashMap<>();
		Map<String, Double> mine = new HashMap<>();
		Map<String, Timestamp> last = new HashMap<>();
		CollaborationDbUtils
				.query("SELECT THREAD_ID, PERSON_ID, ROLES_JSON, LAST_SEEN_AT FROM BRAIN_THREAD_PARTICIPANT "
						+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND (INCLUDED IS NULL OR INCLUDED = ?)", rs -> {
							String person = rs.getString("PERSON_ID");
							if (person.equals(selfId) || automated.contains(person)) {
								return null;
							}
							Map<String, Integer> bySender = sent.getOrDefault(rs.getString("THREAD_ID"), Map.of());
							int theirs = bySender.getOrDefault(person, 0);
							int myCount = selfId == null ? 0 : bySender.getOrDefault(selfId, 0);
							int[] tt = twoWayAndTheirs.computeIfAbsent(person, k -> new int[2]);
							if (theirs > 0 && myCount > 0) {
								tt[0]++;
							}
							tt[1] += Math.min(theirs, THEIR_CAP_PER_THREAD);
							String roles = String.valueOf(CollaborationDbUtils.getString(rs, "ROLES_JSON"));
							double weight = roles.contains("\"to\"") ? 1
									: roles.contains("\"cc\"") ? I_CC / I_WROTE_TO : 0;
							mine.merge(person, weight * myCount, Double::sum);
							Timestamp seen = rs.getTimestamp("LAST_SEEN_AT");
							if (seen != null && (last.get(person) == null || seen.after(last.get(person)))) {
								last.put(person, seen);
							}
							return null;
						}, ownerId, ownerType, true);

		Timestamp now = CollaborationDbUtils.now();
		Map<String, Double> raw = new HashMap<>();
		double max = 0;
		for (Map.Entry<String, int[]> e : twoWayAndTheirs.entrySet()) {
			String person = e.getKey();
			double score = TWO_WAY * e.getValue()[0] + I_WROTE_TO * mine.getOrDefault(person, 0.0)
					+ THEY_WROTE * e.getValue()[1];
			Timestamp seen = last.get(person);
			double days = seen == null ? 365 : Math.max(0, (now.getTime() - seen.getTime()) / 86_400_000.0);
			score *= 0.5 + 0.5 * Math.exp(-days / RECENCY_DAYS);
			raw.put(person, score);
			max = Math.max(max, score);
		}

		List<String[]> people = CollaborationDbUtils.query(
				"SELECT PERSON_ID, EMAIL_NORM, RELATIONSHIP, RELATIONSHIP_STATE "
						+ "FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> new String[] { rs.getString(1), rs.getString(2), rs.getString(3), rs.getString(4) }, ownerId,
				ownerType);
		Set<String> seenPeople = new HashSet<>(raw.keySet());
		double top = max;
		CollaborationDbUtils.inTransaction(conn -> {
			for (String[] p : people) {
				if (automated.contains(p[0])) {
					CollaborationDbUtils.update(conn, "UPDATE BRAIN_PERSON SET STRENGTH = ? WHERE OWNER_ID = ? AND "
							+ "OWNER_TYPE = ? AND PERSON_ID = ?", 0, ownerId, ownerType, p[0]);
					continue;
				}
				if (p[0].equals(selfId) || !seenPeople.contains(p[0])) {
					continue;
				}
				// log scale: one very busy contact no longer pushes everyone else to the bottom
				int strength = top <= 0 ? 0 : (int) Math.round(100 * Math.log1p(raw.get(p[0])) / Math.log1p(top));
				List<Object> params = new ArrayList<>(List.of(strength));
				String sql = "UPDATE BRAIN_PERSON SET STRENGTH = ?, LAST_CONTACT_AT = ?";
				params.add(last.get(p[0]));
				// a suggestion is redone each time (the org's domains may have changed); the
				// owner's choice stays
				if (p[2] == null
						|| ("suggested".equals(p[3]) && ("colleague".equals(p[2]) || "external".equals(p[2])))) {
					sql += ", RELATIONSHIP = ?, RELATIONSHIP_STATE = ?";
					params.add(org.isMine(BrainMailImport.domain(p[1])) ? "colleague" : "external");
					params.add("suggested");
				}
				sql += ", UPDATED_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ?";
				params.addAll(List.of(now, ownerId, ownerType, p[0]));
				CollaborationDbUtils.update(conn, sql, params.toArray());
			}
		});
	}
}
