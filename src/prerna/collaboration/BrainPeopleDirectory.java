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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.auth.User;

// people from the Microsoft directory: colleague, guest, shared mailbox or distribution list, with title and
// department. A directory answer is a fact (state "directory"): the header rules and ranking leave it alone,
// and only the owner overrides it. A mail source with no directory changes nothing.
final class BrainPeopleDirectory {

	static final String DIRECTORY = "directory";
	// the people who wrote to the owner come first; the rest of a big mailbox stays with the rules
	private static final int MAX_LOOKUPS = 1000;
	private static final long FRESH_MS = 30L * 24 * 60 * 60 * 1000;

	private BrainPeopleDirectory() {
	}

	private record Candidate(String id, String email) {
	}

	/** Looks up the people that matter and records what the directory says; returns counts. */
	static Map<String, Object> apply(User user, BrainMailHeaderSource source, String ownerId, String ownerType,
			String selfId, Set<String> alsoCheck) {
		Timestamp now = CollaborationDbUtils.now();
		Timestamp stale = new Timestamp(now.getTime() - FRESH_MS);
		// senders first, by how much they wrote; the owner's choices and fresh answers are skipped
		List<Candidate> candidates = new ArrayList<>();
		Set<String> seen = new LinkedHashSet<>();
		CollaborationDbUtils.query("SELECT p.PERSON_ID, p.EMAIL_NORM, p.RELATIONSHIP_STATE, p.DIRECTORY_CHECKED_AT, "
				+ "COALESCE(m.N, 0) AS N FROM BRAIN_PERSON p LEFT JOIN (SELECT SENDER_PERSON_ID, COUNT(*) AS N FROM "
				+ "BRAIN_MESSAGE WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND SENDER_PERSON_ID IS NOT NULL GROUP BY "
				+ "SENDER_PERSON_ID) m ON m.SENDER_PERSON_ID = p.PERSON_ID WHERE p.OWNER_ID = ? AND p.OWNER_TYPE = ? "
				+ "ORDER BY N DESC, p.PERSON_ID", rs -> {
					String id = rs.getString("PERSON_ID");
					String email = rs.getString("EMAIL_NORM");
					Timestamp checked = rs.getTimestamp("DIRECTORY_CHECKED_AT");
					boolean ownerSet = "confirmed".equals(rs.getString("RELATIONSHIP_STATE"));
					boolean fresh = checked != null && checked.after(stale);
					boolean wanted = email != null && alsoCheck.contains(email);
					if (!id.equals(selfId) && email != null && !ownerSet && (!fresh || wanted)
							&& (candidates.size() < MAX_LOOKUPS || wanted) && seen.add(email)) {
						candidates.add(new Candidate(id, email));
					}
					return null;
				}, ownerId, ownerType, ownerId, ownerType);

		Map<String, Object> out = new LinkedHashMap<>();
		BrainMailHeaderSource.Directory directory = candidates.isEmpty() ? null
				: source.lookup(user, candidates.stream().map(Candidate::email).toList());
		if (directory == null) {
			out.put("directory", "none");
			return out;
		}
		int colleagues = 0;
		int automated = 0;
		List<Object[]> found = new ArrayList<>();
		List<String> missed = new ArrayList<>();
		for (Candidate c : candidates) {
			Map<String, Object> e = directory.entries().get(c.email());
			if (e == null) {
				// only a finished lookup proves the directory does not know them
				if (directory.complete()) {
					missed.add(c.id());
				}
				continue;
			}
			String kind = String.valueOf(e.get("kind"));
			String relationship = switch (kind) {
			case "person" -> "colleague";
			case "guest" -> "external";
			default -> BrainSenderTyping.AUTOMATED;
			};
			colleagues += "colleague".equals(relationship) ? 1 : 0;
			automated += BrainSenderTyping.AUTOMATED.equals(relationship) ? 1 : 0;
			found.add(new Object[] { relationship, e.get("id"), e.get("title"), e.get("department"), e.get("company"),
					c.id() });
		}
		CollaborationDbUtils.batch(conn -> {
			for (Object[] f : found) {
				CollaborationDbUtils.update(conn, "UPDATE BRAIN_PERSON SET RELATIONSHIP = ?, RELATIONSHIP_STATE = ?, "
						+ "DIRECTORY_ID = ?, JOB_TITLE = COALESCE(?, JOB_TITLE), DEPARTMENT = COALESCE(?, DEPARTMENT), "
						+ "COMPANY = COALESCE(?, COMPANY), DIRECTORY_CHECKED_AT = ?, UPDATED_AT = ? WHERE OWNER_ID = ? "
						+ "AND OWNER_TYPE = ? AND PERSON_ID = ?", f[0], DIRECTORY, f[1], f[2], f[3], f[4], now, now,
						ownerId, ownerType, f[5]);
			}
			// not in the directory: back to the rules if an old answer said otherwise
			for (String id : missed) {
				CollaborationDbUtils.update(conn, "UPDATE BRAIN_PERSON SET DIRECTORY_CHECKED_AT = ?, RELATIONSHIP = "
						+ "CASE WHEN RELATIONSHIP_STATE = ? THEN NULL ELSE RELATIONSHIP END, RELATIONSHIP_STATE = CASE "
						+ "WHEN RELATIONSHIP_STATE = ? THEN NULL ELSE RELATIONSHIP_STATE END WHERE OWNER_ID = ? AND "
						+ "OWNER_TYPE = ? AND PERSON_ID = ?", now, DIRECTORY, DIRECTORY, ownerId, ownerType, id);
			}
		});
		out.put("directory", directory.complete() ? "read" : "partial");
		out.put("directoryChecked", candidates.size());
		out.put("directoryColleagues", colleagues);
		out.put("directoryAutomated", automated);
		return out;
	}
}
