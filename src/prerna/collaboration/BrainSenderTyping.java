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

// automated / list senders, from what the classifier decided about their threads; no name or address matching.
// Written as RELATIONSHIP automated (state suggested), so what the owner or the directory set is never overwritten.
public final class BrainSenderTyping {

	public static final String AUTOMATED = "automated";
	private static final String SUGGESTED = "suggested";
	private BrainSenderTyping() {
	}

	/**
	 * After a classifier run: a sender the owner never wrote to, all of whose threads the classifier marked
	 * automated, is suggested as automated; one who no longer fits goes back to the ranking.
	 * Returns how many people are automated.
	 */
	public static int fromThreads(String ownerId, String ownerType) {
		String selfId = CollaborationDbUtils.queryOne("SELECT PERSON_ID FROM BRAIN_PERSON WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ? AND RELATIONSHIP = ?", rs -> rs.getString(1), ownerId, ownerType, "self");
		Set<String> automatedThreads = new HashSet<>(CollaborationDbUtils.query("SELECT THREAD_ID FROM BRAIN_THREAD "
				+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND AUTOMATED = ?", rs -> rs.getString(1), ownerId, ownerType,
				true));
		// threads each person wrote on, and who wrote on each thread
		Map<String, Set<String>> threadsOf = new HashMap<>();
		Map<String, Set<String>> sendersOf = new HashMap<>();
		CollaborationDbUtils.query("SELECT DISTINCT THREAD_ID, SENDER_PERSON_ID FROM BRAIN_MESSAGE WHERE OWNER_ID = ? "
				+ "AND OWNER_TYPE = ? AND THREAD_ID IS NOT NULL AND SENDER_PERSON_ID IS NOT NULL", rs -> {
					threadsOf.computeIfAbsent(rs.getString(2), k -> new HashSet<>()).add(rs.getString(1));
					sendersOf.computeIfAbsent(rs.getString(1), k -> new HashSet<>()).add(rs.getString(2));
					return null;
				}, ownerId, ownerType);
		Set<String> youWrote = new HashSet<>();
		if (selfId != null) {
			for (String thread : threadsOf.getOrDefault(selfId, Set.of())) {
				youWrote.addAll(sendersOf.getOrDefault(thread, Set.of()));
			}
		}

		int automated = 0;
		List<Object[]> changes = new ArrayList<>();
		for (Object[] p : CollaborationDbUtils.query("SELECT PERSON_ID, RELATIONSHIP, RELATIONSHIP_STATE FROM "
				+ "BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ?",
				rs -> new Object[] { rs.getString(1), rs.getString(2), rs.getString(3) }, ownerId, ownerType)) {
			String id = (String) p[0];
			boolean isAutomated = AUTOMATED.equals(p[1]);
			// the owner or the directory decided
			if (id.equals(selfId) || (p[2] != null && !SUGGESTED.equals(p[2]))) {
				automated += isAutomated ? 1 : 0;
				continue;
			}
			Set<String> threads = threadsOf.getOrDefault(id, Set.of());
			boolean fits = !youWrote.contains(id) && !threads.isEmpty() && automatedThreads.containsAll(threads);
			automated += fits ? 1 : 0;
			if (fits && !isAutomated) {
				changes.add(new Object[] { AUTOMATED, SUGGESTED, id });
			} else if (!fits && isAutomated) {
				// ranking sets colleague or external again
				changes.add(new Object[] { null, null, id });
			}
		}
		CollaborationDbUtils.batch(conn -> {
			for (Object[] c : changes) {
				CollaborationDbUtils.update(conn, "UPDATE BRAIN_PERSON SET RELATIONSHIP = ?, RELATIONSHIP_STATE = ?, "
						+ "UPDATED_AT = ? WHERE OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ?", c[0], c[1],
						CollaborationDbUtils.now(), ownerId, ownerType, c[2]);
			}
		});
		return automated;
	}
}
