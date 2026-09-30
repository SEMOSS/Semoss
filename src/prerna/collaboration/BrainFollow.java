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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

// who the owner follows: their people, VIPs on top. Brain suggests from the org chart and from two-way mail
// (state suggested); the owner follows or declines, and a declined person is not suggested again.
public final class BrainFollow {

	public static final String FOLLOWING = "following";
	public static final String SUGGESTED = "suggested";
	public static final String DECLINED = "declined";
	public static final Set<String> STATES = Set.of(FOLLOWING, SUGGESTED, DECLINED);
	// suggestions from mail on top of the org chart
	private static final int FROM_MAIL = 25;
	private static final int MIN_THREADS = 2;

	private BrainFollow() {
	}

	/**
	 * Suggests people to follow; org is person id to "Your manager", "Reports to
	 * you" or "Same manager". Returns how many were suggested.
	 */
	static int suggest(String ownerId, String ownerType, String selfId, Map<String, String> org) {
		// threads the owner wrote on, and who else was on To or Cc there
		Set<String> mine = new HashSet<>();
		if (selfId != null) {
			mine.addAll(CollaborationDbUtils.query(
					"SELECT DISTINCT THREAD_ID FROM BRAIN_MESSAGE WHERE OWNER_ID = ? AND "
							+ "OWNER_TYPE = ? AND SENDER_PERSON_ID = ? AND THREAD_ID IS NOT NULL",
					rs -> rs.getString(1), ownerId, ownerType, selfId));
		}
		Map<String, Integer> together = new HashMap<>();
		CollaborationDbUtils.query("SELECT THREAD_ID, PERSON_ID, ROLES_JSON FROM BRAIN_THREAD_PARTICIPANT WHERE "
				+ "OWNER_ID = ? AND OWNER_TYPE = ?", rs -> {
					String roles = String.valueOf(CollaborationDbUtils.getString(rs, "ROLES_JSON"));
					if (mine.contains(rs.getString(1))
							&& (roles.contains("\"to\"") || roles.contains("\"cc\"") || roles.contains("\"from\""))) {
						together.merge(rs.getString(2), 1, Integer::sum);
					}
					return null;
				}, ownerId, ownerType);

		// not the owner, not automated, and nothing decided yet; VIPs are followed
		// outright
		Map<String, String> reasons = new LinkedHashMap<>();
		List<String> vips = new ArrayList<>();
		List<String[]> byStrength = CollaborationDbUtils.query("SELECT PERSON_ID, RELATIONSHIP, IS_VIP, FOLLOW_STATE "
				+ "FROM BRAIN_PERSON WHERE OWNER_ID = ? AND OWNER_TYPE = ? ORDER BY COALESCE(STRENGTH, 0) DESC, PERSON_ID",
				rs -> new String[] { rs.getString(1), rs.getString(2),
						String.valueOf(Boolean.TRUE.equals(CollaborationDbUtils.getBoolean(rs, "IS_VIP"))),
						rs.getString(4) },
				ownerId, ownerType);
		for (String[] p : byStrength) {
			if (p[0].equals(selfId) || p[3] != null || BrainSenderTyping.AUTOMATED.equals(p[1])) {
				continue;
			}
			if (Boolean.parseBoolean(p[2])) {
				vips.add(p[0]);
			} else if (org.containsKey(p[0])) {
				reasons.put(p[0], org.get(p[0]));
			}
		}
		int fromMail = 0;
		for (String[] p : byStrength) {
			if (fromMail >= FROM_MAIL) {
				break;
			}
			int threads = together.getOrDefault(p[0], 0);
			if (p[0].equals(selfId) || p[3] != null || BrainSenderTyping.AUTOMATED.equals(p[1]) || vips.contains(p[0])
					|| reasons.containsKey(p[0]) || threads < MIN_THREADS) {
				continue;
			}
			reasons.put(p[0], "You write to each other on " + threads + " threads");
			fromMail++;
		}
		CollaborationDbUtils.batch(conn -> {
			for (String id : vips) {
				CollaborationDbUtils.update(conn,
						"UPDATE BRAIN_PERSON SET FOLLOW_STATE = ?, FOLLOW_REASON = ? WHERE "
								+ "OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ?",
						FOLLOWING, "VIP", ownerId, ownerType, id);
			}
			for (Map.Entry<String, String> r : reasons.entrySet()) {
				CollaborationDbUtils.update(conn,
						"UPDATE BRAIN_PERSON SET FOLLOW_STATE = ?, FOLLOW_REASON = ? WHERE "
								+ "OWNER_ID = ? AND OWNER_TYPE = ? AND PERSON_ID = ?",
						SUGGESTED, r.getValue(), ownerId, ownerType, r.getKey());
			}
		});
		return reasons.size();
	}
}
