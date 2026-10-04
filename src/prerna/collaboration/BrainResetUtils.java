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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;

// Wipes one owner's Collaboration data so onboarding starts fresh
public final class BrainResetUtils {

	private static final Logger classLogger = LogManager.getLogger(BrainResetUtils.class);

	// the owner row and source connections carry the Microsoft link; everything
	// else goes
	private static final Set<String> KEPT = Set.of("COLLAB_OWNER", "SOURCE_CONNECTION");

	private BrainResetUtils() {
	}

	public static Map<String, Object> resetMyData(User user) {
		Pair<String, String> owner = CollaborationDbUtils.ownerOf(user);
		String ownerId = owner.getValue0();
		String ownerType = owner.getValue1();
		// same lock as job start, so no import or classify begins mid-reset
		synchronized (CollaborationDbUtils.ownerLock("job", ownerId, ownerType)) {
			if (CollaborationJobUtils.anyRunning(ownerId, ownerType)) {
				throw new IllegalStateException(
						"An import or classify is still running; wait for it to finish, then reset");
			}
			Map<String, Object> deleted = new LinkedHashMap<>();
			CollaborationDbUtils.inTransaction(conn -> {
				for (String table : tables()) {
					int rows = CollaborationDbUtils.update(conn,
							"DELETE FROM " + table + " WHERE OWNER_ID = ? AND OWNER_TYPE = ?", ownerId, ownerType);
					if (rows > 0) {
						deleted.put(table, rows);
					}
				}
			});
			int total = deleted.values().stream().mapToInt(v -> (Integer) v).sum();
			classLogger.info("Collaboration data reset for {} {}: {} rows", ownerType, ownerId, total);
			Map<String, Object> result = new LinkedHashMap<>();
			result.put("rows", total);
			result.put("tables", deleted);
			return result;
		}
	}

	// every table in the schema, newest first so child rows go before the rows they
	// point at
	private static List<String> tables() {
		List<String> tables = new ArrayList<>();
		new CollaborationOwlCreator(CollaborationDbUtils.db().getQueryUtil()).getDBSchema()
				.forEach(t -> tables.add(t.getValue0()));
		tables.removeAll(KEPT);
		Collections.reverse(tables);
		return tables;
	}
}
