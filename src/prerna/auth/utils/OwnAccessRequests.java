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
package prerna.auth.utils;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.javatuples.Pair;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * The signed in user's own access requests on a project or an engine: reading
 * the one that waits for a decision, and withdrawing it. A request is the
 * user's when any of their logins made it, matched on the id and the login type
 * together. Asking again replaces a waiting request, so a user has at most one.
 */
final class OwnAccessRequests {

	private static final String WAITING = "NEW_REQUEST";
	private static final String WITHDRAWN = "WITHDRAWN";

	/** The project access requests table and the column naming the project */
	static final Target PROJECT = new Target("PROJECTACCESSREQUEST", "PROJECTID");
	/** The engine access requests table and the column naming the engine */
	static final Target ENGINE = new Target("ENGINEACCESSREQUEST", "ENGINEID");

	private OwnAccessRequests() {
	}

	/**
	 * Reads the user's access request on a project or an engine that waits for a
	 * decision.
	 *
	 * @param target   the requests table of the project or the engine
	 * @param targetId the project or engine id
	 * @param user     the signed in user
	 * @return the request: {@code id}, {@code permission} (the level asked for),
	 *         {@code comment} (the reason given, or null) and {@code requestedAt}
	 *         (UTC, ISO 8601); null when the user has none waiting
	 */
	static Map<String, Object> waiting(Target target, String targetId, User user) {
		String table = target.table();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(table + "__ID", "ID"));
		qs.addSelector(new QueryColumnSelector(table + "__PERMISSION", "PERMISSION"));
		qs.addSelector(new QueryColumnSelector(table + "__REQUEST_REASON", "REQUEST_REASON"));
		qs.addSelector(new QueryColumnSelector(table + "__REQUEST_TIMESTAMP", "REQUEST_TIMESTAMP"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(table + "__" + target.column(), "==", targetId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(table + "__APPROVER_DECISION", "==", WAITING));
		qs.addExplicitFilter(AbstractSecurityUtils.getUserIdAndTypeFilter(user, table + "__REQUEST_USERID",
				table + "__REQUEST_TYPE"));
		qs.addOrderBy(new QueryColumnOrderBySelector(table + "__REQUEST_TIMESTAMP", "DESC"));
		qs.setLimit(1);
		List<Map<String, Object>> rows = QueryExecutionUtility.flushRsToMap(SystemEngineRegistry.getSecurityDb(), qs);
		if (rows.isEmpty()) {
			return null;
		}
		Map<String, Object> row = rows.get(0);
		Map<String, Object> request = new LinkedHashMap<>();
		request.put("id", row.get("ID"));
		Object level = row.get("PERMISSION");
		request.put("permission", level == null ? null : AccessPermissionEnum.getPermissionValueById(level.toString()));
		request.put("comment", row.get("REQUEST_REASON"));
		request.put("requestedAt", PermissionBatchUtils.toIsoUtc(row.get("REQUEST_TIMESTAMP")));
		return request;
	}

	/**
	 * Withdraws the user's access request on a project or an engine that waits
	 * for a decision. The request is kept, marked withdrawn by the user, so the
	 * record shows it was made and taken back.
	 *
	 * @param target   the requests table of the project or the engine
	 * @param targetId the project or engine id
	 * @param user     the signed in user
	 * @return true when a waiting request was withdrawn, false when the user had
	 *         none
	 * @throws IllegalArgumentException when the request cannot be saved
	 */
	static boolean withdraw(Target target, String targetId, User user) {
		List<Pair<String, String>> logins = new ArrayList<>();
		for (AuthProvider login : user.getLogins()) {
			logins.add(Pair.with(user.getAccessToken(login).getId(), login.name()));
		}
		if (logins.isEmpty()) {
			return false;
		}
		StringBuilder sql = new StringBuilder("UPDATE ").append(target.table())
				.append(" SET APPROVER_DECISION = ?, APPROVER_USERID = ?, APPROVER_TYPE = ?, APPROVER_TIMESTAMP = ?")
				.append(" WHERE ").append(target.column()).append(" = ? AND APPROVER_DECISION = ? AND (");
		for (int i = 0; i < logins.size(); i++) {
			sql.append(i == 0 ? "" : " OR ").append("(REQUEST_USERID = ? AND REQUEST_TYPE = ?)");
		}
		sql.append(")");
		Pair<String, String> withdrawnBy = User.getPrimaryUserIdAndTypePair(user);
		Timestamp now = Utility.getCurrentSqlTimestampUTC();
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		try {
			int withdrawn = QueryExecutionUtility.executeUpdate(securityDb, sql.toString(), ps -> {
				int index = 1;
				ps.setString(index++, WITHDRAWN);
				ps.setString(index++, withdrawnBy.getValue0());
				ps.setString(index++, withdrawnBy.getValue1());
				ps.setTimestamp(index++, now);
				ps.setString(index++, targetId);
				ps.setString(index++, WAITING);
				for (Pair<String, String> login : logins) {
					ps.setString(index++, login.getValue0());
					ps.setString(index++, login.getValue1());
				}
			});
			return withdrawn > 0;
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			throw new IllegalArgumentException("An error occurred withdrawing the access request: " + e.getMessage(),
					e);
		}
	}

	/**
	 * A table of access requests and the column that names what each is for.
	 *
	 * @param table  the requests table
	 * @param column the column naming the project or engine
	 */
	record Target(String table, String column) {
	}

}
