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
package prerna.reactor.agent.admin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import prerna.auth.User;
import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.agent.run.AdminAccess;
import prerna.reactor.agent.run.AgentRunService;
import prerna.reactor.agent.run.AgentRunStore;
import prerna.usertracking.AuditTargetCatalog;
import prerna.usertracking.AuditTargetType;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.ValueUtils;

/**
 * The reads behind the admin Agent Runs page: every user's rooms with agent
 * runs, and the runs in them, as their owners see them on an agent's Activity
 * tab. Each room carries the user it belongs to and its agent's name, and each
 * run its model's name, since an admin may not be able to read the model's
 * metadata.
 *
 * <p>
 * The reads are reached only through {@link #getInstance(User)}, which refuses
 * anyone who is not an admin. The {@link AdminAccess} it gets for the admin is
 * what each {@code ...ForAdmin} read of {@link AgentRunStore} and
 * {@link AgentRunService} takes, so none of them runs without the check either.
 *
 * <p>
 * It is the one agent class that reads the security database, for each room's
 * user, and {@code SystemEngineRegistry} allows that only from this admin
 * package. The runs come from the {@code ...ForAdmin} reads, which read the
 * model inference logs database and never the security database.
 */
public final class AdminAgentRunReadService {

	private static final Logger classLogger = LogManager.getLogger(AdminAgentRunReadService.class);
	/**
	 * How many agents or users whose name matches a filter search are matched
	 * against the rooms
	 */
	private static final long NAME_MATCH_LIMIT = 200L;

	private final AdminAccess access;

	private AdminAgentRunReadService(AdminAccess access) {
		this.access = access;
	}

	/**
	 * Returns the admin Agent Runs reads, for an admin only.
	 *
	 * @param user the user asking
	 * @return the reads
	 * @throws SecurityException when {@code user} is not an admin
	 */
	public static AdminAgentRunReadService getInstance(User user) {
		return new AdminAgentRunReadService(AdminAccess.of(user));
	}

	/**
	 * The agents of the rooms the page lists, most recently active first, for its
	 * agent filter, each with its name.
	 *
	 * @param search agents whose name or id contains this text, in any case, or
	 *               null for every agent
	 * @param limit  maximum number of agents to return
	 * @return each agent's id, name and when its rooms were last active
	 */
	public List<Map<String, Object>> getAgentOptions(String search, long limit) {
		String text = ValueUtils.trimToNull(search);
		List<String> named = text == null ? List.of() : findAgentIdsByName(text);
		List<Map<String, Object>> agents = AgentRunStore.getRoomAgentsForAdmin(access, text, named, limit);
		addNames(agents, "agentId", "agentName", AuditTargetType.PROJECT);
		return agents;
	}

	/**
	 * The users of the rooms the page lists, most recently active first, for its
	 * user filter, each with their name, email and login type.
	 *
	 * @param search users whose name, username, email or id contains this text, in
	 *               any case, or null for every user
	 * @param limit  maximum number of users to return
	 * @return each user's id, login type, name, email and when their rooms were
	 *         last active
	 */
	public List<Map<String, Object>> getUserOptions(String search, long limit) {
		String text = ValueUtils.trimToNull(search);
		List<Pair<String, String>> named = text == null ? List.of() : findUsersByName(text);
		List<Map<String, Object>> users = withoutUntypedDuplicates(
				AgentRunStore.getRoomUsersForAdmin(access, text, named, limit));
		addUsers(users);
		return users;
	}

	/**
	 * One page of rooms with agent runs, across every user, most recently active
	 * first, with how many rooms match in all.
	 *
	 * @param filter which rooms to list
	 * @param limit  maximum number of rooms to return
	 * @param offset number of matching rooms to skip
	 * @return {@code rooms}, each with its user's name, email and login type and
	 *         its agent's name, and {@code total}
	 */
	public Map<String, Object> getRooms(AgentRunStore.RoomFilter filter, long limit, long offset) {
		List<Map<String, Object>> rooms = AgentRunStore.getRoomsForAdmin(access, filter, limit, offset);
		addUsers(rooms);
		addNames(rooms, "agentId", "agentName", AuditTargetType.PROJECT);

		Map<String, Object> page = new LinkedHashMap<>();
		page.put("rooms", rooms);
		page.put("total", AgentRunStore.countRoomsForAdmin(access, filter));
		return page;
	}

	/**
	 * Every run a user has in a room, newest first, each with its model's name.
	 *
	 * @param roomId   room whose runs are returned
	 * @param userId   the user the room belongs to
	 * @param userType the login type of {@code userId}, or null to match the id
	 *                 under any type
	 * @return the user's runs in the room
	 */
	public List<Map<String, Object>> getRunsForRoom(String roomId, String userId, String userType) {
		List<Map<String, Object>> runs = AgentRunStore.getRunsForRoomForAdmin(access, roomId, userId, userType);
		addNames(runs, "modelId", "modelName", AuditTargetType.MODEL);
		return runs;
	}

	/**
	 * Any user's run, with its model's name.
	 *
	 * @param runId           agent run identifier
	 * @param includeMessages whether the run's room messages are included
	 * @return the run's details
	 */
	public Map<String, Object> getRun(String runId, boolean includeMessages) {
		Map<String, Object> run = AgentRunService.get().getRunForAdmin(access, runId, includeMessages);
		addNames(List.of(run), "modelId", "modelName", AuditTargetType.MODEL);
		return run;
	}

	/**
	 * The direct subagent runs of any user's run, each with its model's name.
	 *
	 * @param parentRunId parent run whose direct subagent runs are returned
	 * @return the subagent runs, newest first
	 */
	public List<Map<String, Object>> getSubagentRuns(String parentRunId) {
		List<Map<String, Object>> runs = AgentRunStore.getSubagentRunsForAdmin(access, parentRunId);
		addNames(runs, "modelId", "modelName", AuditTargetType.MODEL);
		return runs;
	}

	/**
	 * Adds the catalog's current name for an id each row holds, such as a room's
	 * agent or a run's model, with one read for every row.
	 *
	 * @param rows    the rows, each with the id under {@code idKey}
	 * @param idKey   where each row holds the id
	 * @param nameKey where each row gets the name, null when the catalog has none
	 * @param type    what kind of catalog entry the id names
	 */
	private static void addNames(List<Map<String, Object>> rows, String idKey, String nameKey, AuditTargetType type) {
		AuditTargetCatalog catalog = AuditTargetCatalog.lookup(
				rows.stream().map(row -> new String[] { type.name(), ValueUtils.trimToNull(row.get(idKey)) }).toList());
		for (Map<String, Object> row : rows) {
			row.put(nameKey, catalog.get(type.name(), ValueUtils.trimToNull(row.get(idKey))).displayName());
		}
	}

	/**
	 * Adds each room's user as the security database has them: their name, email
	 * and login type, with one read for every room. A room whose runs store a login
	 * type gets that login; one with none gets its id's login only when the id has
	 * just one, since otherwise its login is not known. A user who is no longer
	 * there keeps only their id and stored type.
	 *
	 * @param rooms the rooms, each with its {@code userId} and {@code userType}
	 */
	private static void addUsers(List<Map<String, Object>> rooms) {
		Set<String> userIds = new LinkedHashSet<>();
		for (Map<String, Object> room : rooms) {
			String userId = ValueUtils.trimToNull(room.get("userId"));
			if (userId != null) {
				userIds.add(userId);
			}
		}
		Map<String, Map<String, String[]>> users = readUsers(userIds);
		for (Map<String, Object> room : rooms) {
			String userType = ValueUtils.trimToNull(room.get("userType"));
			Map<String, String[]> logins = users.getOrDefault(ValueUtils.trimToNull(room.get("userId")), Map.of());
			String[] user = userType != null ? logins.get(userType)
					: logins.size() == 1 ? logins.values().iterator().next() : null;
			room.put("userName", user == null ? null : user[0]);
			room.put("userEmail", user == null ? null : user[1]);
			room.put("userType", user == null ? userType : user[2]);
		}
	}

	/**
	 * Finds the agents whose name or display name contains a search, so the agent
	 * filter can match them by name.
	 *
	 * @param search the text to find
	 * @return the matching agents' ids, at most {@link #NAME_MATCH_LIMIT}
	 */
	private static List<String> findAgentIdsByName(String search) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTID"));
		OrQueryFilter name = new OrQueryFilter();
		name.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTNAME", "?like", search));
		name.addFilter(SimpleQueryFilter.makeColToValFilter("PROJECT__PROJECTDISPLAYNAME", "?like", search));
		qs.addExplicitFilter(name);
		qs.setLimit(NAME_MATCH_LIMIT);
		try {
			return QueryExecutionUtility.queryList(SystemEngineRegistry.getSecurityDb(), qs,
					rs -> rs.getString("PROJECTID"));
		} catch (Exception e) {
			classLogger.warn("Unable to search the agents by name; the agent filter matches ids only", e);
			return List.of();
		}
	}

	/**
	 * Finds the users whose name, username or email contains a search, so the user
	 * filter can match them by name.
	 *
	 * @param search the text to find
	 * @return the matching users' ids and login types, at most
	 *         {@link #NAME_MATCH_LIMIT}
	 */
	private static List<Pair<String, String>> findUsersByName(String search) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));
		OrQueryFilter name = new OrQueryFilter();
		name.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__NAME", "?like", search));
		name.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__USERNAME", "?like", search));
		name.addFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__EMAIL", "?like", search));
		qs.addExplicitFilter(name);
		qs.setLimit(NAME_MATCH_LIMIT);
		try {
			return QueryExecutionUtility.queryList(SystemEngineRegistry.getSecurityDb(), qs,
					rs -> Pair.with(rs.getString("ID"), rs.getString("TYPE")));
		} catch (Exception e) {
			classLogger.warn("Unable to search the users by name; the user filter matches ids only", e);
			return List.of();
		}
	}

	/**
	 * Drops a user's row with no login type when the same id has one with a type,
	 * since a typed user's filter matches the untyped runs too.
	 *
	 * @param users the users, each with its {@code userId} and {@code userType}
	 * @return the users to offer, in the same order
	 */
	private static List<Map<String, Object>> withoutUntypedDuplicates(List<Map<String, Object>> users) {
		Set<String> typedIds = new HashSet<>();
		for (Map<String, Object> user : users) {
			if (ValueUtils.trimToNull(user.get("userType")) != null) {
				typedIds.add(ValueUtils.trimToNull(user.get("userId")));
			}
		}
		List<Map<String, Object>> offered = new ArrayList<>();
		for (Map<String, Object> user : users) {
			if (ValueUtils.trimToNull(user.get("userType")) != null
					|| !typedIds.contains(ValueUtils.trimToNull(user.get("userId")))) {
				offered.add(user);
			}
		}
		return offered;
	}

	/**
	 * Reads users by id, every login each id has.
	 *
	 * @param userIds the ids to read
	 * @return each login's name, email and login type, keyed by id, then by login
	 *         type in the order read
	 */
	private static Map<String, Map<String, String[]>> readUsers(Set<String> userIds) {
		Map<String, Map<String, String[]>> users = new HashMap<>();
		if (userIds.isEmpty()) {
			return users;
		}
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_USER__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__NAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__USERNAME"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__EMAIL"));
		qs.addSelector(new QueryColumnSelector("SMSS_USER__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_USER__ID", "==", List.copyOf(userIds)));
		try {
			for (String[] user : QueryExecutionUtility.queryList(securityDb, qs,
					rs -> new String[] { rs.getString("ID"), rs.getString("NAME"), rs.getString("USERNAME"),
							rs.getString("EMAIL"), rs.getString("TYPE") })) {
				String name = ValueUtils.trimToNull(user[1]) != null ? user[1] : ValueUtils.trimToNull(user[2]);
				users.computeIfAbsent(user[0], id -> new LinkedHashMap<>()).putIfAbsent(user[4],
						new String[] { name, user[3], user[4] });
			}
		} catch (Exception e) {
			classLogger.warn("Unable to read the users of the agent run rooms; the rooms show only user ids", e);
		}
		return users;
	}
}
