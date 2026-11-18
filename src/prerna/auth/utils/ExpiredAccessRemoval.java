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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditStatus;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.usertracking.UserAuditTrailUtils.AuditEvent;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * Removes access whose end date has passed, from every table that keeps one: a
 * person's, a team's, an app's and another engine's access to an engine, a
 * person's and a team's access to a project or an insight, and a person's place
 * in a custom team. A removal takes the grant's usage limit with it, and the
 * audit trail records it as ended, with SEMOSS as the actor.
 *
 * <p>
 * Every server sweeps the tables once a minute, starting at a random point in
 * its second minute so the servers of a cluster sweep at different times. A row
 * is deleted only while its end date is still past, so access given again in
 * the meantime stays, and a row two servers find at once is recorded only by
 * the server whose delete removed it.
 * </p>
 */
public final class ExpiredAccessRemoval {

	private static final Logger classLogger = LogManager.getLogger(ExpiredAccessRemoval.class);

	private static final String EXPIRED = "EXPIRED";
	private static final Duration TICK = Duration.ofMinutes(1);
	private static final AtomicBoolean SCHEDULED = new AtomicBoolean();
	private static final ScheduledExecutorService SCHEDULE = Executors.newSingleThreadScheduledExecutor(runnable -> {
		Thread thread = new Thread(runnable, "expired-access-removal");
		thread.setDaemon(true);
		return thread;
	});

	/**
	 * A table that keeps an end date, by the access it holds, with the columns that
	 * name one of its rows. The resource the access is on comes first.
	 */
	enum AccessTable {
		/** A person's access to an engine: engine id, user id */
		USER_ON_ENGINE("ENGINEPERMISSION", true, "ENGINEID", "USERID"),
		/** A team's access to an engine: engine id, team id, team type */
		TEAM_ON_ENGINE("GROUPENGINEPERMISSION", true, "ENGINEID", "ID", "TYPE"),
		/**
		 * An app's or another engine's access to an engine: engine id, the grantee's
		 * id, its catalog type
		 */
		ENGINE_ON_ENGINE("ENGINE2ENGINEPERMISSION", true, "ENGINEID", "GRANTEEENGINEID", "GRANTEEENGINETYPE"),
		/** A person's access to a project: project id, user id */
		USER_ON_PROJECT("PROJECTPERMISSION", true, "PROJECTID", "USERID"),
		/** A team's access to a project: project id, team id, team type */
		TEAM_ON_PROJECT("GROUPPROJECTPERMISSION", true, "PROJECTID", "ID", "TYPE"),
		/** A person's access to an insight: project id, insight id, user id */
		USER_ON_INSIGHT("USERINSIGHTPERMISSION", true, "PROJECTID", "INSIGHTID", "USERID"),
		/** A team's access to an insight: project id, insight id, team id, team type */
		TEAM_ON_INSIGHT("GROUPINSIGHTPERMISSION", true, "PROJECTID", "INSIGHTID", "ID", "TYPE"),
		/** A person's place in a custom team: team id, user id, login type */
		MEMBER_OF_TEAM("CUSTOMGROUPASSIGNMENT", false, "GROUPID", "USERID", "TYPE");

		private final String tableName;
		private final boolean hasLevel;
		private final List<String> keyColumns;

		AccessTable(String tableName, boolean hasLevel, String... keyColumns) {
			this.tableName = tableName;
			this.hasLevel = hasLevel;
			this.keyColumns = List.of(keyColumns);
		}

		String tableName() {
			return this.tableName;
		}
	}

	/**
	 * One row whose end date has passed.
	 *
	 * @param table   the table it is in
	 * @param key     the values of the table's key columns, in their order
	 * @param level   the access level it gave, or null for a team membership
	 * @param endDate when it ended
	 */
	record EndedRow(AccessTable table, List<String> key, Integer level, Timestamp endDate) {

		String describe() {
			return this.table.tableName + " " + this.key;
		}
	}

	private ExpiredAccessRemoval() {
	}

	/**
	 * Starts removing ended access once a minute on this server. The first sweep
	 * runs between one and two minutes from now, at a random point, so servers
	 * started together do not sweep together. Calling it again does nothing.
	 */
	public static void startSchedule() {
		if (SCHEDULED.compareAndSet(false, true)) {
			long first = TICK.toSeconds() + ThreadLocalRandom.current().nextLong(TICK.toSeconds() + 1);
			SCHEDULE.scheduleWithFixedDelay(ExpiredAccessRemoval::runScheduledSweep, first, TICK.toSeconds(),
					TimeUnit.SECONDS);
		}
	}

	private static void runScheduledSweep() {
		try {
			int removed = removeAllEndedAccess();
			if (removed > 0) {
				classLogger.info("Removed {} grants whose end date has passed", removed);
			}
		} catch (RuntimeException e) {
			classLogger.error("Failed to remove the access whose end date has passed", e);
		}
	}

	/**
	 * Removes every row whose end date has passed, in every table that keeps one. A
	 * table that cannot be read is logged and skipped, so the others are still
	 * swept.
	 *
	 * @return how many rows this call removed
	 */
	static int removeAllEndedAccess() {
		int removed = 0;
		for (AccessTable table : AccessTable.values()) {
			List<EndedRow> endedRows;
			try {
				endedRows = readEndedRows(table);
			} catch (Exception e) {
				classLogger.error("Failed to read the rows of {} whose end date has passed", table.tableName, e);
				continue;
			}
			for (EndedRow row : endedRows) {
				if (removeEndedRow(row)) {
					removed++;
				}
			}
		}
		return removed;
	}

	/**
	 * Reads the rows of one table whose end date has passed. A row with a key
	 * column left empty cannot be named for its removal and is left out.
	 *
	 * @param table the table
	 * @return its ended rows
	 * @throws Exception when the security database cannot be read
	 */
	static List<EndedRow> readEndedRows(AccessTable table) throws Exception {
		String columns = String.join(", ", table.keyColumns) + (table.hasLevel ? ", PERMISSION" : "") + ", ENDDATE";
		Timestamp now = Utility.getCurrentSqlTimestampUTC();
		List<EndedRow> rows = QueryExecutionUtility.queryList(SystemEngineRegistry.getSecurityDb(),
				"SELECT " + columns + " FROM " + table.tableName + " WHERE ENDDATE IS NOT NULL AND ENDDATE <= ?",
				ps -> ps.setTimestamp(1, now), result -> toEndedRow(table, result));
		List<EndedRow> named = new ArrayList<>();
		for (EndedRow row : rows) {
			if (!row.key().contains(null)) {
				named.add(row);
			}
		}
		return named;
	}

	private static EndedRow toEndedRow(AccessTable table, ResultSet result) throws Exception {
		List<String> key = new ArrayList<>();
		for (int column = 1; column <= table.keyColumns.size(); column++) {
			key.add(result.getString(column));
		}
		int next = table.keyColumns.size() + 1;
		Integer level = null;
		if (table.hasLevel) {
			int value = result.getInt(next++);
			level = result.wasNull() ? null : value;
		}
		return new EndedRow(table, key, level, result.getTimestamp(next));
	}

	/**
	 * Removes one ended row, with the usage limit on it, and records it in the
	 * audit trail. The row is deleted only while its end date is still past.
	 *
	 * @param row the ended row
	 * @return whether this call removed it; false when it was given again, already
	 *         removed, or could not be removed
	 */
	static boolean removeEndedRow(EndedRow row) {
		AccessTable table = row.table();
		Map<ModelUsageLimitKey, ModelUsageLimit> limits = readUsageLimits(table, row.key());
		StringBuilder where = new StringBuilder();
		for (String column : table.keyColumns) {
			where.append(column).append(" = ? AND ");
		}
		where.append("ENDDATE IS NOT NULL AND ENDDATE <= ?");
		Timestamp now = Utility.getCurrentSqlTimestampUTC();
		int deleted;
		try {
			deleted = QueryExecutionUtility.write(SystemEngineRegistry.getSecurityDb(), connection -> {
				int count = QueryExecutionUtility.executeUpdate(connection,
						"DELETE FROM " + table.tableName + " WHERE " + where, ps -> {
							int index = 1;
							for (String value : row.key()) {
								ps.setString(index++, value);
							}
							ps.setTimestamp(index, now);
						});
				if (count > 0) {
					deleteUsageLimit(connection, table, row.key());
				}
				return count;
			});
		} catch (Exception e) {
			classLogger.error("Failed to remove the ended access {}", row.describe(), e);
			return false;
		}
		if (deleted == 0) {
			return false;
		}
		SecurityModelUsageLimitUtils.markChanged();
		recordRemoval(table, row.key(), row.level(), row.endDate());
		if (!limits.isEmpty()) {
			ModelUsageLimitAudit.recordRemovedWithExpiredGrant(row.key().get(0), limits);
		}
		return true;
	}

	private static void deleteUsageLimit(Connection connection, AccessTable table, List<String> key) throws Exception {
		switch (table) {
		case USER_ON_ENGINE -> SecurityModelUsageLimitUtils.deleteDirectLimits(connection, key.get(0), key.get(1));
		case TEAM_ON_ENGINE -> SecurityModelUsageLimitUtils.setLimit(connection,
				ModelUsageLimitKey.team(key.get(0), key.get(1), key.get(2)), null, null);
		case ENGINE_ON_ENGINE -> SecurityModelUsageLimitUtils.setLimit(connection,
				ModelUsageLimitKey.grantee(key.get(0), key.get(1), key.get(2)), null, null);
		default -> {
		}
		}
	}

	/**
	 * Reads the usage limits on a grant to a model, before the grant is removed,
	 * for the audit trail: a person's Direct Limits, a team's Team Limit, or an
	 * app's App Limit or an engine's Engine Limit. A grant to a project or an
	 * insight has none, and a read that fails gives none, so it never stops the
	 * removal.
	 *
	 * @param table the table the grant is in
	 * @param key   the grant's key, the engine id first
	 * @return the limits on the grant, by key
	 */
	private static Map<ModelUsageLimitKey, ModelUsageLimit> readUsageLimits(AccessTable table, List<String> key) {
		ModelUsageLimitKey.Type type = switch (table) {
		case USER_ON_ENGINE -> ModelUsageLimitKey.Type.USER;
		case TEAM_ON_ENGINE -> ModelUsageLimitKey.Type.TEAM;
		case ENGINE_ON_ENGINE -> ModelUsageLimitKey.Type.ENGINE;
		default -> null;
		};
		if (type == null) {
			return Map.of();
		}
		String engineId = key.get(0);
		String subjectId = key.get(1);
		String subjectType = table == AccessTable.TEAM_ON_ENGINE || table == AccessTable.ENGINE_ON_ENGINE ? key.get(2)
				: null;
		Map<ModelUsageLimitKey, ModelUsageLimit> limits = new LinkedHashMap<>();
		try {
			SecurityModelUsageLimitUtils.getEngineLimits(engineId).forEach((limitKey, limit) -> {
				if (limitKey.type() == type && engineId.equals(limitKey.engineId())
						&& subjectId.equals(limitKey.subjectId())
						&& (subjectType == null || subjectType.equals(limitKey.subjectType()))) {
					limits.put(limitKey, limit);
				}
			});
		} catch (RuntimeException e) {
			classLogger.warn("Unable to read the usage limits on the ended access {} {}", table.tableName, key, e);
			return Map.of();
		}
		return limits;
	}

	/**
	 * Records access SEMOSS removed because its end date passed, with SEMOSS as the
	 * actor: Access Ended on the engine, project or insight, naming the person,
	 * team, app or engine that had it and the level it gave, or a team member
	 * removed for a place in a custom team. A failure to record is logged, since
	 * the access is already removed.
	 *
	 * @param table   the table the access was in
	 * @param key     the values of the table's key columns, in their order
	 * @param level   the access level it gave, or null when it is not known or is a
	 *                team membership
	 * @param endDate when it ended, or null when it is not known
	 */
	private static void recordRemoval(AccessTable table, List<String> key, Integer level, Timestamp endDate) {
		try {
			AuditEvent event = new AuditEvent()
					.eventType(table == AccessTable.MEMBER_OF_TEAM ? AuditEventType.GROUP_MEMBER_REMOVE
							: AuditEventType.PERMISSION_DELETE)
					.action(UserAuditTrailUtils.PERMISSION_EXPIRED_ACTION).status(AuditStatus.SUCCESS).systemActor();
			Map<String, Object> details = new LinkedHashMap<>();
			switch (table) {
			case USER_ON_ENGINE, TEAM_ON_ENGINE, ENGINE_ON_ENGINE ->
				event.target(SecurityEngineUtils.getAuditTargetType(key.get(0)), key.get(0), null).context(null,
						key.get(0), null, null);
			case USER_ON_PROJECT, TEAM_ON_PROJECT ->
				event.target(AuditTargetType.PROJECT, key.get(0), null).context(key.get(0), null, null, null);
			case USER_ON_INSIGHT, TEAM_ON_INSIGHT ->
				event.target(AuditTargetType.INSIGHT, key.get(1), null).context(key.get(0), null, key.get(1), null);
			case MEMBER_OF_TEAM -> event.target(AuditTargetType.GROUP, key.get(0), key.get(0));
			}
			switch (table) {
			case USER_ON_ENGINE, USER_ON_PROJECT -> {
				event.subject(key.get(1), null, null);
				details.put("granteeId", key.get(1));
			}
			case USER_ON_INSIGHT -> {
				event.subject(key.get(2), null, null);
				details.put("granteeId", key.get(2));
			}
			case TEAM_ON_ENGINE, TEAM_ON_PROJECT -> {
				details.put("groupId", key.get(1));
				details.put("groupType", key.get(2));
			}
			case TEAM_ON_INSIGHT -> {
				details.put("groupId", key.get(2));
				details.put("groupType", key.get(3));
			}
			case ENGINE_ON_ENGINE -> {
				details.put("granteeEngineId", key.get(1));
				details.put("granteeEngineType", key.get(2));
			}
			case MEMBER_OF_TEAM -> {
				event.subject(key.get(1), key.get(2), null);
				details.put("groupType", SecurityGroupManagerUtils.CUSTOM_GROUP_TYPE);
			}
			}
			details.put("reason", EXPIRED);
			if (endDate != null) {
				details.put("endDate", endDate.toLocalDateTime().toString());
			}
			if (level != null) {
				event.oldValue(Map.of("permission", AccessPermissionEnum.getPermissionValueById(level)));
			}
			UserAuditTrailUtils.recordEvent(event.details(details));
		} catch (RuntimeException e) {
			classLogger.warn("Unable to audit the ended access {} {}", table.tableName, key, e);
		}
	}
}
