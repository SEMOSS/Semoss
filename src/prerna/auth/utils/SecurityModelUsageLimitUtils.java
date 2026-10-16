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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;

import com.google.gson.JsonParseException;

import prerna.auth.User;
import prerna.auth.access.ModelUsageLimit;
import prerna.auth.access.ModelUsageLimitKey;
import prerna.auth.access.ModelUsageLimitKey.Type;
import prerna.auth.access.ModelUsageLimitRule;
import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;
import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.ValueUtils;
import prerna.util.gson.GsonUtility;
import prerna.util.sql.AbstractSqlQueryUtil;

/**
 * Reads and writes model usage limits, kept in {@code MODELUSAGELIMIT} (one row
 * for each source that has a limit defined) and {@code MODELUSAGELIMITRULE}
 * (its rules).
 *
 * <p>
 * The limits on grants and on a model's public use and total are written by
 * {@code PermissionBatchUtils}, in the batch's transaction, through the
 * package-private methods here. The platform default and team totals are
 * written by admins, through the public methods that take
 * {@link SecurityAdminUtils}. A person's all-models default is written by
 * admins too, with the person: {@link SecurityAdminUtils#editUser} and
 * {@link SecurityUpdateUtils#registerUser}, and it moves with an admin added
 * person when they first sign in ({@code SecurityNativeUserUtils.addNativeUser}
 * and the login provider sign-in).
 * </p>
 *
 * <p>
 * Every write ends with {@link #markChanged()}, called once its transaction has
 * committed, so the limits cached on this server are read again.
 * </p>
 *
 * <ul>
 * <li><b>Reading.</b> {@link #getEngineLimits} reads every limit on a model in
 * one query, and {@link #getLimits} reads several keys at once, with one more
 * query for the limits across all models. A key missing from the result has no
 * limit defined; a read that fails throws, so a missing limit is never
 * assumed.</li>
 * <li><b>One row per key.</b> Nothing in the database enforces it, so a read
 * takes the row changed last should two exist, and the next write for the key
 * replaces both.</li>
 * <li><b>A Direct Limit and its login type.</b> {@code ENGINEPERMISSION} keeps
 * only the user id, but a Direct Limit keeps the login type too; one whose type
 * is not known is kept with none and applies to the user id on any login. Read
 * a Direct Limit with {@link #findDirectLimit}, which falls back to it.</li>
 * <li><b>The version is per server.</b> On a cluster, the other servers read a
 * change when the paths they cached expire, within half a minute
 * ({@code ModelUsageGuard}).</li>
 * </ul>
 */
public final class SecurityModelUsageLimitUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityModelUsageLimitUtils.class);

	private static final String SELECT_LIMITS = "SELECT L.ID, L.ENGINEID, L.LIMITTYPE, L.SUBJECTID, "
			+ "L.SUBJECTTYPE, L.UNLIMITED, L.DATEMODIFIED, R.METRIC, R.AMOUNT, R.LIMITWINDOW, R.WINDOWLENGTH, "
			+ "R.COUNTS FROM MODELUSAGELIMIT L LEFT OUTER JOIN MODELUSAGELIMITRULE R ON L.ID = R.LIMITID";
	private static final String INSERT_LIMIT = "INSERT INTO MODELUSAGELIMIT (ID, ENGINEID, LIMITTYPE, SUBJECTID, "
			+ "SUBJECTTYPE, UNLIMITED, DATEADDED, ADDEDBYID, ADDEDBYTYPE, DATEMODIFIED, MODIFIEDBYID, MODIFIEDBYTYPE) "
			+ "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)";
	private static final String INSERT_RULE = "INSERT INTO MODELUSAGELIMITRULE (LIMITID, METRIC, AMOUNT, "
			+ "LIMITWINDOW, WINDOWLENGTH, COUNTS) VALUES (?,?,?,?,?,?)";
	private static final String SELECT_LOGIN_TYPES = "SELECT DISTINCT TYPE FROM SMSS_USER WHERE ID = ? "
			+ "AND TYPE IS NOT NULL AND TYPE <> ''";
	/**
	 * Gives each grant's limit kept with no login type the login type of its user
	 * id, when the id has exactly one login in SMSS_USER
	 */
	private static final String FILL_DIRECT_LOGIN_TYPES = "UPDATE MODELUSAGELIMIT SET SUBJECTTYPE = "
			+ "(SELECT MIN(U.TYPE) FROM SMSS_USER U WHERE U.ID = MODELUSAGELIMIT.SUBJECTID "
			+ "AND U.TYPE IS NOT NULL AND U.TYPE <> '') "
			+ "WHERE LIMITTYPE = ? AND ENGINEID IS NOT NULL AND SUBJECTTYPE IS NULL "
			+ "AND (SELECT COUNT(DISTINCT U.TYPE) FROM SMSS_USER U WHERE U.ID = MODELUSAGELIMIT.SUBJECTID "
			+ "AND U.TYPE IS NOT NULL AND U.TYPE <> '') = 1";

	private static final Comparator<ModelUsageLimitRule> RULE_ORDER = Comparator
			.comparing((ModelUsageLimitRule rule) -> rule.getCounts() == null ? 0 : rule.getCounts().ordinal())
			.reversed().thenComparing(rule -> rule.getMetric() == null ? 0 : rule.getMetric().ordinal())
			.thenComparing(rule -> rule.getWindow() == null ? 0 : rule.getWindow().ordinal())
			.thenComparing(ModelUsageLimitRule::getWindowLength);

	/**
	 * Bumped after every write has committed, so cached limits know they may be out
	 * of date
	 */
	private static final AtomicLong VERSION = new AtomicLong();

	private SecurityModelUsageLimitUtils() {

	}

	/**
	 * The version of the limits on this server, which a cache such as
	 * {@code ModelUsageGuard}'s compares to tell whether what it read is out of
	 * date.
	 *
	 * @return a number that changes whenever a limit, or a grant or a team
	 *         membership that decides which limit applies, is written on this
	 *         server
	 */
	public static long version() {
		return VERSION.get();
	}

	/**
	 * Marks the limits as changed. Every write that changes a limit, or a grant or
	 * a team membership that decides which limit applies, calls it once its
	 * transaction has committed: a cache that read the limits during the
	 * transaction then reads them again, rather than keep what it read under the
	 * new version. The writing methods here, such as {@link #setLimit}, leave it to
	 * their callers, since they run inside the caller's transaction.
	 */
	static void markChanged() {
		VERSION.incrementAndGet();
	}

	/*
	 * Reading
	 */

	/**
	 * Reads the limit defined for one key.
	 *
	 * @param key which limit; for a person's grant, see {@link #findDirectLimit}
	 *            for how the login type is matched
	 * @return its limit, or null for no limit defined
	 */
	public static ModelUsageLimit getLimit(ModelUsageLimitKey key) {
		return getLimits(List.of(key)).get(key);
	}

	/**
	 * Reads every limit defined on a model, in one query.
	 *
	 * @param engineId the model
	 * @return every limit defined on the model: people's and teams' grants, public
	 *         use and the model's total
	 */
	public static Map<ModelUsageLimitKey, ModelUsageLimit> getEngineLimits(String engineId) {
		ValueUtils.requireNonBlank(engineId, "Must define the model");
		return readLimits(SELECT_LIMITS + " WHERE L.ENGINEID = ?", List.of(engineId));
	}

	/**
	 * The limit on a person's grant on a model, from the model's limits. A grant's
	 * limit is kept with the person's login type; one whose login type is not known
	 * is kept with none, and applies to the user id on any login.
	 *
	 * @param limits   the model's limits, from {@link #getEngineLimits}
	 * @param engineId the model
	 * @param userId   the user id
	 * @param type     the login type, or null when it is not known
	 * @return the limit kept with the login type, else the one kept with none,
	 *         else, with no login type given, the only one the user id has; or null
	 *         when none is defined
	 */
	public static ModelUsageLimit findDirectLimit(Map<ModelUsageLimitKey, ModelUsageLimit> limits, String engineId,
			String userId, String type) {
		if (limits == null || engineId == null || userId == null) {
			return null;
		}
		ModelUsageLimitKey untyped = ModelUsageLimitKey.direct(engineId, userId, null);
		if (type != null) {
			ModelUsageLimit typed = limits.get(ModelUsageLimitKey.direct(engineId, userId, type));
			return typed != null ? typed : limits.get(untyped);
		}
		if (limits.containsKey(untyped)) {
			return limits.get(untyped);
		}
		ModelUsageLimit only = null;
		for (Map.Entry<ModelUsageLimitKey, ModelUsageLimit> entry : limits.entrySet()) {
			ModelUsageLimitKey key = entry.getKey();
			if (key.isDirect() && engineId.equals(key.engineId()) && userId.equals(key.subjectId())) {
				if (only != null) {
					// the user id has a limit on more than one login, so which one applies is not
					// known
					return null;
				}
				only = entry.getValue();
			}
		}
		return only;
	}

	/**
	 * Reads the login type of a user id, for a Direct Limit written with no login
	 * type.
	 *
	 * @param connection the security database connection, in the caller's
	 *                   transaction
	 * @param userId     the user id
	 * @return its login type when it has exactly one login in SMSS_USER with a
	 *         login type, or null
	 */
	static String loginTypeOf(Connection connection, String userId) throws Exception {
		if (userId == null || userId.isBlank()) {
			return null;
		}
		List<String> types = QueryExecutionUtility.queryList(connection, SELECT_LOGIN_TYPES,
				ps -> ps.setString(1, userId), rs -> rs.getString(1));
		return types.size() == 1 ? types.get(0) : null;
	}

	/**
	 * Gives the grants' limits kept with no login type the login type of their user
	 * id, where it has exactly one login. The rest stay with none and apply to the
	 * user id on any login. Run at start up.
	 *
	 * @param securityDb the security database
	 */
	static void fillDirectLimitLoginTypes(IRDBMSEngine securityDb) {
		try {
			int filled = QueryExecutionUtility.write(securityDb, connection -> QueryExecutionUtility
					.executeUpdate(connection, FILL_DIRECT_LOGIN_TYPES, ps -> ps.setString(1, Type.USER.name())));
			if (filled > 0) {
				classLogger.info("Gave {} usage limits on people's grants their login type", filled);
				markChanged();
			}
		} catch (Exception e) {
			classLogger.warn("Unable to give the usage limits on people's grants their login type", e);
		}
	}

	/**
	 * Reads the limits of several keys at once: each model's limits in one query,
	 * and one more query for the limits across all models.
	 *
	 * @param keys which limits
	 * @return the limits defined for any of them; keys with no limit defined are
	 *         left out
	 */
	public static Map<ModelUsageLimitKey, ModelUsageLimit> getLimits(Collection<ModelUsageLimitKey> keys) {
		Map<ModelUsageLimitKey, ModelUsageLimit> found = new LinkedHashMap<>();
		if (keys == null || keys.isEmpty()) {
			return found;
		}
		Set<String> engineIds = new LinkedHashSet<>();
		Set<String> allModelsIds = new LinkedHashSet<>();
		boolean anyAllModels = false;
		for (ModelUsageLimitKey key : keys) {
			if (key.engineId() != null) {
				engineIds.add(key.engineId());
			} else {
				anyAllModels = true;
				if (key.subjectId() != null) {
					allModelsIds.add(key.subjectId());
				}
			}
		}
		Map<ModelUsageLimitKey, ModelUsageLimit> read = new LinkedHashMap<>();
		for (String engineId : engineIds) {
			read.putAll(getEngineLimits(engineId));
		}
		if (anyAllModels) {
			StringBuilder sql = new StringBuilder(SELECT_LIMITS)
					.append(" WHERE L.ENGINEID IS NULL AND (L.LIMITTYPE = ?");
			List<Object> values = new ArrayList<>();
			values.add(Type.PLATFORM.name());
			if (!allModelsIds.isEmpty()) {
				sql.append(" OR L.SUBJECTID IN (")
						.append(String.join(",", Collections.nCopies(allModelsIds.size(), "?"))).append(")");
				values.addAll(allModelsIds);
			}
			sql.append(")");
			read.putAll(readLimits(sql.toString(), values));
		}
		for (ModelUsageLimitKey key : keys) {
			ModelUsageLimit limit = key.isDirect()
					? findDirectLimit(read, key.engineId(), key.subjectId(), key.subjectType())
					: read.get(key);
			if (limit != null) {
				found.put(key, limit);
			}
		}
		return found;
	}

	/**
	 * Reads the General Platform Limit.
	 *
	 * @return the platform default, for everyone with no other limit, or null
	 */
	public static ModelUsageLimit getPlatformDefault() {
		return getLimit(ModelUsageLimitKey.platform());
	}

	/**
	 * Reads a team's Team Total, the ceiling on its use across all models.
	 *
	 * @param teamId   the team id
	 * @param teamType the team's type
	 * @return the team's total across all models, or null
	 */
	public static ModelUsageLimit getTeamTotal(String teamId, String teamType) {
		return getLimit(ModelUsageLimitKey.teamTotal(teamId, teamType));
	}

	private static Map<ModelUsageLimitKey, ModelUsageLimit> readLimits(String sql, List<?> values) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		Map<ModelUsageLimitKey, Boolean> unlimited = new LinkedHashMap<>();
		Map<ModelUsageLimitKey, List<ModelUsageLimitRule>> rules = new LinkedHashMap<>();
		// the row each key's limit is read from, with when it was last changed
		Map<ModelUsageLimitKey, Pair<String, Timestamp>> rowOf = new LinkedHashMap<>();
		try {
			List<Object[]> rows = QueryExecutionUtility.queryList(securityDb, sql, ps -> {
				for (int i = 0; i < values.size(); i++) {
					ps.setString(i + 1, String.valueOf(values.get(i)));
				}
			}, rs -> {
				long amount = rs.getLong("AMOUNT");
				boolean amountNull = rs.wasNull();
				int length = rs.getInt("WINDOWLENGTH");
				boolean lengthNull = rs.wasNull();
				return new Object[] { rs.getString("ENGINEID"), rs.getString("LIMITTYPE"), rs.getString("SUBJECTID"),
						rs.getString("SUBJECTTYPE"), rs.getBoolean("UNLIMITED"), rs.getString("METRIC"),
						amountNull ? null : amount, rs.getString("LIMITWINDOW"), lengthNull ? null : length,
						rs.getString("COUNTS"), rs.getString("ID"), rs.getTimestamp("DATEMODIFIED") };
			});
			for (Object[] row : rows) {
				ModelUsageLimitKey key = new ModelUsageLimitKey(Type.valueOf((String) row[1]), (String) row[0],
						(String) row[2], (String) row[3]);
				Pair<String, Timestamp> limitRow = Pair.with((String) row[10], (Timestamp) row[11]);
				Pair<String, Timestamp> kept = rowOf.get(key);
				if (kept != null && !kept.getValue0().equals(limitRow.getValue0())) {
					// a key has one row; should two have been written at the same time, the
					// one changed last is read, and the next write replaces both
					if (!changedLater(limitRow, kept)) {
						continue;
					}
					rules.remove(key);
				}
				rowOf.put(key, limitRow);
				unlimited.put(key, Boolean.TRUE.equals(row[4]));
				List<ModelUsageLimitRule> keyRules = rules.computeIfAbsent(key, k -> new ArrayList<>());
				if (row[5] != null) {
					keyRules.add(
							ruleOf((String) row[5], (Long) row[6], (String) row[7], (Integer) row[8], (String) row[9]));
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to read model usage limits.", e);
			throw new IllegalStateException("Unable to read model usage limits", e);
		}
		Map<ModelUsageLimitKey, ModelUsageLimit> limits = new LinkedHashMap<>();
		for (Map.Entry<ModelUsageLimitKey, Boolean> entry : unlimited.entrySet()) {
			List<ModelUsageLimitRule> keyRules = rules.get(entry.getKey());
			keyRules.sort(RULE_ORDER);
			limits.put(entry.getKey(), new ModelUsageLimit(entry.getValue(), keyRules));
		}
		return limits;
	}

	/**
	 * Compares when two rows of the same key were last changed, to pick the row a
	 * read takes.
	 *
	 * @param row   a limit row's id and when it was last changed
	 * @param other another row's
	 * @return whether the row was changed after the other one, by id when neither
	 *         or both say when
	 */
	private static boolean changedLater(Pair<String, Timestamp> row, Pair<String, Timestamp> other) {
		Timestamp changed = row.getValue1();
		Timestamp otherChanged = other.getValue1();
		if (changed != null && otherChanged != null && !changed.equals(otherChanged)) {
			return changed.after(otherChanged);
		}
		if (changed == null && otherChanged != null) {
			return false;
		}
		if (changed != null && otherChanged == null) {
			return true;
		}
		return row.getValue0().compareTo(other.getValue0()) > 0;
	}

	private static ModelUsageLimitRule ruleOf(String metric, Long amount, String window, Integer windowLength,
			String counts) {
		int length = windowLength == null ? 1 : windowLength;
		return new ModelUsageLimitRule(Metric.valueOf(metric), amount, Window.valueOf(window), length,
				Counts.valueOf(counts));
	}

	/*
	 * Writing, in the caller's transaction
	 */

	/**
	 * Replaces the limit defined for a key, rules and all: every row the key has is
	 * deleted, its rules first, and one new row is written, which keeps when the
	 * limit was first added and by whom. So a key has one row, even when two writes
	 * made more than one. Validate the limit first with {@link #requireValid}. The
	 * caller calls {@link #markChanged()} once its transaction has committed.
	 * <p>
	 * It is package-private, and runs in the caller's transaction, because every
	 * limit is written by code that has already checked who may change it: a
	 * permission batch, where a grant and its limit then commit or fail together,
	 * or the admin methods here.
	 * </p>
	 *
	 * @param connection the security database connection, in the caller's
	 *                   transaction
	 * @param limitKey   which limit. A person's grant on a model is kept with their
	 *                   login type, read from SMSS_USER when it is not given, and
	 *                   writing it replaces the row kept with no login type.
	 * @param limit      the limit, or null to remove it, leaving no limit defined
	 * @param by         who is changing it, as user id and login type
	 */
	static void setLimit(Connection connection, ModelUsageLimitKey limitKey, ModelUsageLimit limit,
			Pair<String, String> by) throws Exception {
		AbstractSqlQueryUtil queryUtil = SystemEngineRegistry.getSecurityDb().getQueryUtil();
		// a grant's limit keeps the person's login type, from SMSS_USER when the
		// change does not say
		ModelUsageLimitKey key = limitKey.isDirect() && limitKey.subjectType() == null ? ModelUsageLimitKey.direct(
				limitKey.engineId(), limitKey.subjectId(), loginTypeOf(connection, limitKey.subjectId())) : limitKey;
		List<String> values = new ArrayList<>();
		String where = keyMatches(key, values);
		// who first added the limit, from the earliest row the key has
		Object[] added = null;
		if (limit != null) {
			List<Object[]> rows = QueryExecutionUtility.queryList(connection,
					"SELECT DATEADDED, ADDEDBYID, ADDEDBYTYPE FROM MODELUSAGELIMIT WHERE " + where,
					ps -> bindAll(ps, values),
					rs -> new Object[] { rs.getTimestamp(1), rs.getString(2), rs.getString(3) });
			for (Object[] row : rows) {
				Timestamp rowAdded = (Timestamp) row[0];
				if (added == null
						|| (rowAdded != null && (added[0] == null || rowAdded.before((Timestamp) added[0])))) {
					added = row;
				}
			}
		}
		deleteWhere(connection, where, values);
		if (limit == null) {
			return;
		}
		Timestamp now = Utility.getCurrentSqlTimestampUTC();
		String byId = by == null ? null : by.getValue0();
		String byType = by == null ? null : by.getValue1();
		Timestamp addedOn = added == null || added[0] == null ? now : (Timestamp) added[0];
		String addedById = added == null ? byId : (String) added[1];
		String addedByType = added == null ? byType : (String) added[2];
		String id = UUID.randomUUID().toString();
		QueryExecutionUtility.executeUpdate(connection, INSERT_LIMIT, ps -> {
			int index = 1;
			ps.setString(index++, id);
			queryUtil.setNullableString(ps, index++, key.engineId());
			ps.setString(index++, key.type().name());
			queryUtil.setNullableString(ps, index++, key.subjectId());
			queryUtil.setNullableString(ps, index++, key.subjectType());
			ps.setBoolean(index++, limit.isUnlimited());
			ps.setTimestamp(index++, addedOn);
			queryUtil.setNullableString(ps, index++, addedById);
			queryUtil.setNullableString(ps, index++, addedByType);
			ps.setTimestamp(index++, now);
			queryUtil.setNullableString(ps, index++, byId);
			queryUtil.setNullableString(ps, index++, byType);
		});
		if (!limit.getRules().isEmpty()) {
			QueryExecutionUtility.executeBatch(connection, INSERT_RULE, limit.getRules(), (ps, rule) -> {
				int index = 1;
				ps.setString(index++, id);
				ps.setString(index++, rule.getMetric().name());
				if (rule.getAmount() == null) {
					ps.setNull(index++, Types.BIGINT);
				} else {
					ps.setLong(index++, rule.getAmount());
				}
				ps.setString(index++, rule.getWindow().name());
				if (rule.getWindowLength() == 1) {
					ps.setNull(index++, Types.INTEGER);
				} else {
					ps.setInt(index++, rule.getWindowLength());
				}
				ps.setString(index++, rule.getCounts().name());
			});
		}
	}

	/**
	 * Removes every limit on a model, for when the model is deleted.
	 */
	static void deleteEngineLimits(Connection connection, String engineId) throws Exception {
		deleteWhere(connection, "ENGINEID = ?", List.of(engineId));
	}

	/**
	 * Removes a team's limits on every model and its total, for when the team is
	 * deleted.
	 */
	static void deleteTeamLimits(Connection connection, String teamId, String teamType) throws Exception {
		deleteWhere(connection, "LIMITTYPE = ? AND SUBJECTID = ? AND SUBJECTTYPE = ?",
				List.of(Type.TEAM.name(), teamId, teamType));
	}

	/**
	 * Removes a person's all-models default, for when the person is deleted.
	 */
	static void deleteAllModelsDefault(Connection connection, String userId, String type) throws Exception {
		deleteWhere(connection, "ENGINEID IS NULL AND LIMITTYPE = ? AND SUBJECTID = ? AND SUBJECTTYPE = ?",
				List.of(Type.USER.name(), userId, type));
	}

	/**
	 * Removes the limits on a person's grant on a model, on every login type, for
	 * when the grant is removed: ENGINEPERMISSION keeps only the user id, so the
	 * grant was the same for each of them.
	 */
	static void deleteDirectLimits(Connection connection, String engineId, String userId) throws Exception {
		deleteWhere(connection, "ENGINEID = ? AND LIMITTYPE = ? AND SUBJECTID = ?",
				List.of(engineId, Type.USER.name(), userId));
	}

	/**
	 * Removes the limits on a person's grants that no longer exist, for when their
	 * grants are removed or replaced, such as by the login sync.
	 */
	static void deleteDirectLimitsWithoutGrant(Connection connection, String userId) throws Exception {
		deleteWhere(connection,
				"ENGINEID IS NOT NULL AND LIMITTYPE = ? AND SUBJECTID = ? AND NOT EXISTS (SELECT 1 FROM "
						+ "ENGINEPERMISSION P WHERE P.USERID = MODELUSAGELIMIT.SUBJECTID "
						+ "AND P.ENGINEID = MODELUSAGELIMIT.ENGINEID)",
				List.of(Type.USER.name(), userId));
	}

	/**
	 * Deletes the limit rows that match, their rules first. The caller calls
	 * {@link #markChanged()} once its transaction has committed.
	 */
	private static void deleteWhere(Connection connection, String where, List<String> values) throws Exception {
		QueryExecutionUtility.executeUpdate(connection,
				"DELETE FROM MODELUSAGELIMITRULE WHERE LIMITID IN (SELECT ID FROM MODELUSAGELIMIT WHERE " + where + ")",
				ps -> bindAll(ps, values));
		QueryExecutionUtility.executeUpdate(connection, "DELETE FROM MODELUSAGELIMIT WHERE " + where,
				ps -> bindAll(ps, values));
	}

	private static void bindAll(PreparedStatement ps, List<String> values) throws SQLException {
		for (int i = 0; i < values.size(); i++) {
			ps.setString(i + 1, values.get(i));
		}
	}

	/**
	 * Builds the SQL condition that matches a key's rows, adding the values it
	 * binds.
	 *
	 * @param key    which limit
	 * @param values the values to bind, which this adds to
	 * @return the condition that matches every row of the key; for a grant's limit
	 *         with a login type, the row kept with no login type too, so writing
	 *         the limit replaces it
	 */
	private static String keyMatches(ModelUsageLimitKey key, List<String> values) {
		values.add(key.type().name());
		String subject = "LIMITTYPE = ? AND " + matches("ENGINEID", key.engineId(), values) + " AND "
				+ matches("SUBJECTID", key.subjectId(), values) + " AND ";
		if (key.isDirect() && key.subjectType() != null) {
			values.add(key.subjectType());
			return subject + "(SUBJECTTYPE = ? OR SUBJECTTYPE IS NULL)";
		}
		return subject + matches("SUBJECTTYPE", key.subjectType(), values);
	}

	private static String matches(String column, String value, List<String> values) {
		if (value == null) {
			return column + " IS NULL";
		}
		values.add(value);
		return column + " = ?";
	}

	/*
	 * Admin entry points
	 */

	/**
	 * Sets the platform default, the limit for everyone with no other limit.
	 *
	 * @param adminUtils the admin utilities, which prove the user is an admin
	 * @param user       the admin
	 * @param limit      the limit, or null to remove it
	 */
	public static void setPlatformDefault(SecurityAdminUtils adminUtils, User user, ModelUsageLimit limit)
			throws Exception {
		saveAsAdmin(adminUtils, user, ModelUsageLimitKey.platform(), limit);
	}

	/**
	 * Sets a team's total across all models.
	 *
	 * @param adminUtils the admin utilities, which prove the user is an admin
	 * @param user       the admin
	 * @param teamId     the team id
	 * @param teamType   the team's type
	 * @param limit      the limit, or null to remove it
	 * @throws IllegalArgumentException when the team does not exist
	 */
	public static void setTeamTotal(SecurityAdminUtils adminUtils, User user, String teamId, String teamType,
			ModelUsageLimit limit) throws Exception {
		requireAdmin(adminUtils);
		ValueUtils.requireNonBlank(teamId, "Must define the team");
		ValueUtils.requireNonBlank(teamType, "Must define the team's type");
		if (!AdminSecurityGroupUtils.getUncheckedInstance().groupExists(teamId, teamType)) {
			throw new IllegalArgumentException("Team " + teamId + " does not exist");
		}
		saveAsAdmin(adminUtils, user, ModelUsageLimitKey.teamTotal(teamId, teamType), limit);
	}

	private static void saveAsAdmin(SecurityAdminUtils adminUtils, User user, ModelUsageLimitKey key,
			ModelUsageLimit limit) throws Exception {
		requireAdmin(adminUtils);
		ModelUsageLimit valid = requireValid(limit, key, true);
		Pair<String, String> by = User.getPrimaryUserIdAndTypePair(user);
		QueryExecutionUtility.write(SystemEngineRegistry.getSecurityDb(), connection -> {
			setLimit(connection, key, valid, by);
			return null;
		});
		markChanged();
	}

	private static void requireAdmin(SecurityAdminUtils adminUtils) {
		if (adminUtils == null) {
			throw new IllegalArgumentException("Admin mode needs the admin utilities");
		}
	}

	/*
	 * Validation
	 */

	/**
	 * Reads a limit sent as JSON, such as
	 * {@code {"unlimited": false, "rules": [{"metric": "TOKENS", "amount": 100000,
	 * "window": "MINUTE", "windowLength": 10, "counts": "EACH_PERSON"}]}}, where
	 * {@code windowLength} is how many minutes or hours a minute or hour window is,
	 * left out for 1.
	 *
	 * @param json the limit, or blank or {@code null} for no limit defined
	 * @return the limit, or null
	 * @throws IllegalArgumentException when the text is not a limit
	 */
	public static ModelUsageLimit readJson(String json) {
		if (json == null || json.isBlank() || "null".equals(json.trim())) {
			return null;
		}
		try {
			return GsonUtility.getDefaultGson().fromJson(json, ModelUsageLimit.class);
		} catch (JsonParseException e) {
			throw new IllegalArgumentException("The usage limit is not valid: " + e.getMessage());
		}
	}

	/**
	 * Drops the rules with an amount of 0, since 0 means no limit defined, and
	 * turns a limit left with no rules into no limit defined.
	 *
	 * @param limit the limit, or null
	 * @return the limit, or null
	 */
	public static ModelUsageLimit normalize(ModelUsageLimit limit) {
		if (limit == null) {
			return null;
		}
		if (limit.isUnlimited()) {
			return limit;
		}
		List<ModelUsageLimitRule> rules = new ArrayList<>();
		for (ModelUsageLimitRule rule : limit.getRules()) {
			if (rule == null) {
				continue;
			}
			if (rule.getAmount() == null || rule.getAmount() != 0L) {
				rules.add(rule);
			}
		}
		return rules.isEmpty() ? null : ModelUsageLimit.of(rules);
	}

	/**
	 * Normalizes a limit and checks it can be defined for a key.
	 *
	 * @param limit               the limit, or null
	 * @param key                 where it would be defined
	 * @param costLimitsAvailable whether the model has known prices; ignored for
	 *                            limits across all models, whose cost rules count
	 *                            only the models with known prices
	 * @return the normalized limit, or null
	 * @throws IllegalArgumentException with the reason, when it cannot
	 */
	public static ModelUsageLimit requireValid(ModelUsageLimit limit, ModelUsageLimitKey key,
			boolean costLimitsAvailable) {
		ModelUsageLimit normalized = normalize(limit);
		String problem = findProblem(normalized, key, costLimitsAvailable);
		if (problem != null) {
			throw new IllegalArgumentException(problem);
		}
		return normalized;
	}

	/**
	 * Checks whether a normalized limit can be defined for a key, such as whether
	 * the key can be Unlimited or have shared or cost rules, and whether each rule
	 * is complete.
	 *
	 * @param limit               a normalized limit, or null
	 * @param key                 where it would be defined
	 * @param costLimitsAvailable whether the model has known prices
	 * @return why the limit cannot be defined there, or null when it can
	 */
	public static String findProblem(ModelUsageLimit limit, ModelUsageLimitKey key, boolean costLimitsAvailable) {
		if (limit == null) {
			return null;
		}
		boolean ceiling = key.isCeiling();
		if (limit.isUnlimited()) {
			if (!limit.getRules().isEmpty()) {
				return "An Unlimited limit has no rules";
			}
			if (ceiling || key.type() == Type.PLATFORM) {
				return describe(key) + " cannot be Unlimited. Remove its limit to leave it without one";
			}
			return null;
		}
		boolean canShare = key.type() == Type.TEAM || key.type() == Type.PUBLIC || key.type() == Type.MODEL;
		Set<String> measures = new HashSet<>();
		for (ModelUsageLimitRule rule : limit.getRules()) {
			if (rule.getMetric() == null || rule.getWindow() == null || rule.getCounts() == null) {
				return "Each rule needs a metric, a window and whose use it counts";
			}
			String lengthProblem = findWindowLengthProblem(rule);
			if (lengthProblem != null) {
				return lengthProblem;
			}
			if (rule.getAmount() == null || rule.getAmount() < 1) {
				return "Each rule's amount is 1 or more";
			}
			if (rule.getCounts() == Counts.SHARED && !canShare) {
				return "Only a team, public use and a model's total can have shared rules";
			}
			if (rule.getCounts() == Counts.EACH_PERSON && ceiling) {
				return describe(key) + " has shared rules only";
			}
			if (rule.getMetric() == Metric.COST && key.engineId() != null && !costLimitsAvailable) {
				return "This model has no known prices, so its limits cannot count cost";
			}
			if (!measures.add(rule.getMetric() + ":" + rule.getWindow() + ":" + rule.getWindowLength() + ":"
					+ rule.getCounts())) {
				return "A limit has only one rule for each metric, window and whose use it counts";
			}
		}
		return null;
	}

	private static String findWindowLengthProblem(ModelUsageLimitRule rule) {
		Integer length = rule.getWindowLengthOrNull();
		if (length == null || length == 1) {
			return null;
		}
		if (rule.getWindow() == Window.MINUTE) {
			return length >= 1 && length <= ModelUsageLimitRule.MAX_MINUTES ? null
					: "A minute window is 1 to " + ModelUsageLimitRule.MAX_MINUTES
							+ " minutes long. Use an hour window for longer";
		}
		if (rule.getWindow() == Window.HOUR) {
			return length >= 1 && length <= ModelUsageLimitRule.MAX_HOURS ? null
					: "An hour window is 1 to " + ModelUsageLimitRule.MAX_HOURS + " hours long";
		}
		return "Only minute and hour windows can be more than one minute or hour long";
	}

	private static String describe(ModelUsageLimitKey key) {
		switch (key.type()) {
		case MODEL:
			return "A model's total";
		case PLATFORM:
			return "The General Platform Limit";
		case TEAM:
			return key.engineId() == null ? "A team's total" : "A team's limit";
		case PUBLIC:
			return "The Public Use Limit";
		default:
			return "A person's limit";
		}
	}
}
