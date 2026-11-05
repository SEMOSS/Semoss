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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import prerna.auth.User;
import prerna.auth.AccessPermissionEnum;
import prerna.auth.access.PermissionBatch;
import prerna.auth.access.PermissionChange;
import prerna.auth.access.PermissionGrant;
import prerna.auth.access.PermissionSubject;
import prerna.auth.access.PermissionTargetResource;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.engine.impl.usage.UsageCharge;
import prerna.engine.impl.usage.UsageLimit;
import prerna.engine.impl.usage.UsageLimitKey;
import prerna.engine.impl.usage.UsageLimitKey.Source;
import prerna.engine.impl.usage.UsageLimiter;
import prerna.engine.impl.usage.UsagePolicyVersion;
import prerna.engine.impl.usage.UsageRule.Counts;
import prerna.engine.impl.usage.UsageRule.Metric;
import prerna.engine.impl.usage.UsageStoreException;
import prerna.usertracking.AuditEventType;
import prerna.usertracking.AuditStatus;
import prerna.usertracking.AuditTargetType;
import prerna.usertracking.UserAuditTrailUtils;
import prerna.usertracking.UserAuditTrailUtils.AuditEvent;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;

/**
 * Catalog-scoped policies for new engine limit adapters. Model storage stays
 * unchanged.
 */
public final class SecurityEngineUsageLimitUtils {
	private static final Logger LOG = LogManager.getLogger(SecurityEngineUsageLimitUtils.class);
	private static final int SUBJECT_BATCH_SIZE = 250;
	// Adding a catalog requires a metric profile and an execution adapter, not new
	// tables.
	// MODEL is intentionally absent: it continues through the compatibility path.
	private static final Map<CATALOG_TYPE, Set<Metric>> CATALOG_METRICS = Map.of(CATALOG_TYPE.DATABASE,
			Set.of(Metric.QUERIES, Metric.ROWS, Metric.COMPUTE_TIME));
	private static final String SELECT_POLICIES = "SELECT CATALOGTYPE,LIMITSOURCE,ENGINEID,SUBJECTID,SUBJECTTYPE,LIMITJSON FROM ENGINEUSAGELIMIT";

	private SecurityEngineUsageLimitUtils() {
	}

	public static long version() {
		return UsagePolicyVersion.current();
	}

	static void markChanged() {
		UsagePolicyVersion.changed();
	}

	static void deleteEnginePolicies(Connection connection, String engineId) throws Exception {
		QueryExecutionUtility.executeUpdate(connection, "DELETE FROM ENGINEUSAGELIMIT WHERE ENGINEID = ?",
				ps -> ps.setString(1, engineId));
	}

	static void deleteUserPolicies(Connection connection, String userId, String userType) throws Exception {
		deleteSubjectPolicies(connection, "DIRECT", "USER_TOTAL", userId, userType);
	}

	static void deleteTeamPolicies(Connection connection, String teamId, String teamType) throws Exception {
		deleteSubjectPolicies(connection, "TEAM", "TEAM_TOTAL", teamId, teamType);
	}

	private static void deleteSubjectPolicies(Connection connection, String first, String second, String id,
			String type) throws Exception {
		QueryExecutionUtility.executeUpdate(connection,
				"DELETE FROM ENGINEUSAGELIMIT WHERE LIMITSOURCE IN (?,?) AND SUBJECTID = ? AND SUBJECTTYPE = ?", ps -> {
					ps.setString(1, first);
					ps.setString(2, second);
					ps.setString(3, id);
					ps.setString(4, type);
				});
	}

	static void renameTeamPolicies(Connection connection, String oldId, String newId, String type)
			throws Exception {
		var keys = QueryExecutionUtility.queryList(connection,
				"SELECT CATALOGTYPE,LIMITSOURCE,ENGINEID FROM ENGINEUSAGELIMIT WHERE SUBJECTID = ? AND SUBJECTTYPE = ? AND LIMITSOURCE IN ('TEAM','TEAM_TOTAL')",
				ps -> {
					ps.setString(1, oldId);
					ps.setString(2, type);
				}, rs -> new UsageLimitKey(CATALOG_TYPE.valueOf(rs.getString(1)), Source.valueOf(rs.getString(2)),
						rs.getString(3), oldId, type));
		for (var old : keys) {
			var renamed = new UsageLimitKey(old.catalog(), old.source(), old.engineId(), newId, type);
			QueryExecutionUtility.executeUpdate(connection,
					"UPDATE ENGINEUSAGELIMIT SET ID = ?,SUBJECTID = ? WHERE ID = ?", ps -> {
						ps.setString(1, renamed.id());
						ps.setString(2, newId);
						ps.setString(3, old.id());
					});
		}
	}

	public record Entry(UsageLimitKey key, UsageLimit usageLimit) {
	}

	private static Entry readEntry(ResultSet result) throws SQLException {
		return new Entry(new UsageLimitKey(CATALOG_TYPE.valueOf(result.getString(1)),
				Source.valueOf(result.getString(2)), result.getString(3), result.getString(4), result.getString(5)),
				decode(result.getString(6)));
	}

	public static void validate(UsageLimitKey key, UsageLimit limit) {
		if (key == null)
			throw new IllegalArgumentException("Must provide the usage policy key");
		key.validate();
		// New catalogs opt in here; models must continue through their existing API.
		var supported = CATALOG_METRICS.get(key.catalog());
		if (supported == null)
			throw new IllegalArgumentException("This catalog has no generic usage adapter");
		if (limit == null)
			return;
		limit.validate();
		if (limit.unlimited() && (key.ceiling() || key.source() == Source.PLATFORM))
			throw new IllegalArgumentException("A total or platform default cannot be Unlimited");
		for (var rule : limit.rules()) {
			if (!supported.contains(rule.metric()))
				throw new IllegalArgumentException("Unsupported metric for this engine catalog");
			if (key.ceiling() && rule.counts() != Counts.SHARED)
				throw new IllegalArgumentException("A total has shared rules only");
			if (rule.counts() == Counts.SHARED && key.source() != Source.TEAM && key.source() != Source.PUBLIC
					&& !key.ceiling())
				throw new IllegalArgumentException("Only teams, public use and totals support shared rules");
		}
	}

	/**
	 * Owner-only change to this engine's policy, independent of permission grants.
	 */
	public static void setLimit(User actor, UsageLimitKey key, UsageLimit limit) throws Exception {
		if (key == null || key.engineId() == null || actor == null || actor.isAnonymous()
				|| !SecurityEngineUtils.userIsOwner(actor, key.engineId()))
			throw new IllegalArgumentException("Only engine owners can set this usage limit");
		requireEngineCatalog(key);
		save(actor, key, limit, false);
	}

	/**
	 * The utility is proof of admin authorization; no caller-supplied admin flag.
	 */
	public static void setAdminLimit(SecurityAdminUtils admin, User actor, UsageLimitKey key, UsageLimit limit)
			throws Exception {
		if (admin == null || actor == null || actor.isAnonymous())
			throw new IllegalArgumentException("Administrator access is required");
		if (key != null && key.engineId() != null)
			requireEngineCatalog(key);
		save(actor, key, limit, true);
	}

	private static void requireEngineCatalog(UsageLimitKey key) {
		Object[] type = SecurityEngineUtils.getEngineTypeAndSubtype(key.engineId());
		if (key.catalog() == null || type == null || type.length == 0
				|| !key.catalog().name().equals(String.valueOf(type[0])))
			throw new IllegalArgumentException("Engine does not exist or user does not have access");
	}

	private static void save(User actor, UsageLimitKey key, UsageLimit limit, boolean admin) throws Exception {
		UsageLimit normalized=EngineUsagePermissionSupport.normalize(limit);
		validate(key, normalized);
		if(key.engineId()!=null) {saveEnginePolicy(actor,key,normalized,admin);return;}
		var who=User.getPrimaryUserIdAndTypePair(actor);
		UsageLimit before=getLimit(key);
		QueryExecutionUtility.write(SystemEngineRegistry.getSecurityDb(),conn -> {setLimit(conn,key,normalized,who);return null;});
		markChanged();
		auditChange(actor,key,before,normalized,admin);
	}

	private static void saveEnginePolicy(User actor,UsageLimitKey key,UsageLimit limit,boolean admin) throws Exception {
		PermissionSubject subject;
		PermissionGrant grant;
		if(key.source()==Source.DIRECT || key.source()==Source.TEAM) {
			boolean team=key.source()==Source.TEAM;
			String sql=team ? "SELECT PERMISSION,ENDDATE FROM GROUPENGINEPERMISSION WHERE ENGINEID = ? AND ID = ? AND TYPE = ?" : "SELECT PERMISSION,ENDDATE FROM ENGINEPERMISSION WHERE ENGINEID = ? AND USERID = ?";
			grant=QueryExecutionUtility.queryOne(SystemEngineRegistry.getSecurityDb(),sql,ps -> {ps.setString(1,key.engineId());ps.setString(2,key.subjectId());if(team)ps.setString(3,key.subjectType());},rs -> {
				int level=rs.getInt(1);
				return rs.wasNull() ? null : new PermissionGrant(AccessPermissionEnum.getPermissionValueById(level),PermissionBatchUtils.toIsoUtc(rs.getTimestamp(2)));
			});
			if(grant==null)throw new IllegalArgumentException("The subject has no grant on this engine");
			subject=team ? PermissionSubject.ofTeam(key.subjectId(),key.subjectType()) : PermissionSubject.ofUser(key.subjectId(),key.subjectType());
		} else {
			subject=new PermissionSubject(key.source()==Source.PUBLIC ? PermissionSubject.SubjectCategory.PUBLIC : PermissionSubject.SubjectCategory.ENGINE,null,null);
			grant=new PermissionGrant();
		}
		grant.withEngineUsageLimit(limit);
		PermissionBatchUtils.save(actor,List.of(new PermissionBatch(PermissionTargetResource.engine(key.engineId()),List.of(PermissionChange.change(subject,grant)))),admin ? PermissionBatchOptions.adminMode() : PermissionBatchOptions.userMode());
	}

	static void setLimit(Connection connection,UsageLimitKey key,UsageLimit limit,Pair<String,String> by) throws Exception {
		validate(key,limit);
		QueryExecutionUtility.executeUpdate(connection,"DELETE FROM ENGINEUSAGELIMIT WHERE ID = ?",ps -> ps.setString(1,key.id()));
		if(limit!=null)QueryExecutionUtility.executeUpdate(connection,
			"INSERT INTO ENGINEUSAGELIMIT (ID,CATALOGTYPE,ENGINEID,LIMITSOURCE,SUBJECTID,SUBJECTTYPE,LIMITJSON,MODIFIEDAT,MODIFIEDBY,MODIFIEDBYTYPE) VALUES (?,?,?,?,?,?,?,?,?,?)",ps -> {
				ps.setString(1,key.id());ps.setString(2,key.catalog().name());ps.setString(3,key.engineId());ps.setString(4,key.source().name());ps.setString(5,key.subjectId());ps.setString(6,key.subjectType());
				SystemEngineRegistry.getSecurityDb().getQueryUtil().setNullableJson(ps,7,limit,GsonUtility.getDefaultGson());ps.setTimestamp(8,Utility.getCurrentSqlTimestampUTC());ps.setString(9,by.getValue0());ps.setString(10,by.getValue1());
			});
	}

	static void deleteDirectPolicies(Connection connection,String engineId,String userId) throws Exception {
		QueryExecutionUtility.executeUpdate(connection,"DELETE FROM ENGINEUSAGELIMIT WHERE ENGINEID = ? AND LIMITSOURCE = 'DIRECT' AND SUBJECTID = ?",ps -> {ps.setString(1,engineId);ps.setString(2,userId);});
	}

	public static List<Entry> readEnginePolicies(CATALOG_TYPE catalog,String engineId) {
		try {return QueryExecutionUtility.queryList(SystemEngineRegistry.getSecurityDb(),SELECT_POLICIES+" WHERE CATALOGTYPE = ? AND ENGINEID = ?",ps -> {ps.setString(1,catalog.name());ps.setString(2,engineId);},SecurityEngineUsageLimitUtils::readEntry);}
		catch(Exception failure) {throw new UsageStoreException("Unable to read engine usage limits",failure);}
	}

	static void auditChange(User actor,UsageLimitKey key,UsageLimit before,UsageLimit limit,boolean admin) {
		try {
			if (Utility.isUserTrackingEnabled()) {
				Map<String, Object> oldValue = new LinkedHashMap<>();
				oldValue.put("usageLimit", before);
				Map<String, Object> newValue = new LinkedHashMap<>();
				newValue.put("usageLimit", limit);
				UserAuditTrailUtils.recordEvent(new AuditEvent().actorUser(actor).actorIsAdmin(admin)
						.eventType(AuditEventType.USAGE_LIMIT_UPDATE).status(AuditStatus.SUCCESS)
						.target(key.engineId() == null ? AuditTargetType.PLATFORM : AuditTargetType.DATABASE,
								key.engineId(), null)
						.context(null, key.engineId(), null, null).oldValue(oldValue).newValue(newValue)
						.details(Map.of("catalogType", key.catalog().name(), "limitType", key.source().name(),
								"limitKey", key)));
			}
		} catch (Exception e) {
			LOG.warn("Unable to audit engine usage policy change", e);
		}
	}

	// Raw reads are server-side infrastructure. Public views below authorize before
	// reading.
	public static UsageLimit getLimit(UsageLimitKey key) {
		try {
			return QueryExecutionUtility.queryOne(SystemEngineRegistry.getSecurityDb(),
					"SELECT LIMITJSON FROM ENGINEUSAGELIMIT WHERE ID = ?", ps -> ps.setString(1, key.id()),
					rs -> decode(rs.getString(1)));
		} catch (Exception e) {
			throw new UsageStoreException("Unable to read usage policy", e);
		}
	}

	public static List<Entry> readForEngine(String engineId) {
		return readForEngine(CATALOG_TYPE.DATABASE, engineId);
	}

	public static List<Entry> readForEngine(CATALOG_TYPE catalog, String engineId) {
		try {
			return QueryExecutionUtility.queryList(SystemEngineRegistry.getSecurityDb(),
					SELECT_POLICIES + " WHERE CATALOGTYPE = ? AND (ENGINEID = ? OR ENGINEID IS NULL)", ps -> {
						ps.setString(1, catalog.name());
						ps.setString(2, engineId);
					}, SecurityEngineUsageLimitUtils::readEntry);
		} catch (Exception e) {
			throw new UsageStoreException("Unable to read usage policies", e);
		}
	}

	/**
	 * Read only policies relevant to this identity; never load every user's totals.
	 */
	static List<Entry> readForPerson(String engineId, EngineUsageIdentity.Person person) {
		return readForPerson(CATALOG_TYPE.DATABASE, engineId, person);
	}

	static List<Entry> readForPerson(CATALOG_TYPE catalog, String engineId, EngineUsageIdentity.Person person) {
		var subjects = new LinkedHashSet<Pair<String, String>>(person.logins());
		for (var team : person.teams())
			subjects.add(Pair.with(team.teamId(), team.teamType()));
		var ordered = new ArrayList<>(subjects);
		Map<UsageLimitKey, Entry> result = new LinkedHashMap<>();
		for (int start = 0; start < ordered.size(); start += SUBJECT_BATCH_SIZE) {
			var batch = ordered.subList(start, Math.min(start + SUBJECT_BATCH_SIZE, ordered.size()));
			StringBuilder sql = new StringBuilder(SELECT_POLICIES
					+ " WHERE CATALOGTYPE = ? AND (ENGINEID = ? OR ENGINEID IS NULL) AND (LIMITSOURCE IN ('ENGINE_TOTAL','PUBLIC','PLATFORM')");
			for (var ignored : batch)
				sql.append(" OR (SUBJECTID = ? AND SUBJECTTYPE = ?)");
			sql.append(")");
			try {
				List<Entry> found = QueryExecutionUtility.queryList(SystemEngineRegistry.getSecurityDb(),
						sql.toString(), ps -> {
							ps.setString(1, catalog.name());
							ps.setString(2, engineId);
							int index = 3;
							for (var subject : batch) {
								ps.setString(index++, subject.getValue0());
								ps.setString(index++, subject.getValue1());
							}
						}, SecurityEngineUsageLimitUtils::readEntry);
				for (var entry : found)
					result.put(entry.key(), entry);
			} catch (Exception e) {
				throw new UsageStoreException("Unable to read the person's usage policies", e);
			}
		}
		return List.copyOf(result.values());
	}

	public static List<Entry> getEngineLimits(User actor, String engineId) {
		return getEngineLimits(actor, CATALOG_TYPE.DATABASE, engineId);
	}

	public static List<Entry> getEngineLimits(User actor, CATALOG_TYPE catalog, String engineId) {
		if (actor == null || actor.isAnonymous() || !SecurityEngineUtils.userIsOwner(actor, engineId))
			throw new IllegalArgumentException("Only engine owners can read these usage policies");
		// Cross-engine personal and team defaults are private to admins and their
		// subject.
		try {
			return QueryExecutionUtility.queryList(SystemEngineRegistry.getSecurityDb(),
					SELECT_POLICIES + " WHERE CATALOGTYPE = ? AND ENGINEID = ?", ps -> {
						ps.setString(1, catalog.name());
						ps.setString(2, engineId);
					}, SecurityEngineUsageLimitUtils::readEntry);
		} catch (Exception e) {
			throw new UsageStoreException("Unable to read engine usage policies", e);
		}
	}

	public static UsageLimit decode(String json) {
		return json == null || json.isBlank() || "null".equals(json) ? null
				: GsonUtility.getDefaultGson().fromJson(json, UsageLimit.class);
	}

	public static UsageLimiter.History history() {
		return HISTORY;
	}

	private static final UsageLimiter.History HISTORY = new UsageLimiter.History() {
		@Override
		public long used(String counterKey) {
			try {
				Long amount = QueryExecutionUtility.queryOne(SystemEngineRegistry.getSecurityDb(),
						"SELECT SUM(AMOUNT) FROM ENGINEUSAGEHISTORY WHERE COUNTERID = ?",
						ps -> ps.setString(1, counterKey), rs -> rs.getLong(1));
				return amount == null ? 0 : amount;
			} catch (Exception e) {
				throw new UsageStoreException("Unable to recover engine usage", e);
			}
		}

		@Override
		public void record(Map<String, UsageCharge> charges) {
			try {
				QueryExecutionUtility.write(SystemEngineRegistry.getSecurityDb(), conn -> {
					QueryExecutionUtility.executeBatch(conn,
							"INSERT INTO ENGINEUSAGEHISTORY (ID,COUNTERID,AMOUNT,EXPIRESAT) VALUES (?,?,?,?)",
							charges.values(), (ps, charge) -> {
								ps.setString(1, UUID.randomUUID().toString());
								ps.setString(2, charge.key());
								ps.setLong(3, charge.amount());
								ps.setLong(4, charge.expiresAtMillis());
							});
					return null;
				});
			} catch (Exception e) {
				throw new UsageStoreException("Unable to persist engine usage", e);
			}
		}
	};
}
