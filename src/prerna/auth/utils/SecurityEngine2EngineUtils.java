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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.access.EngineGrantee;
import prerna.auth.access.PermissionSubject;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * Reads the permissions engines and projects give to apps and to engines. An
 * app's custom reactors, an agent's runs, or another engine's own code, use
 * such a permission in place of the signed in user's own, so an app can write
 * to a database for people who have no access to it themselves, an agent can
 * use a skill its users cannot open, and a vector database can embed with a
 * model whose owner lets it. The rows are in {@code ENGINE2ENGINEPERMISSION},
 * one for each target and grantee: the target by its id and type
 * ({@code ENGINETYPE}, {@code PROJECT} for a project), the grantee by its id
 * and catalog type ({@code PROJECT} for an app). Only a permission batch writes
 * them ({@link PermissionBatchUtils}).
 */
final class SecurityEngine2EngineUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityEngine2EngineUtils.class);

	private static final String ENGINE_ID = "ENGINE2ENGINEPERMISSION__ENGINEID";
	private static final String ENGINE_TYPE = "ENGINE2ENGINEPERMISSION__ENGINETYPE";
	private static final String GRANTEE_ENGINE_ID = "ENGINE2ENGINEPERMISSION__GRANTEEENGINEID";
	private static final String GRANTEE_ENGINE_TYPE = "ENGINE2ENGINEPERMISSION__GRANTEEENGINETYPE";
	private static final String PERMISSION = "ENGINE2ENGINEPERMISSION__PERMISSION";
	private static final String END_DATE = "ENGINE2ENGINEPERMISSION__ENDDATE";

	private static final String ENGINE_GRANTEES = """
			SELECT P.GRANTEEENGINEID, P.GRANTEEENGINETYPE, P.PERMISSION, P.DATEADDED, P.ENDDATE, \
			PR.PROJECTNAME, PR.PROJECTDISPLAYNAME, PR.TYPE AS PROJECTTYPE, E.ENGINENAME, E.ENGINEDISPLAYNAME, \
			E.ENGINESUBTYPE \
			FROM ENGINE2ENGINEPERMISSION P \
			LEFT OUTER JOIN PROJECT PR ON P.GRANTEEENGINETYPE = ? AND PR.PROJECTID = P.GRANTEEENGINEID \
			LEFT OUTER JOIN ENGINE E ON P.GRANTEEENGINETYPE <> ? AND E.ENGINEID = P.GRANTEEENGINEID \
			WHERE P.ENGINEID = ? AND %s \
			ORDER BY P.GRANTEEENGINETYPE, P.GRANTEEENGINEID""";

	private static final String GRANTEE_PERMISSION = """
			SELECT P.PERMISSION FROM ENGINE2ENGINEPERMISSION P \
			WHERE P.GRANTEEENGINEID = ? AND P.GRANTEEENGINETYPE = ? AND P.ENGINEID = ? AND %s""";

	private static final String ALL_GRANTS = """
			SELECT P.ENGINEID, P.ENGINETYPE, P.GRANTEEENGINEID, P.GRANTEEENGINETYPE, P.PERMISSION, P.ENDDATE, \
			TE.ENGINETYPE AS TARGETCATALOGTYPE, TE.ENGINENAME AS TARGETENGINENAME, \
			TE.ENGINEDISPLAYNAME AS TARGETENGINEDISPLAYNAME, TE.ENGINESUBTYPE AS TARGETENGINESUBTYPE, \
			TP.PROJECTNAME AS TARGETPROJECTNAME, TP.PROJECTDISPLAYNAME AS TARGETPROJECTDISPLAYNAME, \
			TP.TYPE AS TARGETPROJECTTYPE, \
			GE.ENGINENAME AS GRANTEEENGINENAME, GE.ENGINEDISPLAYNAME AS GRANTEEENGINEDISPLAYNAME, \
			GE.ENGINESUBTYPE AS GRANTEEENGINESUBTYPE, \
			GP.PROJECTNAME AS GRANTEEPROJECTNAME, GP.PROJECTDISPLAYNAME AS GRANTEEPROJECTDISPLAYNAME, \
			GP.TYPE AS GRANTEEPROJECTTYPE \
			FROM ENGINE2ENGINEPERMISSION P \
			LEFT OUTER JOIN ENGINE TE ON (P.ENGINETYPE IS NULL OR P.ENGINETYPE <> ?) AND TE.ENGINEID = P.ENGINEID \
			LEFT OUTER JOIN PROJECT TP ON P.ENGINETYPE = ? AND TP.PROJECTID = P.ENGINEID \
			LEFT OUTER JOIN ENGINE GE ON P.GRANTEEENGINETYPE <> ? AND GE.ENGINEID = P.GRANTEEENGINEID \
			LEFT OUTER JOIN PROJECT GP ON P.GRANTEEENGINETYPE = ? AND GP.PROJECTID = P.GRANTEEENGINEID \
			ORDER BY P.ENGINEID, P.GRANTEEENGINETYPE, P.GRANTEEENGINEID""";

	/** A row whose target is a project */
	private static final String TARGET_IS_PROJECT = "P.ENGINETYPE = '" + PermissionSubject.PROJECT_TYPE + "'";
	/**
	 * A row whose target is an engine; rows written before the target's type was
	 * kept have none
	 */
	private static final String TARGET_IS_ENGINE = "(P.ENGINETYPE IS NULL OR P.ENGINETYPE <> '"
			+ PermissionSubject.PROJECT_TYPE + "')";

	private static final String ENGINES_GIVING_ACCESS = """
			SELECT P.ENGINEID, COALESCE(P.ENGINETYPE, E.ENGINETYPE) AS ENGINETYPE, P.PERMISSION, P.DATEADDED, \
			P.ENDDATE, E.ENGINENAME, E.ENGINEDISPLAYNAME, E.ENGINESUBTYPE, \
			PR.PROJECTNAME, PR.PROJECTDISPLAYNAME, PR.TYPE AS PROJECTTYPE \
			FROM ENGINE2ENGINEPERMISSION P \
			LEFT OUTER JOIN ENGINE E ON (P.ENGINETYPE IS NULL OR P.ENGINETYPE <> ?) AND E.ENGINEID = P.ENGINEID \
			LEFT OUTER JOIN PROJECT PR ON P.ENGINETYPE = ? AND PR.PROJECTID = P.ENGINEID \
			WHERE P.GRANTEEENGINEID = ? AND P.GRANTEEENGINETYPE = ? \
			ORDER BY P.ENGINETYPE, P.ENGINEID""";

	private SecurityEngine2EngineUtils() {
	}

	/**
	 * Reads the level an engine gives an app or another engine, as stored, whether
	 * or not it has ended.
	 *
	 * @param grantee  the app or engine
	 * @param engineId the engine giving the permission
	 * @return the level's id, or null when the engine gives the grantee none
	 */
	static Integer getGranteePermission(EngineGrantee grantee, String engineId) {
		return getGranteePermission(grantee, engineId, false);
	}

	/**
	 * Reads the level an engine or a project gives an app or an engine, as stored,
	 * whether or not it has ended.
	 *
	 * @param grantee  the app or engine
	 * @param targetId the engine or project giving the permission
	 * @param project  whether the target is a project
	 * @return the level's id, or null when the target gives the grantee none
	 */
	static Integer getGranteePermission(EngineGrantee grantee, String targetId, boolean project) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		try {
			List<Integer> levels = QueryExecutionUtility.queryList(securityDb,
					GRANTEE_PERMISSION.formatted(targetCondition(project)), ps -> {
						ps.setString(1, grantee.engineId());
						ps.setString(2, grantee.engineType());
						ps.setString(3, targetId);
					}, rs -> {
						int level = rs.getInt(1);
						return rs.wasNull() ? null : level;
					});
			return levels.isEmpty() ? null : levels.get(0);
		} catch (Exception e) {
			classLogger.error("Unable to read the permission {} {} gives {} {}", project ? "project" : "engine",
					targetId, grantee.engineType(), grantee.engineId(), e);
			return null;
		}
	}

	/**
	 * Reads the apps and engines an engine gives a permission to, with each one's
	 * name, level and dates, apps first, then by type and id. A row keeps the
	 * columns' names: {@code GRANTEEENGINEID}, {@code GRANTEEENGINETYPE},
	 * {@code PERMISSION}, {@code DATEADDED}, {@code ENDDATE}, an app's
	 * {@code PROJECTNAME}, {@code PROJECTDISPLAYNAME} and {@code PROJECTTYPE}, and
	 * an engine's {@code ENGINENAME}, {@code ENGINEDISPLAYNAME} and
	 * {@code ENGINESUBTYPE}.
	 *
	 * @param engineId the engine
	 * @return one row for each app and engine
	 */
	static List<Map<String, Object>> getEngineGrantees(String engineId) {
		return getGrantees(engineId, false);
	}

	/**
	 * Reads the apps and engines an engine or a project gives a permission to, in
	 * the shape {@link #getEngineGrantees(String)} reads them.
	 *
	 * @param targetId the engine or project
	 * @param project  whether it is a project
	 * @return one row for each app and engine
	 */
	static List<Map<String, Object>> getGrantees(String targetId, boolean project) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		try {
			return QueryExecutionUtility.queryList(securityDb, ENGINE_GRANTEES.formatted(targetCondition(project)),
					ps -> {
						ps.setString(1, PermissionSubject.PROJECT_TYPE);
						ps.setString(2, PermissionSubject.PROJECT_TYPE);
						ps.setString(3, targetId);
					}, rs -> {
						Map<String, Object> row = new LinkedHashMap<>();
						row.put("GRANTEEENGINEID", rs.getString("GRANTEEENGINEID"));
						row.put("GRANTEEENGINETYPE", rs.getString("GRANTEEENGINETYPE"));
						int level = rs.getInt("PERMISSION");
						row.put("PERMISSION", rs.wasNull() ? null : level);
						row.put("DATEADDED", rs.getTimestamp("DATEADDED"));
						row.put("ENDDATE", rs.getTimestamp("ENDDATE"));
						row.put("PROJECTNAME", rs.getString("PROJECTNAME"));
						row.put("PROJECTDISPLAYNAME", rs.getString("PROJECTDISPLAYNAME"));
						row.put("PROJECTTYPE", rs.getString("PROJECTTYPE"));
						row.put("ENGINENAME", rs.getString("ENGINENAME"));
						row.put("ENGINEDISPLAYNAME", rs.getString("ENGINEDISPLAYNAME"));
						row.put("ENGINESUBTYPE", rs.getString("ENGINESUBTYPE"));
						return row;
					});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to read the apps and engines " + (project ? "project " : "engine ")
					+ targetId + " gives a permission to", e);
		}
	}

	/**
	 * Reads the engines and projects that give an app or another engine a
	 * permission, the other side of {@link #getGrantees(String, boolean)}: each one
	 * whose owners let the app's custom reactors, its agent runs, or the engine's
	 * own code, use it, with its name, level and dates, by catalog type and id. A
	 * row keeps the columns' names: {@code ENGINEID}, {@code ENGINETYPE}
	 * ({@code PROJECT} for a project), {@code PERMISSION}, {@code DATEADDED},
	 * {@code ENDDATE}, an engine's {@code ENGINENAME}, {@code ENGINEDISPLAYNAME}
	 * and {@code ENGINESUBTYPE}, and a project's {@code PROJECTNAME},
	 * {@code PROJECTDISPLAYNAME} and {@code PROJECTTYPE}.
	 *
	 * @param grantee the app or engine
	 * @return one row for each engine that gives it a permission
	 */
	static List<Map<String, Object>> getEnginesGivingAccessTo(EngineGrantee grantee) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		try {
			return QueryExecutionUtility.queryList(securityDb, ENGINES_GIVING_ACCESS, ps -> {
				ps.setString(1, PermissionSubject.PROJECT_TYPE);
				ps.setString(2, PermissionSubject.PROJECT_TYPE);
				ps.setString(3, grantee.engineId());
				ps.setString(4, grantee.engineType());
			}, rs -> {
				Map<String, Object> row = new LinkedHashMap<>();
				row.put("ENGINEID", rs.getString("ENGINEID"));
				row.put("ENGINETYPE", rs.getString("ENGINETYPE"));
				int level = rs.getInt("PERMISSION");
				row.put("PERMISSION", rs.wasNull() ? null : level);
				row.put("DATEADDED", rs.getTimestamp("DATEADDED"));
				row.put("ENDDATE", rs.getTimestamp("ENDDATE"));
				row.put("ENGINENAME", rs.getString("ENGINENAME"));
				row.put("ENGINEDISPLAYNAME", rs.getString("ENGINEDISPLAYNAME"));
				row.put("ENGINESUBTYPE", rs.getString("ENGINESUBTYPE"));
				row.put("PROJECTNAME", rs.getString("PROJECTNAME"));
				row.put("PROJECTDISPLAYNAME", rs.getString("PROJECTDISPLAYNAME"));
				row.put("PROJECTTYPE", rs.getString("PROJECTTYPE"));
				return row;
			});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to read the engines that give " + grantee.engineType() + " "
					+ grantee.engineId() + " a permission", e);
		}
	}

	/**
	 * Reads every permission any engine or project gives an app or an engine, with
	 * the names of both sides, for the page that lists them all. A row keeps the
	 * columns' names: {@code ENGINEID}, {@code ENGINETYPE} ({@code PROJECT} for a
	 * project target), {@code GRANTEEENGINEID}, {@code GRANTEEENGINETYPE},
	 * {@code PERMISSION}, {@code ENDDATE}, an engine target's
	 * {@code TARGETCATALOGTYPE}, {@code TARGETENGINENAME},
	 * {@code TARGETENGINEDISPLAYNAME} and {@code TARGETENGINESUBTYPE}, a project
	 * target's {@code TARGETPROJECTNAME}, {@code TARGETPROJECTDISPLAYNAME} and
	 * {@code TARGETPROJECTTYPE}, and the grantee's names and subtype in the same
	 * way ({@code GRANTEE...}).
	 *
	 * @return one row for each permission, by target and grantee
	 */
	static List<Map<String, Object>> getAllGrants() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String[] columns = { "ENGINEID", "ENGINETYPE", "GRANTEEENGINEID", "GRANTEEENGINETYPE", "TARGETCATALOGTYPE",
				"TARGETENGINENAME", "TARGETENGINEDISPLAYNAME", "TARGETENGINESUBTYPE", "TARGETPROJECTNAME",
				"TARGETPROJECTDISPLAYNAME", "TARGETPROJECTTYPE", "GRANTEEENGINENAME", "GRANTEEENGINEDISPLAYNAME",
				"GRANTEEENGINESUBTYPE", "GRANTEEPROJECTNAME", "GRANTEEPROJECTDISPLAYNAME", "GRANTEEPROJECTTYPE" };
		try {
			return QueryExecutionUtility.queryList(securityDb, ALL_GRANTS, ps -> {
				for (int i = 1; i <= 4; i++) {
					ps.setString(i, PermissionSubject.PROJECT_TYPE);
				}
			}, rs -> {
				Map<String, Object> row = new LinkedHashMap<>();
				for (String column : columns) {
					row.put(column, rs.getString(column));
				}
				int level = rs.getInt("PERMISSION");
				row.put("PERMISSION", rs.wasNull() ? null : level);
				row.put("ENDDATE", rs.getTimestamp("ENDDATE"));
				return row;
			});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to read the permissions engines and projects give", e);
		}
	}

	/**
	 * Reads every permission an engine gives an app or another engine that is in
	 * effect now, for the check the engine proxy runs on each call. A permission
	 * that has ended is left out, and so is one with no level.
	 *
	 * @return for each target, by {@link #targetKey(String, boolean)}, the level of
	 *         each app or engine it gives one to
	 */
	static Map<String, Map<EngineGrantee, AccessPermissionEnum>> getPermissionsInEffect() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(ENGINE_ID));
		qs.addSelector(new QueryColumnSelector(GRANTEE_ENGINE_ID));
		qs.addSelector(new QueryColumnSelector(GRANTEE_ENGINE_TYPE));
		qs.addSelector(new QueryColumnSelector(PERMISSION));
		qs.addSelector(new QueryColumnSelector(ENGINE_TYPE));
		qs.addExplicitFilter(AbstractSecurityUtils.getUnexpiredFilter(END_DATE));
		Map<String, Map<EngineGrantee, AccessPermissionEnum>> permissions = new HashMap<>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				Object[] values = row.getValues();
				AccessPermissionEnum level = levelOf(values[3]);
				if (values[0] != null && values[1] != null && values[2] != null && level != null) {
					boolean project = PermissionSubject.PROJECT_TYPE.equals(values[4]);
					permissions.computeIfAbsent(targetKey(values[0].toString(), project), key -> new HashMap<>())
							.put(new EngineGrantee(values[1].toString(), values[2].toString()), level);
				}
			}
		} catch (Exception e) {
			throw new IllegalStateException("Unable to read the permissions engines give to apps and engines", e);
		}
		return permissions;
	}

	/**
	 * The key of a target in {@link #getPermissionsInEffect()}, so a project and an
	 * engine with the same id are two targets.
	 *
	 * @param targetId the engine or project
	 * @param project  whether it is a project
	 * @return the key
	 */
	static String targetKey(String targetId, boolean project) {
		return (project ? "P:" : "E:") + targetId;
	}

	private static String targetCondition(boolean project) {
		return project ? TARGET_IS_PROJECT : TARGET_IS_ENGINE;
	}

	private static AccessPermissionEnum levelOf(Object value) {
		if (value instanceof Number) {
			int id = ((Number) value).intValue();
			for (AccessPermissionEnum level : AccessPermissionEnum.values()) {
				if (level.getId() == id) {
					return level;
				}
			}
		}
		return null;
	}

}
