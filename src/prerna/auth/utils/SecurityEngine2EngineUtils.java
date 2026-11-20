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
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * Reads the permissions engines give to apps and to other engines. An app's
 * custom reactors, or another engine's own code, use such a permission in place
 * of the signed in user's own, so an app can write to a database for people who
 * have no access to it themselves, and a vector database can embed with a model
 * whose owner lets it. The rows are in {@code ENGINE2ENGINEPERMISSION}, one for
 * each engine and grantee, the grantee kept by its id and catalog type
 * ({@code PROJECT} for an app), and only a permission batch writes them
 * ({@link PermissionBatchUtils}).
 */
final class SecurityEngine2EngineUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityEngine2EngineUtils.class);

	private static final String ENGINE_ID = "ENGINE2ENGINEPERMISSION__ENGINEID";
	private static final String GRANTEE_ENGINE_ID = "ENGINE2ENGINEPERMISSION__GRANTEEENGINEID";
	private static final String GRANTEE_ENGINE_TYPE = "ENGINE2ENGINEPERMISSION__GRANTEEENGINETYPE";
	private static final String PERMISSION = "ENGINE2ENGINEPERMISSION__PERMISSION";
	private static final String END_DATE = "ENGINE2ENGINEPERMISSION__ENDDATE";

	private static final String ENGINE_GRANTEES = """
			SELECT P.GRANTEEENGINEID, P.GRANTEEENGINETYPE, P.PERMISSION, P.DATEADDED, P.ENDDATE, \
			PR.PROJECTNAME, PR.PROJECTDISPLAYNAME, E.ENGINENAME, E.ENGINEDISPLAYNAME, E.ENGINESUBTYPE \
			FROM ENGINE2ENGINEPERMISSION P \
			LEFT OUTER JOIN PROJECT PR ON P.GRANTEEENGINETYPE = ? AND PR.PROJECTID = P.GRANTEEENGINEID \
			LEFT OUTER JOIN ENGINE E ON P.GRANTEEENGINETYPE <> ? AND E.ENGINEID = P.GRANTEEENGINEID \
			WHERE P.ENGINEID = ? \
			ORDER BY P.GRANTEEENGINETYPE, P.GRANTEEENGINEID""";

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
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(PERMISSION));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(GRANTEE_ENGINE_ID, "==", grantee.engineId()));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(GRANTEE_ENGINE_TYPE, "==", grantee.engineType()));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(ENGINE_ID, "==", engineId));
		Integer permission = null;
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			if (wrapper.hasNext()) {
				Object value = wrapper.next().getValues()[0];
				if (value instanceof Number) {
					permission = ((Number) value).intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Unable to read the permission engine {} gives {} {}", engineId, grantee.engineType(),
					grantee.engineId(), e);
		}
		return permission;
	}

	/**
	 * Reads the apps and engines an engine gives a permission to, with each one's
	 * name, level and dates, apps first, then by type and id. A row keeps the
	 * columns' names: {@code GRANTEEENGINEID}, {@code GRANTEEENGINETYPE},
	 * {@code PERMISSION}, {@code DATEADDED}, {@code ENDDATE}, an app's
	 * {@code PROJECTNAME} and {@code PROJECTDISPLAYNAME}, and an engine's
	 * {@code ENGINENAME}, {@code ENGINEDISPLAYNAME} and {@code ENGINESUBTYPE}.
	 *
	 * @param engineId the engine
	 * @return one row for each app and engine
	 */
	static List<Map<String, Object>> getEngineGrantees(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		try {
			return QueryExecutionUtility.queryList(securityDb, ENGINE_GRANTEES, ps -> {
				ps.setString(1, PermissionSubject.PROJECT_TYPE);
				ps.setString(2, PermissionSubject.PROJECT_TYPE);
				ps.setString(3, engineId);
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
				row.put("ENGINENAME", rs.getString("ENGINENAME"));
				row.put("ENGINEDISPLAYNAME", rs.getString("ENGINEDISPLAYNAME"));
				row.put("ENGINESUBTYPE", rs.getString("ENGINESUBTYPE"));
				return row;
			});
		} catch (Exception e) {
			throw new IllegalStateException("Unable to read the apps and engines engine " + engineId
					+ " gives a permission to", e);
		}
	}

	/**
	 * Reads every permission an engine gives an app or another engine that is in
	 * effect now, for the check the engine proxy runs on each call. A permission
	 * that has ended is left out, and so is one with no level.
	 *
	 * @return for each engine id, the level of each app or engine it gives one to
	 */
	static Map<String, Map<EngineGrantee, AccessPermissionEnum>> getPermissionsInEffect() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(ENGINE_ID));
		qs.addSelector(new QueryColumnSelector(GRANTEE_ENGINE_ID));
		qs.addSelector(new QueryColumnSelector(GRANTEE_ENGINE_TYPE));
		qs.addSelector(new QueryColumnSelector(PERMISSION));
		qs.addExplicitFilter(AbstractSecurityUtils.getUnexpiredFilter(END_DATE));
		Map<String, Map<EngineGrantee, AccessPermissionEnum>> permissions = new HashMap<>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				Object[] values = row.getValues();
				AccessPermissionEnum level = levelOf(values[3]);
				if (values[0] != null && values[1] != null && values[2] != null && level != null) {
					permissions.computeIfAbsent(values[0].toString(), engineId -> new HashMap<>())
							.put(new EngineGrantee(values[1].toString(), values[2].toString()), level);
				}
			}
		} catch (Exception e) {
			throw new IllegalStateException("Unable to read the permissions engines give to apps and engines", e);
		}
		return permissions;
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
