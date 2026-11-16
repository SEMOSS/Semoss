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
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessPermissionEnum;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

/**
 * Reads the permissions engines give to projects. A project's custom reactors
 * use such a permission in place of the signed in user's own, so an app can
 * write to a database or a storage engine for people who have no access to it
 * themselves. The rows are in {@code PROJECTENGINEPERMISSION}, one for each
 * project and engine, and only a permission batch writes them
 * ({@link PermissionBatchUtils}).
 */
final class SecurityProjectEngineUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityProjectEngineUtils.class);

	private static final String PROJECT_ID = "PROJECTENGINEPERMISSION__PROJECTID";
	private static final String ENGINE_ID = "PROJECTENGINEPERMISSION__ENGINEID";
	private static final String PERMISSION = "PROJECTENGINEPERMISSION__PERMISSION";
	private static final String DATE_ADDED = "PROJECTENGINEPERMISSION__DATEADDED";
	private static final String END_DATE = "PROJECTENGINEPERMISSION__ENDDATE";

	private SecurityProjectEngineUtils() {
	}

	/**
	 * Reads the level an engine gives a project, as stored, whether or not it has
	 * ended.
	 *
	 * @param projectId the project
	 * @param engineId  the engine
	 * @return the level's id, or null when the engine gives the project none
	 */
	static Integer getProjectEnginePermission(String projectId, String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(PERMISSION));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(PROJECT_ID, "==", projectId));
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
			classLogger.error("Unable to read the permission engine {} gives project {}", engineId, projectId, e);
		}
		return permission;
	}

	/**
	 * Reads the projects an engine gives a permission to, with each project's name
	 * and display name, level and dates, ordered by project id. A row keeps the
	 * columns' names: {@code PROJECTID}, {@code PROJECTNAME},
	 * {@code PROJECTDISPLAYNAME}, {@code PERMISSION}, {@code DATEADDED} and
	 * {@code ENDDATE}.
	 *
	 * @param engineId the engine
	 * @return one row for each project
	 */
	static List<Map<String, Object>> getEngineProjects(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(PROJECT_ID));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTNAME"));
		qs.addSelector(new QueryColumnSelector("PROJECT__PROJECTDISPLAYNAME"));
		qs.addSelector(new QueryColumnSelector(PERMISSION));
		qs.addSelector(new QueryColumnSelector(DATE_ADDED));
		qs.addSelector(new QueryColumnSelector(END_DATE));
		qs.addRelation("PROJECTENGINEPERMISSION", "PROJECT", "left.outer.join");
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(ENGINE_ID, "==", engineId));
		qs.addOrderBy(new QueryColumnOrderBySelector(PROJECT_ID));
		return QueryExecutionUtility.flushRsToMap(securityDb, qs);
	}

	/**
	 * Reads every permission an engine gives a project that is in effect now, for
	 * the check the engine proxy runs on each call. A permission that has ended is
	 * left out, and so is one with no level.
	 *
	 * @return for each engine id, the level of each project id it gives one to
	 */
	static Map<String, Map<String, AccessPermissionEnum>> getPermissionsInEffect() {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(ENGINE_ID));
		qs.addSelector(new QueryColumnSelector(PROJECT_ID));
		qs.addSelector(new QueryColumnSelector(PERMISSION));
		qs.addExplicitFilter(AbstractSecurityUtils.getUnexpiredFilter(END_DATE));
		Map<String, Map<String, AccessPermissionEnum>> permissions = new HashMap<>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs)) {
			while (wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				Object[] values = row.getValues();
				AccessPermissionEnum level = levelOf(values[2]);
				if (values[0] != null && values[1] != null && level != null) {
					permissions.computeIfAbsent(values[0].toString(), engineId -> new HashMap<>())
							.put(values[1].toString(), level);
				}
			}
		} catch (Exception e) {
			throw new IllegalStateException("Unable to read the permissions engines give to projects", e);
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
