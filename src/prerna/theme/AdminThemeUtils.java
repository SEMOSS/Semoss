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
package prerna.theme;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;

public class AdminThemeUtils extends AbstractThemeUtils {

	private static final Logger classLogger = LogManager.getLogger(AdminThemeUtils.class);

	private static AdminThemeUtils instance = new AdminThemeUtils();

	private AdminThemeUtils() {

	}

	public static AdminThemeUtils getInstance(User user) {
		if (user == null) {
			return null;
		}
		if (SecurityAdminUtils.userIsAdmin(user)) {
			return instance;
		}
		return null;
	}

	/**
	 * Get the active theme. A failure reading the theme database is logged and
	 * gives an empty map, so the UI still loads without a theme.
	 *
	 * @return the active theme's row, or an empty map when no theme is active or
	 *         the read fails
	 */
	public static Object getActiveAdminTheme() {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();
		if (themeDb == null) {
			return new HashMap<>();
		}

		final String ADMIN_THEME_PREFIX = "ADMIN_THEME__";
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(ADMIN_THEME_PREFIX + "ID"));
		qs.addSelector(new QueryColumnSelector(ADMIN_THEME_PREFIX + "THEME_NAME"));
		qs.addSelector(new QueryColumnSelector(ADMIN_THEME_PREFIX + "THEME_MAP"));
		qs.addSelector(new QueryColumnSelector(ADMIN_THEME_PREFIX + "IS_ACTIVE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(ADMIN_THEME_PREFIX + "IS_ACTIVE", "==", true,
				PixelDataType.BOOLEAN));

		List<Map<String, Object>> retVal;
		try {
			retVal = QueryExecutionUtility.flushRsToMap(themeDb, qs);
		} catch (Exception e) {
			classLogger.error("Failed to read the active admin theme", e);
			return new HashMap<>();
		}
		if (retVal == null || retVal.isEmpty()) {
			return new HashMap<>();
		}

		return retVal.get(0);
	}

	/*
	 * all other methods should be on the instance so that we cannot bypass security
	 * easily
	 */

	/**
	 * Get all the admin level themes. A failure reading the theme database is
	 * logged and gives an empty list, so the UI still loads.
	 *
	 * @return the admin themes, or an empty list when the read fails
	 */
	public List<Map<String, Object>> getAdminThemes(int limit, int offset) {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();
		final String ADMIN_THEME_PREFIX = "ADMIN_THEME__";
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(ADMIN_THEME_PREFIX + "ID"));
		qs.addSelector(new QueryColumnSelector(ADMIN_THEME_PREFIX + "THEME_NAME"));
		qs.addSelector(new QueryColumnSelector(ADMIN_THEME_PREFIX + "THEME_MAP"));
		qs.addSelector(new QueryColumnSelector(ADMIN_THEME_PREFIX + "IS_ACTIVE"));
		if (limit > 0) {
			qs.setLimit(limit);
		}
		if (offset > 0) {
			qs.setOffSet(offset);
		}

		try {
			return QueryExecutionUtility.flushRsToMap(themeDb, qs);
		} catch (Exception e) {
			classLogger.error("Failed to read the admin themes", e);
			return new ArrayList<>();
		}
	}

	/**
	 * Set the active admin level theme This means setting the is_active boolean to
	 * true for 1 theme while the previous true value to false.
	 *
	 * <p>
	 * Both updates run in one write on the shared executor, so the previous theme
	 * is deactivated only together with the new one being activated. The playground
	 * theme cache is refreshed after that write commits; a failed write leaves the
	 * cache as it was.
	 *
	 * @param themeId id of the theme to activate
	 * @return true when the active theme was saved; false when the write failed
	 *         (error logged)
	 */
	public boolean setActiveTheme(String themeId) {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();
		try {
			QueryExecutionUtility.write(themeDb, connection -> {
				QueryExecutionUtility.executeUpdate(connection, "UPDATE ADMIN_THEME SET IS_ACTIVE=? WHERE IS_ACTIVE=?",
						ps -> {
							int parameterIndex = 1;
							ps.setBoolean(parameterIndex++, false);
							ps.setBoolean(parameterIndex++, true);
						});
				QueryExecutionUtility.executeUpdate(connection, "UPDATE ADMIN_THEME SET IS_ACTIVE=? WHERE ID=?", ps -> {
					int parameterIndex = 1;
					ps.setBoolean(parameterIndex++, true);
					ps.setString(parameterIndex++, themeId);
				});
				return null;
			});
		} catch (Exception e) {
			classLogger.error("Failed to set admin theme {} as the active theme", themeId, e);
			return false;
		}
		PlaygroundThemeUtils.refreshCacheFromActiveTheme();
		return true;
	}

	/**
	 * Set the active admin level theme to false so the UI resets to the base theme
	 * 
	 * @return
	 */
	public boolean setAllThemesInactive() {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();
		try {
			QueryExecutionUtility.executeUpdate(themeDb, "UPDATE ADMIN_THEME SET IS_ACTIVE=? WHERE IS_ACTIVE=?", ps -> {
				int parameterIndex = 1;
				ps.setBoolean(parameterIndex++, false);
				ps.setBoolean(parameterIndex++, false);
			});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to set all admin themes inactive", e);
			return false;
		}

		return true;
	}

	/**
	 * Insert a new admin level theme. When the theme is active, it is then made the
	 * only active theme by {@link #setActiveTheme(String)} in a separate write; a
	 * failure there is logged and the inserted theme is kept.
	 * 
	 * @param themeId
	 * @param themeName
	 * @param themeMap
	 * @param isActive
	 * @return
	 */
	public String createAdminTheme(String themeName, String themeMap, boolean isActive) {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();
		String themeId = UUID.randomUUID().toString();

		try {
			QueryExecutionUtility.executeUpdate(themeDb,
					"INSERT INTO ADMIN_THEME (ID, THEME_NAME, THEME_MAP, IS_ACTIVE) VALUES (?,?,?,?)", ps -> {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, themeId);
						ps.setString(parameterIndex++, themeName);
						themeDb.getQueryUtil().setNullableLargeText(ps, parameterIndex++, themeMap);
						ps.setBoolean(parameterIndex++, isActive);
					});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to create admin theme {} with id {}", themeName, themeId, e);
			return null;
		}

		if (isActive) {
			setActiveTheme(themeId);
		}
		return themeId;
	}

	/**
	 * Edit an existing admin level theme. When the theme is active, it is then made
	 * the only active theme by {@link #setActiveTheme(String)} in a separate write,
	 * and the playground theme cache is refreshed again afterward.
	 * 
	 * @param themeId
	 * @param themeName
	 * @param themeMap
	 * @param isActive
	 * @return
	 */
	public boolean editAdminTheme(String themeId, String themeName, String themeMap, boolean isActive) {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();
		try {
			QueryExecutionUtility.executeUpdate(themeDb,
					"UPDATE ADMIN_THEME SET THEME_NAME=?, THEME_MAP=?, IS_ACTIVE=? WHERE ID=?", ps -> {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, themeName);
						themeDb.getQueryUtil().setNullableLargeText(ps, parameterIndex++, themeMap);
						ps.setBoolean(parameterIndex++, isActive);
						ps.setString(parameterIndex++, themeId);
					});
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to edit admin theme {} with name {}", themeId, themeName, e);
			return false;
		}

		if (isActive) {
			setActiveTheme(themeId);
			PlaygroundThemeUtils.refreshCacheFromActiveTheme();
		}
		return true;
	}

	/**
	 * Delete a admin level theme
	 * 
	 * @param themeId
	 * @return
	 */
	public boolean deleteAdminTheme(String themeId) {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();

		try {
			QueryExecutionUtility.executeUpdate(themeDb, "DELETE FROM ADMIN_THEME WHERE ID=?",
					ps -> ps.setString(1, themeId));
		} catch (RuntimeException e) {
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to delete admin theme {}", themeId, e);
			return false;
		}

		return true;
	}
}
