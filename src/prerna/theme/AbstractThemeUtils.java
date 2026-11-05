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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;

public abstract class AbstractThemeUtils {

	private static final Logger classLogger = LogManager.getLogger(AbstractThemeUtils.class);

	static boolean initialized = false;

	/**
	 * Only used for static references
	 */
	AbstractThemeUtils() {

	}

	public static void loadThemingDatabase() throws Exception {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();
		ThemeOwlCreator owlCreator = new ThemeOwlCreator(themeDb.getQueryUtil());
		if (owlCreator.needsRemake(themeDb)) {
			owlCreator.remakeOwl(themeDb);
		}
		initialize();
		initialized = true;
		PlaygroundThemeUtils.refreshCacheFromActiveTheme();
	}

	/**
	 * Creates the theme tables when they do not exist. Each DDL statement runs and
	 * commits on its own through the engine, with no transaction across statements,
	 * since several dialects commit DDL implicitly. Table lookups run in their own
	 * reads on the shared executor.
	 *
	 * @throws Exception when a table cannot be looked up or created
	 */
	private static void initialize() throws Exception {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();
		String[] adminThemeColNames = null;
		String[] adminThemeTypes = null;
		String[] blocksTableTypes = null;

		// ADMIN_THEME
		AbstractSqlQueryUtil queryUtil = themeDb.getQueryUtil();

		adminThemeColNames = new String[] { "ID", "THEME_NAME", "THEME_MAP", "IS_ACTIVE" };
		adminThemeTypes = new String[] { "varchar(255)", "varchar(255)", queryUtil.getClobDataTypeName(),
				queryUtil.getBooleanDataTypeName() };
		if (queryUtil.allowsIfExistsTableSyntax()) {
			themeDb.insertData(queryUtil.createTableIfNotExists(ThemeDbTable.ADMIN_THEME.getThemeDbTableName(),
					adminThemeColNames, adminThemeTypes));
		} else {
			if (!tableExists(themeDb, ThemeDbTable.ADMIN_THEME.getThemeDbTableName())) {
				themeDb.insertData(queryUtil.createTable(ThemeDbTable.ADMIN_THEME.getThemeDbTableName(),
						adminThemeColNames, adminThemeTypes));
			}
		}

		// BLOCKS_TABLE
		blocksTableTypes = BlocksThemeUtils.getThemeColTypes(queryUtil);
		if (queryUtil.allowsIfExistsTableSyntax()) {
			themeDb.insertData(queryUtil.createTableIfNotExists(ThemeDbTable.BLOCKS_TABLE.getThemeDbTableName(),
					BlocksThemeUtils.BLOCK_COLUMN_NAMES, blocksTableTypes));
		} else {
			if (!tableExists(themeDb, ThemeDbTable.BLOCKS_TABLE.getThemeDbTableName())) {
				themeDb.insertData(queryUtil.createTable(ThemeDbTable.BLOCKS_TABLE.getThemeDbTableName(),
						BlocksThemeUtils.BLOCK_COLUMN_NAMES, blocksTableTypes));
				populateBlocksTemplateTable(BlocksThemeUtils.BLOCK_COLUMN_NAMES, blocksTableTypes, queryUtil);
			}
		}
	}

	/**
	 * Checks whether a theme table exists, in one read on the shared executor.
	 *
	 * @param themeDb   the theme database
	 * @param tableName table to look up
	 * @return true when the table exists; false when it does not or the lookup
	 *         query fails
	 * @throws Exception when the connection cannot be acquired or released
	 */
	private static boolean tableExists(IRDBMSEngine themeDb, String tableName) throws Exception {
		return QueryExecutionUtility.read(themeDb, connection -> themeDb.getQueryUtil().tableExists(connection,
				tableName, themeDb.getDatabase(), themeDb.getSchema()));
	}

	/**
	 * Clears the blocks table so it can be filled with the template blocks. The
	 * delete is a data migration stage and runs in one write on the shared
	 * executor.
	 *
	 * @param blocksTemplateColNames blocks table columns
	 * @param blocksTemplateTypes    blocks table column types
	 * @param queryUtil              dialect of the theme database
	 * @throws Exception when the rows cannot be deleted
	 */
	private static void populateBlocksTemplateTable(String[] blocksTemplateColNames, String[] blocksTemplateTypes,
			AbstractSqlQueryUtil queryUtil) throws Exception {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();

		classLogger.info("Rebuilding Blocks_Table Table");

		// delete the contents of the entire table
		QueryExecutionUtility.executeUpdate(themeDb, "DELETE FROM " + ThemeDbTable.BLOCKS_TABLE.getThemeDbTableName(),
				QueryExecutionUtility.NO_PARAMETERS);
	}

	/**
	 * Determine if the theme db is present to be able to set custom themes
	 * 
	 * @return
	 */
	public static boolean isInitalized() {
		return AbstractThemeUtils.initialized;
	}

}
