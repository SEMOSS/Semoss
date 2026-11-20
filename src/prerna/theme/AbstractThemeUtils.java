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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
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

	private static void initialize() throws Exception {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();
		String[] adminThemeColNames = null;
		String[] adminThemeTypes = null;
		String[] blocksTableTypes = null;
		/*
		 * Currently used
		 */

		// ADMIN_THEME
		AbstractSqlQueryUtil queryUtil = themeDb.getQueryUtil();

		adminThemeColNames = new String[] { "ID", "THEME_NAME", "THEME_MAP", "IS_ACTIVE" };
		adminThemeTypes = new String[] { "varchar(255)", "varchar(255)", queryUtil.getClobDataTypeName(),
				queryUtil.getBooleanDataTypeName() };
		if (queryUtil.allowsIfExistsTableSyntax()) {
			themeDb.insertData(queryUtil.createTableIfNotExists(ThemeDbTable.ADMIN_THEME.getThemeDbTableName(),
					adminThemeColNames, adminThemeTypes));
		} else {
			if (!queryUtil.tableExists(themeDb.getConnection(), ThemeDbTable.ADMIN_THEME.getThemeDbTableName(),
					themeDb.getDatabase(), themeDb.getSchema())) {
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
			if (!queryUtil.tableExists(themeDb.getConnection(), ThemeDbTable.BLOCKS_TABLE.getThemeDbTableName(),
					themeDb.getDatabase(), themeDb.getSchema())) {
				themeDb.insertData(queryUtil.createTable(ThemeDbTable.BLOCKS_TABLE.getThemeDbTableName(),
						BlocksThemeUtils.BLOCK_COLUMN_NAMES, blocksTableTypes));
				populateBlocksTemplateTable(BlocksThemeUtils.BLOCK_COLUMN_NAMES, blocksTableTypes, queryUtil);
			}
		}
//			if (!BlocksThemeUtils.getBlockNames().containsAll(BlocksThemeUtils.BASE_BLOCKS)) {
//				populateBlocksTemplateTable(BlocksThemeUtils.BLOCK_COLUMN_NAMES, blocksTemplateTypes, queryUtil);
//			}

		// commit the changes
		themeDb.commit();
	}

	private static void populateBlocksTemplateTable(String[] blocksTemplateColNames, String[] blocksTemplateTypes,
			AbstractSqlQueryUtil queryUtil) throws Exception {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();

		classLogger.info("Rebuilding Blocks_Table Table");

		// delete the contents of the entire table
		themeDb.removeData("DELETE FROM " + ThemeDbTable.BLOCKS_TABLE.getThemeDbTableName());

//			for (Object[] entry : BlocksThemeUtils.BLOCKS_DEFAULT_ENTRIES) {
//				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TABLE.getThemeDbTableName(),
//						blocksTemplateColNames, blocksTemplateTypes, entry));
//			}
	}

	/**
	 * Determine if the theme db is present to be able to set custom themes
	 * 
	 * @return
	 */
	public static boolean isInitalized() {
		return AbstractThemeUtils.initialized;
	}

	static List<Map<String, Object>> flushRsToMap(IRawSelectWrapper wrapper) {
		List<Map<String, Object>> result = new ArrayList<>();
		while (wrapper.hasNext()) {
			IHeadersDataRow headerRow = wrapper.next();
			String[] headers = headerRow.getHeaders();
			Object[] values = headerRow.getValues();
			Map<String, Object> map = new HashMap<>();
			for (int i = 0; i < headers.length; i++) {
				if (values[i] instanceof java.sql.Clob) {
					map.put(headers[i], AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[i]));
				} else {
					map.put(headers[i], values[i]);
				}
			}
			result.add(map);
		}
		return result;
	}

}
