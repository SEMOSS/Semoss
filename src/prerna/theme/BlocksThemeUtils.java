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

import java.lang.reflect.Type;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IRDBMSEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class BlocksThemeUtils extends AbstractThemeUtils {

	private static final Logger classLogger = LogManager.getLogger(BlocksThemeUtils.class);

	private static BlocksThemeUtils instance = new BlocksThemeUtils();

	private static final String BLOCK_QUERY = "INSERT INTO " + ThemeDbTable.BLOCKS_TABLE.getThemeDbTableName() + " (ID, NAME, SECTION, HOVER_TEXT, BLOCK_JSON, DATE_ADDED, IS_LATEST, CREATED_BY, BLOCK_QUERIES, BLOCK_VARIABLE) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	public static String[] BLOCK_COLUMN_NAMES = new String[] { "ID", "NAME", "SECTION", "HOVER_TEXT", "BLOCK_JSON" , "DATE_ADDED", "IS_LATEST" , "CREATED_BY", "BLOCK_QUERIES", "BLOCK_VARIABLE" };

	private BlocksThemeUtils() {

	}

	private static ThemeDbTable validateThemeDbTable(String tablename) {
		ThemeDbTable table = ThemeDbTable.valueOf(tablename);
		if (table == null || !table.equals(ThemeDbTable.BLOCKS_TABLE)) {
			throw new IllegalArgumentException("Requested table not found");
		}
		return table;
	}

	// get all blocks
	public static List<Map<String, Object>> getClientBlocks(String tableName, GenRowFilters filters)
			throws SQLException {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();
		ThemeDbTable table = validateThemeDbTable(tableName);
		final String blocksPrefix = table.getThemeDbTablePrefix();
		List<Map<String, Object>> retVal = null;

		SelectQueryStruct qs = new SelectQueryStruct();

		for (String colName : BlocksThemeUtils.BLOCK_COLUMN_NAMES) {
			qs.addSelector(new QueryColumnSelector(blocksPrefix + colName));
		}
		if (filters != null) {
			qs.mergeExplicitFilters(filters);
		}

		try {
			retVal = QueryExecutionUtility.flushRsToMap(themeDb, qs);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		if (retVal == null || retVal.isEmpty()) {
			return new ArrayList<>();
		}

		return retVal.stream().map(record -> {
			convertBlockJsonStringToJSONObject(record);
			return record.entrySet().stream()
					.collect(Collectors.toMap(entry -> entry.getKey().toLowerCase(), Map.Entry::getValue));
		}).collect(Collectors.toList());
	}

	// convert block_json field into json for output
	private static void convertBlockJsonStringToJSONObject(Map<String, Object> map) {
	    try {
	        Gson gson = new Gson();
	        Type type = new TypeToken<Map<String, Object>>() {}.getType();
	        // Convert BLOCK_JSON
	        String blockJson = (String) map.get("BLOCK_JSON");
	        if (blockJson != null) {
	            map.put("json", gson.fromJson(blockJson, type));
	        }
	        // Convert BLOCK_QUERIES
	        String blockQueries = (String) map.get("BLOCK_QUERIES");
	        if (blockQueries != null) {
	            map.put("queries", gson.fromJson(blockQueries, type));
	        }
	        // Convert BLOCK_VARIABLE
	        String blockVariable = (String) map.get("BLOCK_VARIABLE");
	        if (blockVariable != null) {
	            map.put("variable", gson.fromJson(blockVariable, type));
	        }
	       
	        map.remove("BLOCK_JSON");
	        map.remove("BLOCK_QUERIES");
	        map.remove("BLOCK_VARIABLE");
	    } catch (Exception e) {
	        throw new SemossPixelException("Error converting BLOCK_* to json object", e);
	    }
	}

	public static Map<String, Object> getBlock(String blockId, String tableName) throws SQLException {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();
		ThemeDbTable table = validateThemeDbTable(tableName);

		SelectQueryStruct qs = new SelectQueryStruct();

		for (String colName : BlocksThemeUtils.BLOCK_COLUMN_NAMES) {
			qs.addSelector(new QueryColumnSelector(table.getThemeDbTablePrefix() + colName));
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				ThemeDbTable.BLOCKS_TABLE.getThemeDbTablePrefix() + "ID", "==", blockId, PixelDataType.CONST_STRING));
		qs.addExplicitFilter(SimpleQueryFilter
				.makeColToValFilter(ThemeDbTable.BLOCKS_TABLE.getThemeDbTablePrefix() + "IS_LATEST", "==", 1));

		List<Map<String, Object>> retVal = null;
		try {
			retVal = QueryExecutionUtility.flushRsToMap(themeDb, qs);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		if (retVal == null || retVal.isEmpty()) {
			return new HashMap<>();
		}

		return retVal.get(0);
	}

	public static boolean deleteBlock(String blockId, String tableName, boolean hardDelete) throws SQLException {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();
		ThemeDbTable table = validateThemeDbTable(tableName);

		if (hardDelete) {
			String query = "DELETE FROM " + table.getThemeDbTableName() + " WHERE ID = ?";
			PreparedStatement ps = null;

			try {
				ps = themeDb.getPreparedStatement(query);
				ps.setString(1, blockId);
				int rowsAffected = ps.executeUpdate();
				return (rowsAffected > 0);
			} catch (SQLException e) {
				classLogger.error(Constants.STACKTRACE, e);
				return false;
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(themeDb, ps);
			}
		} else {
			return updateBlock(blockId);
		}
	}

	// add block function
	public static String addBlock(Map<String, Object> blockDetails) {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();

		boolean allowClob = themeDb.getQueryUtil().allowClobJavaObject();
		String blockId = UUID.randomUUID().toString();
		blockDetails.put("id", blockId);
		validateBlockDetails(blockDetails);
		insertBlock(blockDetails, allowClob, blockId);
		return blockId;
	}

	// validate the input map for required fields
	private static void validateBlockDetails(Map<String, Object> blockDetails) {
		validateString(blockDetails, "name", false, false);
		validateString(blockDetails, "section", false, false);
		validateString(blockDetails, "json", false, false);
		validateString(blockDetails, "queries", false, false);
		validateString(blockDetails, "variable", false, false);

	}

	// validate the individual fields
	private static void validateString(Map<String, Object> blockDetails, String mapKey, boolean nullable,
			boolean allowEmpty) {
		String value = null;
		try {
			value = (String) blockDetails.get(mapKey);
			value = value != null ? value.trim() : value;
			if (value == null && !nullable) {
				throw new IllegalArgumentException(mapKey + " cannot be null, when adding in a new Block");
			}
			if (value != null && value.isEmpty() && !allowEmpty) {
				throw new IllegalArgumentException(mapKey + " cannot be null, when adding in a new Block");
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		}
	}

	// insert the row into blocks_table table
	private static void insertBlock(Map<String, Object> blockDetails, boolean allowClob, String blockId) {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();
		PreparedStatement blockPS = null;
		try {
			blockPS = themeDb.getPreparedStatement(BLOCK_QUERY);
			int parameterIndex = 1;
			blockPS.setString(parameterIndex++, blockId);
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("name")));
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("section")).toUpperCase());
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("hover_text")));
			// BLOCK_JSON
			if (allowClob) {
				Clob toclob = themeDb.getConnection().createClob();
				toclob.setString(1, String.valueOf(blockDetails.get("json")));
				blockPS.setClob(parameterIndex++, toclob);
			} else {
				blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("json")));
			}
			blockPS.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
			blockPS.setBoolean(parameterIndex++, true);
			// blockPS.setBoolean(parameterIndex++, true); // IS_LATEST
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("created_by"))); // CREATED_BY
			
			// BLOCK_QUERIES
		     if (allowClob) {
		         Clob queryClob = themeDb.getConnection().createClob();
		         queryClob.setString(1, String.valueOf(blockDetails.get("queries")));
		         blockPS.setClob(parameterIndex++, queryClob);
		     } else {
		         blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("queries")));
		     }
			
			// BLOCK_VARIABLE
		     if (allowClob) {
		         Clob varClob = themeDb.getConnection().createClob();
		         varClob.setString(1, String.valueOf(blockDetails.get("variable")));
		         blockPS.setClob(parameterIndex++, varClob);
		     } else {
		         blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("variable")));
		     }
			blockPS.executeUpdate();
			if (!blockPS.getConnection().getAutoCommit()) {
				blockPS.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(themeDb, null, blockPS, null);
		}
	}

	// update the row in blocks_table associated with the ID to be latest
	// (If not soft delete)
	private static boolean updateBlock(String blockId) {
		IRDBMSEngine themeDb = SystemEngineRegistry.getThemesDb();
		String[] colToUpdate = { "IS_LATEST" };
		String[] whereCol = { "ID" };
		String promptPermissionQuery = themeDb.getQueryUtil().createUpdatePreparedStatementString(
				ThemeDbTable.BLOCKS_TABLE.getThemeDbTableName(), colToUpdate, whereCol);
		PreparedStatement ps = null;
		try {
			ps = themeDb.getPreparedStatement(promptPermissionQuery);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, false);
			ps.setString(parameterIndex++, blockId);
			int rowsAffected = ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
			return (rowsAffected > 0);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(themeDb, ps);
		}

	}

	public static String[] getThemeColTypes(AbstractSqlQueryUtil queryUtil) {
		return new String[] { "varchar(255)", "varchar(255)", "varchar(255)", "varchar(500)", queryUtil.getClobDataTypeName(), queryUtil.getDateWithTimeDataType(), queryUtil.getBooleanDataTypeName(),"varchar(255)", queryUtil.getClobDataTypeName(), queryUtil.getClobDataTypeName() };
	}

}
