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

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class BlocksThemeUtils extends AbstractThemeUtils {

	private static final Logger classLogger = LogManager.getLogger(BlocksThemeUtils.class);

	private static BlocksThemeUtils instance = new BlocksThemeUtils();

	private static final String BLOCK_QUERY = "INSERT INTO " + ThemeDbTable.BLOCKS_TABLE.getThemeDbTableName() + " (ID, NAME, SECTION, HOVER_TEXT, BLOCK_JSON, DATE_ADDED, IS_LATEST, CREATED_BY) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

	public static String[] BLOCK_COLUMN_NAMES = new String[] { "ID", "NAME", "SECTION", "HOVER_TEXT", "BLOCK_JSON" , "DATE_ADDED", "IS_LATEST" , "CREATED_BY" };

	
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
	public static List<Map<String, Object>> getClientBlocks(String tableName, GenRowFilters filters) throws SQLException {
		ThemeDbTable table = validateThemeDbTable(tableName);
		final String blocksPrefix = table.getThemeDbTablePrefix();
		List<Map<String, Object>> retVal = null;
		
		SelectQueryStruct qs = new SelectQueryStruct();
		
		for (String colName : BlocksThemeUtils.BLOCK_COLUMN_NAMES) {
			qs.addSelector(new QueryColumnSelector(blocksPrefix + colName));
		}
		if(filters != null) {
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
		
		return retVal.stream()
		        .map(record -> {
		            convertBlockJsonStringToJSONObject(record);
		            return record.entrySet().stream()
		                    .collect(Collectors.toMap(
		                        entry -> entry.getKey().toLowerCase(),
		                        Map.Entry::getValue
		                    ));
		        })
		        .collect(Collectors.toList());
	}
	// convert block_json field into json for output
	private static void convertBlockJsonStringToJSONObject(Map<String, Object> map) {
	    try {
	        String blockJson = (String) map.get("BLOCK_JSON");
	        Gson gson = new Gson();
	        Type type = new TypeToken<Map<String, Object>>() {}.getType();
	        map.put("json", gson.fromJson(blockJson, type));
	        map.remove("BLOCK_JSON");
	    } catch (Exception e) {
	        throw new SemossPixelException("Error converting BLOCK_JSON to json object", e);
	    }
	}
	 

	public static Map<String, Object> getBlock(String blockId, String tableName) throws SQLException {
		ThemeDbTable table = validateThemeDbTable(tableName);

		SelectQueryStruct qs = new SelectQueryStruct();
		
		for (String colName : BlocksThemeUtils.BLOCK_COLUMN_NAMES) {
			qs.addSelector(new QueryColumnSelector(table.getThemeDbTablePrefix() + colName));
		}
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(ThemeDbTable.BLOCKS_TABLE.getThemeDbTablePrefix() + "ID", "==",
						blockId, PixelDataType.CONST_STRING));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(ThemeDbTable.BLOCKS_TABLE.getThemeDbTablePrefix() + "IS_LATEST", "==", 1));

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
		PreparedStatement blockPS = null;
		try {
			blockPS = themeDb.getPreparedStatement(BLOCK_QUERY);
			int parameterIndex = 1;
			blockPS.setString(parameterIndex++, blockId);
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("name")));
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("section")).toUpperCase());
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("hover_text")));
			if (allowClob) {
				Clob toclob = themeDb.getConnection().createClob();
				toclob.setString(1, String.valueOf(blockDetails.get("json")));
				blockPS.setClob(parameterIndex++, toclob);
			} else {
				blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("json")));
			}
			blockPS.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
			blockPS.setBoolean(parameterIndex++, true);
			//blockPS.setBoolean(parameterIndex++, true); // IS_LATEST
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("created_by"))); // CREATED_BY
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
		String[] colToUpdate = { "IS_LATEST" };
		String[] whereCol = { "ID" };
		String promptPermissionQuery = themeDb.getQueryUtil().createUpdatePreparedStatementString(ThemeDbTable.BLOCKS_TABLE.getThemeDbTableName(),
				colToUpdate, whereCol);
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
		return new String[] { "varchar(255)", "varchar(255)", "varchar(255)", "varchar(500)", queryUtil.getClobDataTypeName(), queryUtil.getDateWithTimeDataType(), queryUtil.getBooleanDataTypeName(),"varchar(255)" };
	}

}
