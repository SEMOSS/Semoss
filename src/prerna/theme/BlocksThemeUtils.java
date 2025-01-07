package prerna.theme;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;

public class BlocksThemeUtils extends AbstractThemeUtils {

	private static final Logger classLogger = LogManager.getLogger(BlocksThemeUtils.class);

	private static BlocksThemeUtils instance = new BlocksThemeUtils();

	public static final ArrayList<String> BASE_BLOCKS = new ArrayList<String>(Arrays.asList("Audio Player", "Button",
			"Checkbox", "Input", "Select", "Upload", "Container", "Progress", "Iframe", "PDF Viewer", "Image", "Logs",
			"Toggle Button", "Link", "Markdown", "HTML", "Text H1 styled", "Text H1", "Text H2", "Text H3", "Text H4",
			"Text H5", "Text H6", "Text P", "Text P Italics", "Compare LLMs", "Mermaid", "Vega", "Grid", "Bar Chart",
			"Grouped Bar Chart", "Pie Chart", "Radial Plot", "Line Chart", "Area Chart", "Area Chart with Gradient",
			"Scatter Plot", "General Mermaid", "Class Diagram", "Sequence Diagram", "State Diagram",
			"Entity Relationship Diagram", "User Journey", "Gantt", "Pie Chart", "Quadrant Chart",
			"Requirement Diagram", "Git Diagram", "C4 Diagram", "Mindmap", "Timeline", "Sankey", "XY Chart",
			"Block Diagram"));

	private static final String BLOCK_QUERY = "INSERT INTO BLOCKS_TEMPLATE (ID, NAME, SECTION, IMAGE, HOVER_IMAGE, BLOCK_JSON, CLASSIFICATION, IS_DELETABLE, DATE_ADDED, IS_LATEST) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private BlocksThemeUtils() {

	}

	
	private static void validateThemeDbTable(ThemeDbTable table) {
		if (table == null || table.equals(ThemeDbTable.ADMIN_THEME)) {
			throw new IllegalArgumentException("Requested table not found");
		}
	}

	
	public static List<String> getBlockNames() throws SQLException {

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTablePrefix() + "NAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(
				ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTablePrefix() + "IS_DELETABLE", "==", false,
				PixelDataType.BOOLEAN));

		List<Map<String, Object>> queryRes = null;
		try {
			queryRes = QueryExecutionUtility.flushRsToMap(themeDb, qs);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		if (queryRes == null || queryRes.isEmpty()) {
			return new ArrayList<>();
		}

		List<String> output = queryRes.parallelStream().map(mapObj -> (String) mapObj.get("NAME"))
				.collect(Collectors.toList());

		return output;
	}
	
	public static Object getThemeData(String tableName, GenRowFilters filters) throws SQLException {
		ThemeDbTable table = ThemeDbTable.valueOf(tableName);
		validateThemeDbTable(table);

		final String blocksPrefix = table.getThemeDbTablePrefix();

		SelectQueryStruct qs = new SelectQueryStruct();

		for (String colName : AbstractThemeUtils.blocksTemplateColNames) {
			qs.addSelector(new QueryColumnSelector(blocksPrefix + colName));
		}
		
		if(filters != null) {
			qs.mergeExplicitFilters(filters);
		}
		
		List<Map<String, Object>> retVal = null;
		try {
			retVal = QueryExecutionUtility.flushRsToMap(themeDb, qs);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		if (retVal == null || retVal.isEmpty()) {
			return new HashMap<>();
		}

		return retVal;
	}

	
	public static Map<String, Object> getBlock(String blockId, String tableName) throws SQLException {
		ThemeDbTable table = ThemeDbTable.valueOf(tableName);
		validateThemeDbTable(table);

		SelectQueryStruct qs = new SelectQueryStruct();

		for (String colName : AbstractThemeUtils.blocksTemplateColNames) {
			qs.addSelector(new QueryColumnSelector(table.getThemeDbTablePrefix() + colName));
		}
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTablePrefix() + "ID", "==",
						blockId, PixelDataType.CONST_STRING));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTablePrefix() + "IS_LATEST", "==", 1));

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
		ThemeDbTable table = ThemeDbTable.valueOf(tableName);
		validateThemeDbTable(table);

		try {
			if (!isDeletable(blockId, table)) {
				throw new SecurityException("Not allowed to delete this block");
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		}

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
	
	
	private static boolean isDeletable(String blockId, ThemeDbTable table) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(table.getThemeDbTablePrefix() + "IS_DELETABLE"));
		qs.addExplicitFilter(
				SimpleQueryFilter.makeColToValFilter(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTablePrefix() + "ID", "==",
						blockId, PixelDataType.CONST_STRING));
		List<Map<String, Object>> retVal = null;
		try {
			retVal = QueryExecutionUtility.flushRsToMap(themeDb, qs);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		if (retVal == null || retVal.isEmpty()) {
			throw new IllegalArgumentException("Block Id does not exist");
		}

		return (boolean) retVal.get(0).get("IS_DELETABLE");
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

	
	// edit block function 
	public static boolean editBlock(Map<String, Object> editDetails, String tableName) {
		ThemeDbTable table = ThemeDbTable.valueOf(tableName);
		boolean allowClob = themeDb.getQueryUtil().allowClobJavaObject();
		String blockId = (String) editDetails.get("id");
		try {
			if (!isDeletable(blockId, table)) {
				throw new SecurityException("Not allowed to delete this block");
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		}
		updateBlock(blockId);
		insertBlock(editDetails, allowClob, blockId);
		return true;
	}

	
	// validate the input map for required fields
	private static void validateBlockDetails(Map<String, Object> blockDetails) {
		validateString(blockDetails, "id", false, false);
		validateString(blockDetails, "name", false, false);
		validateString(blockDetails, "section", false, false);
		validateString(blockDetails, "image", false, false);
		validateString(blockDetails, "block_json", false, false);
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

	
	// insert the row into blocks_template table
	private static void insertBlock(Map<String, Object> blockDetails, boolean allowClob, String blockId) {
		PreparedStatement blockPS = null;
		try {
			blockPS = themeDb.getPreparedStatement(BLOCK_QUERY);
			int parameterIndex = 1;
			blockPS.setString(parameterIndex++, blockId);
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("name")));
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("section")).toUpperCase());
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("image")).toUpperCase());
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("image")).toUpperCase());
			if (allowClob) {
				Clob toclob = themeDb.getConnection().createClob();
				toclob.setString(1, String.valueOf(blockDetails.get("block_json")));
				blockPS.setClob(parameterIndex++, toclob);
			} else {
				blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("block_json")));
			}
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("classification")).toUpperCase());
			blockPS.setBoolean(parameterIndex++, true);
			blockPS.setTimestamp(parameterIndex++, Utility.getCurrentSqlTimestampUTC());
			blockPS.setBoolean(parameterIndex++, true);
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

	
	// update the row in blocks_template associated with the ID to be latest
	// (similar to soft delete)
	private static boolean updateBlock(String blockId) {
		String[] colToUpdate = { "IS_LATEST" };
		String[] whereCol = { "ID" };
		String promptPermissionQuery = themeDb.getQueryUtil().createUpdatePreparedStatementString("BLOCKS_TEMPLATE",
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

}
