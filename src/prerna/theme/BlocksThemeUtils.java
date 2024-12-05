package prerna.theme;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
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
			"Text H5", "Text H6", "Text P", "Text P Italics", "Mermaid", "Vega", "Grid", "Bar Chart",
			"Grouped Bar Chart", "Pie Chart", "Radial Plot", "Line Chart", "Area Chart", "Area Chart with Gradient",
			"Scatter Plot", "General Mermaid", "Class Diagram", "Sequence Diagram", "State Diagram",
			"Entity Relationship Diagram", "User Journey", "Gantt", "Pie Chart", "Quadrant Chart",
			"Requirement Diagram", "Git Diagram", "C4 Diagram", "Mindmap", "Timeline", "Sankey", "XY Chart",
			"Block Diagram"));

	private static final String BLOCK_QUERY = "INSERT INTO BLOCKS_TEMPLATE (ID, NAME, SECTION, IMAGE, HOVER_IMAGE, BLOCK_JSON, CLASSIFICATION, IS_DELETABLE, DATE_ADDED, IS_LATEST)"
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	private BlocksThemeUtils() {

	}

	private static void validateThemeDbTable(ThemeDbTable table) {
		if (table == null || table.equals(ThemeDbTable.ADMIN_THEME)) {
			throw new IllegalArgumentException("Requested table not found");
		}
	}

	public static ArrayList<String> getBlockNames() throws SQLException {

		String query = "SELECT bt.NAME FROM BLOCKS_TEMPLATE bt ";

		ArrayList<String> namesInTable = new ArrayList<>();

		PreparedStatement ps = null;

		try {
			ps = themeDb.getPreparedStatement(query);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				String name = rs.getString("NAME");
				namesInTable.add(name);
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return null;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(themeDb, ps);
		}
		return namesInTable;
	}

	public static Object getBlocks(String tableName) throws SQLException {
		ThemeDbTable table = ThemeDbTable.valueOf(tableName);
		validateThemeDbTable(table);

		final String blocksPrefix = table.getThemeDbTablePrefix();

		SelectQueryStruct qs = new SelectQueryStruct();

		for (String colName : AbstractThemeUtils.blocksTemplateColNames) {
			if (!colName.equals("JSON_1")) {
				qs.addSelector(new QueryColumnSelector(blocksPrefix + colName));
			}
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

//		String query = "SELECT * FROM " + table.getThemeDbTableName();
//		
//		Map<String, Map<String, Object>> output = new HashMap<>();
//		
//		PreparedStatement ps = null;
//		
//		try {
//			ps = themeDb.getPreparedStatement(query);
//			ResultSet rs = ps.executeQuery();
//			while (rs.next()) {
//				Map<String, Object> innerQuery = new HashMap<>();
//				innerQuery.put("id", rs.getString("ID"));
//				innerQuery.put("name", rs.getString("NAME"));
//				innerQuery.put("section", rs.getString("SECTION"));
//				innerQuery.put("image", rs.getString("IMAGE"));
//				innerQuery.put("hover_image", rs.getString("HOVER_IMAGE"));
//				innerQuery.put("json", rs.getString("JSON"));
//				innerQuery.put("classification", rs.getString("CLASSIFICATION"));
//				output.put(rs.getString("ID"), innerQuery);
//			}
//		} catch (SQLException e) {
//			classLogger.error(Constants.STACKTRACE, e);
//			return null;
//		} finally {
//			ConnectionUtils.closeAllConnectionsIfPooling(themeDb, ps);
//		}
//		
//		return output;
	}

	public static Map<String, Object> getBlock(String blockId, String tableName) throws SQLException {
		ThemeDbTable table = ThemeDbTable.valueOf(tableName);
		validateThemeDbTable(table);
		String query = "SELECT * FROM " + table.getThemeDbTableName() + " WHERE ID = ?";

		Map<String, Object> output = new HashMap<>();
		PreparedStatement ps = null;
		try {
			ps = themeDb.getPreparedStatement(query);
			ps.setString(1, blockId);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				output.put("id", rs.getString("ID"));
				output.put("name", rs.getString("NAME"));
				output.put("section", rs.getString("SECTION"));
				output.put("image", rs.getString("IMAGE"));
				output.put("hover_image", rs.getString("HOVER_IMAGE"));
				output.put("json", rs.getString("JSON"));
				output.put("classification", rs.getString("CLASSIFICATION"));
			} else {
				throw new IllegalArgumentException("Block ID not found");
			}
		} catch (SQLException | IllegalArgumentException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return null;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(themeDb, ps);
		}

		return output;
	}

	public static boolean deleteBlock(String blockId, String tableName) throws SQLException {
		ThemeDbTable table = ThemeDbTable.valueOf(tableName);
		validateThemeDbTable(table);
		String query = "DELETE FROM " + table.getThemeDbTableName() + " WHERE ID = ?";
		PreparedStatement ps = null;

		try {
			ps = themeDb.getPreparedStatement(query);
			ps.setString(1, blockId);
			int rowsAffected = ps.executeUpdate();

			if (rowsAffected > 0) {
				return true;
			} else {
				throw new IllegalArgumentException("Block ID not found");
			}
		} catch (SQLException | IllegalArgumentException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(themeDb, ps);
		}
	}

	//add block function
	public static void addBlock(Map<String, Object> blockDetails) {
		boolean allowClob = themeDb.getQueryUtil().allowClobJavaObject();
		String blockId = UUID.randomUUID().toString();
		validateBlockDetails(blockDetails);
		insertBlock(blockDetails, allowClob, blockId);
		
		
	}
	
	//edit block function
	public static void editBlock(Map<String, Object> editDetails) {
		boolean allowClob = themeDb.getQueryUtil().allowClobJavaObject();
		String blockId = (String) editDetails.get("id");
		validateBlockDetails(editDetails);
		updateBlock(blockId);
		insertBlock(editDetails, allowClob, blockId);
	}
	
	//validate the input map for required fields
	private static void validateBlockDetails(Map<String, Object> blockDetails) {
		validateString(blockDetails, "name", false, false);
		validateString(blockDetails, "section", false, false);
		validateString(blockDetails, "image", false, false);
		validateString(blockDetails, "block_json", false, false);
	}
	
	//validate the individual fields
	private static void validateString(Map<String, Object> blockDetails, String mapKey, boolean nullable, boolean allowEmpty) {
		String value = null;
		try {
			value = (String) blockDetails.get(mapKey);
			value = value != null ? value.trim(): value;
			
			if(value == null && !nullable) {
				throw new IllegalArgumentException(mapKey + " cannot be null, when adding in a new Block");
			}
			
			if(value != null && value.isEmpty() && !allowEmpty) {
				throw new IllegalArgumentException(mapKey + " cannot be null, when adding in a new Block");
			}
			
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		}
	}
	
	//insert the row into blocks_template table
	private static void insertBlock(Map<String, Object> blockDetails, boolean allowClob, String blockId) {
		PreparedStatement blockPS = null;
		try {
			blockPS = themeDb.getPreparedStatement(BLOCK_QUERY);
			int parameterIndex = 1;
			blockPS.setString(parameterIndex++, blockId);
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("name")));
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("section")).toUpperCase());
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("image")).toUpperCase());
			blockPS.setString(parameterIndex++, String.valueOf(blockDetails.get("hover_image")).toUpperCase());
			if(allowClob) {
				Clob toclob = themeDb.getConnection().createClob();
				toclob.setString(1,  String.valueOf(blockDetails.get("block_json")));
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
			
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(themeDb, null, blockPS, null);
		}
	}
	
	//update the row in blocks_template associated with the ID to be latest (similar to soft delete)
	private static void updateBlock(String blockId) {
		String[] colToUpdate = {"IS_LATEST"};
		String[] whereCol = {"ID"};
		String promptPermissionQuery = themeDb.getQueryUtil().createUpdatePreparedStatementString("BLOCKS_TEMPLATE", colToUpdate, whereCol);

		PreparedStatement ps = null;
		
		try {
			ps = themeDb.getPreparedStatement(promptPermissionQuery);
			int parameterIndex = 1;
			ps.setBoolean(parameterIndex++, false);
			ps.setString(parameterIndex++, blockId);
			ps.executeUpdate();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(themeDb, ps);
		}
		
	}


}
