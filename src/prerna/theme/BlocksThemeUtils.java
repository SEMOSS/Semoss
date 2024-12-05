package prerna.theme;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;

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

//	public static boolean editBlock(String blockId) throws SQLException {
//		String query = "";
//		
//		PreparedStatement ps = null;
//		try {
//	        ps = themeDb.getPreparedStatement(query);
//	        ps.setString(1, blockId);
//	        int rowsAffected = ps.executeUpdate();
//
//	        if (rowsAffected > 0) {
//	            return true;
//	        } else {
//	            throw new IllegalArgumentException("Block ID not found");
//	        }
//	    } catch (SQLException | IllegalArgumentException e) {
//	        classLogger.error(Constants.STACKTRACE, e);
//	        return false;
//	    } finally {
//	        ConnectionUtils.closeAllConnectionsIfPooling(themeDb, ps);
//	    }
//	}

}
