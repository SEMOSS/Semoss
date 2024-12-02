package prerna.theme;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.ConnectionUtils;
import prerna.util.Constants;

public class BlocksThemeUtils extends AbstractThemeUtils {

	private static final Logger classLogger = LogManager.getLogger(BlocksThemeUtils.class);

	private static BlocksThemeUtils instance = new BlocksThemeUtils();

	private BlocksThemeUtils() {

	}
	
	public static Map<String, Map<String, Object>> getBlocks() throws SQLException {
		
		String query = "SELECT * FROM BLOCKS_TEMPLATE";
		
		Map<String, Map<String, Object>> output = new HashMap<>();
		
		PreparedStatement ps = null;
		
		try {
			ps = themeDb.getPreparedStatement(query);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				Map<String, Object> innerQuery = new HashMap<>();
				innerQuery.put("id", rs.getString("ID"));
				innerQuery.put("name", rs.getString("NAME"));
				innerQuery.put("section", rs.getString("SECTION"));
				innerQuery.put("image", rs.getString("IMAGE"));
				innerQuery.put("hover_image", rs.getString("HOVER_IMAGE"));
				innerQuery.put("json", rs.getString("JSON"));
				innerQuery.put("classification", rs.getString("CLASSIFICATION"));
				output.put(rs.getString("ID"), innerQuery);
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return null;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(themeDb, ps);
		}
		
		return output;
	}
	
	public static Map<String, Object> getBlock(String blockId) throws SQLException {
		
		String query = "SELECT * FROM BLOCKS_TEMPLATE WHERE ID = ?";
		
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

}
