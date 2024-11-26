package prerna.theme;

import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;

public class BlocksThemeUtils extends AbstractThemeUtils {

	private static final Logger classLogger = LogManager.getLogger(BlocksThemeUtils.class);

	private static BlocksThemeUtils instance = new BlocksThemeUtils();

	private BlocksThemeUtils() {

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
//				output.put("json", rs.getClob("JSON").getCharacterStream());
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
