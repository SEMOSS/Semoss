package prerna.theme;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;

public class AdminThemeUtils extends AbstractThemeUtils {

	private static AdminThemeUtils instance = new AdminThemeUtils();
	
	private AdminThemeUtils() {
		
	}
	
	public static AdminThemeUtils getInstance(User user) {
		// if no security
		// do whatever you want!
		if(!AbstractSecurityUtils.securityEnabled()) {
			return instance;
		}
		if(user == null) {
			return null;
		}
		if(SecurityAdminUtils.userIsAdmin(user)) {
			return instance;
		}
		return null;
	}
	
	/*
	 * all other methods should be on the instance
	 * so that we cannot bypass security easily
	 */
	
	/**
	 * Get all the admin level themes
	 * @return
	 */
	public List<Map<String, Object>> getAdminThemes() {
		String query = "SELECT id, theme_name, theme_map, is_active FROM ADMIN_THEME;";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(themeDb, query);
		return flushRsToMap(wrapper);
	}
	
	/**
	 * Set the active admin level theme
	 * This means setting the is_active boolean to true for 1 theme while the previous true value to false;
	 * @param themeId
	 * @return
	 */
	public boolean setActiveTheme(String themeId) {
		String query = "UPDATE ADMIN_THEME SET IS_ACTIVE=FALSE WHERE IS_ACTIVE=TRUE; UPDATE ADMIN_THEME SET IS_ACTIVE=TRUE WHERE ID='" + themeId + "'";
		try {
			themeDb.insertData(query);
			themeDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * Insert a new admin level theme
	 * @param themeId
	 * @param themeName
	 * @param themeMap
	 * @param isActive
	 * @return
	 */
	public boolean createAdminTheme(String themeId, String themeName, String themeMap, boolean isActive) {
		themeName = RdbmsQueryBuilder.escapeForSQLStatement(themeName);
		themeMap = RdbmsQueryBuilder.escapeForSQLStatement(themeMap);

		String[] colNames = new String[] { "id", "theme_name", "theme_map", "is_active" };
		String[] types = new String[] { "varchar(255)", "varchar(255)", "clob", "boolean" };
		Object[] data = new Object[]{themeId, themeName, themeMap, isActive};
		String insertQuery = RdbmsQueryBuilder.makeInsert("ADMIN_THEME", colNames, types, data);
		try {
			themeDb.insertData(insertQuery);
			themeDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * Edit an existing admin level theme
	 * @param themeId
	 * @param themeName
	 * @param themeMap
	 * @param isActive
	 * @return
	 */
	public boolean editAdminTheme(String themeId, String themeName, String themeMap, boolean isActive) {
		themeName = RdbmsQueryBuilder.escapeForSQLStatement(themeName);
		themeMap = RdbmsQueryBuilder.escapeForSQLStatement(themeMap);

		String updateQuery = "UPDATE ADMIN_THEME SET theme_name='" + themeName + "' AND theme_map='" + themeMap 
				+ "' AND is_active=" + isActive + " WHERE id='" + themeId + "';";
		try {
			themeDb.insertData(updateQuery);
			themeDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * Delete a admin level theme
	 * @param themeId
	 * @return
	 */
	public boolean deleteAdminTheme(String themeId) {
		String deleteQuery = "DELETE FROM ADMIN_THEME WHERE id='" + themeId + "'";
		try {
			themeDb.removeData(deleteQuery);
			themeDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	static List<Map<String, Object>> flushRsToMap(IRawSelectWrapper wrapper) {
		List<Map<String, Object>> result = new Vector<Map<String, Object>>();
		while(wrapper.hasNext()) {
			IHeadersDataRow headerRow = wrapper.next();
			String[] headers = headerRow.getHeaders();
			Object[] values = headerRow.getValues();
			Map<String, Object> map = new HashMap<String, Object>();
			for(int i = 0; i < headers.length; i++) {
				map.put(headers[i], values[i]);
			}
			result.add(map);
		}
		return result;
	}
}
