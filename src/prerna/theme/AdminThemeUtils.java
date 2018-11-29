package prerna.theme;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.ds.util.RdbmsQueryBuilder;
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
	
	/**
	 * Get the active theme
	 * @return
	 */
	public static Object getActiveAdminTheme() {
		String query = "SELECT theme_map FROM ADMIN_THEME WHERE is_active=TRUE;";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(themeDb, query);
		Object retVal = flushRsToObject(wrapper);
		if(retVal == null) {
			return new HashMap();
		}
		return retVal;
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
	public String createAdminTheme(String themeName, String themeMap, boolean isActive) {
		String themeId = UUID.randomUUID().toString();
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
			return null;
		}
		
		if(isActive) {
			setActiveTheme(themeId);
		}
		return themeId;
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

		String updateQuery = "UPDATE ADMIN_THEME SET theme_name='" + themeName + "', theme_map='" + themeMap 
				+ "', is_active=" + isActive + " WHERE id='" + themeId + "';";
		try {
			themeDb.insertData(updateQuery);
			themeDb.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
		
		if(isActive) {
			setActiveTheme(themeId);
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
}
