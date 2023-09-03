package prerna.theme;

import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;

public class AdminThemeUtils extends AbstractThemeUtils {
	
	private static final Logger classLogger = LogManager.getLogger(AdminThemeUtils.class);

	private static AdminThemeUtils instance = new AdminThemeUtils();

	private AdminThemeUtils() {

	}

	public static AdminThemeUtils getInstance(User user) {
		if (user == null) {
			return null;
		}
		if (SecurityAdminUtils.userIsAdmin(user)) {
			return instance;
		}
		return null;
	}

	/**
	 * Get the active theme
	 * 
	 * @return
	 */
	public static Object getActiveAdminTheme() {
		if (themeDb == null) {
			return new HashMap();
		}

		String query = "SELECT id, theme_name, theme_map, is_active FROM ADMIN_THEME WHERE is_active=TRUE;";

		List<Map<String, Object>> retVal = null;
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(themeDb, query);
			retVal = flushRsToMap(wrapper);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		if (retVal == null || retVal.isEmpty()) {
			return new HashMap();
		}

		return retVal.get(0);
	}

	/*
	 * all other methods should be on the instance
	 * so that we cannot bypass security easily
	 */

	/**
	 * Get all the admin level themes
	 * 
	 * @return
	 */
	public List<Map<String, Object>> getAdminThemes() {
		String query = "SELECT id, theme_name, theme_map, is_active FROM ADMIN_THEME;";
		IRawSelectWrapper wrapper;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(themeDb, query);
			return flushRsToMap(wrapper);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return new ArrayList<>();
	}

	/**
	 * Set the active admin level theme This means setting the is_active boolean to
	 * true for 1 theme while the previous true value to false;
	 * 
	 * @param themeId
	 * @return
	 */
	public boolean setActiveTheme(String themeId) {
		String query = "UPDATE ADMIN_THEME SET IS_ACTIVE=FALSE WHERE IS_ACTIVE=TRUE; UPDATE ADMIN_THEME SET IS_ACTIVE=TRUE WHERE ID='"
				+ themeId + "'";
		try {
			themeDb.insertData(query);
			themeDb.commit();
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		}
		return true;
	}

	/**
	 * Set the active admin level theme to false so the UI resets to the base theme
	 * 
	 * @return
	 */
	public boolean setAllThemesInactive() {
		String query = "UPDATE ADMIN_THEME SET IS_ACTIVE=FALSE WHERE IS_ACTIVE=TRUE;";
		try {
			themeDb.insertData(query);
			themeDb.commit();
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		}
		return true;
	}

	/**
	 * Insert a new admin level theme
	 * 
	 * @param themeId
	 * @param themeName
	 * @param themeMap
	 * @param isActive
	 * @return
	 */
	public String createAdminTheme(String themeName, String themeMap, boolean isActive) {
		String themeId = UUID.randomUUID().toString();
		
		PreparedStatement ps = null;
		try {
			ps = themeDb.getPreparedStatement("INSERT INTO ADMIN_THEME (id, theme_name, theme_map, is_active) VALUES (?,?,?,?)");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, themeId);
			ps.setString(parameterIndex++, themeName);
			themeDb.getQueryUtil().handleInsertionOfClob(ps.getConnection(), ps, themeMap, parameterIndex++, new Gson());
			ps.setBoolean(parameterIndex++, isActive);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (UnsupportedEncodingException | SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return null;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(themeDb, ps);
		}

		if (isActive) {
			setActiveTheme(themeId);
		}
		return themeId;
	}

	/**
	 * Edit an existing admin level theme
	 * 
	 * @param themeId
	 * @param themeName
	 * @param themeMap
	 * @param isActive
	 * @return
	 */
	public boolean editAdminTheme(String themeId, String themeName, String themeMap, boolean isActive) {
		
		PreparedStatement ps = null;
		try {
			ps = themeDb.getPreparedStatement("UPDATE ADMIN_THEME SET theme_name=?, theme_map=?, is_active=? WHERE id=? ");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, themeName);
			themeDb.getQueryUtil().handleInsertionOfClob(ps.getConnection(), ps, themeMap, parameterIndex++, new Gson());
			ps.setBoolean(parameterIndex++, isActive);
			ps.setString(parameterIndex++, themeId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (UnsupportedEncodingException | SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(themeDb, ps);
		}

		if (isActive) {
			setActiveTheme(themeId);
		}
		return true;
	}

	/**
	 * Delete a admin level theme
	 * 
	 * @param themeId
	 * @return
	 */
	public boolean deleteAdminTheme(String themeId) {
		
		PreparedStatement ps = null;
		try {
			ps = themeDb.getPreparedStatement("DELETE FROM ADMIN_THEME WHERE id=?");
			ps.setString(1, themeId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			return false;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(themeDb, ps);
		}
		
		return true;
	}
}
