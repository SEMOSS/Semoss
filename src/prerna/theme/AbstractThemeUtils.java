package prerna.theme;

import java.sql.SQLException;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.Constants;
import prerna.util.Utility;

public abstract class AbstractThemeUtils {

	static boolean initialized = false;
	static RDBMSNativeEngine themeDb;

	
	/**
	 * Only used for static references
	 */
	AbstractThemeUtils() {
		
	}
	
	public static void loadThemingDatabase() throws SQLException {
		themeDb = (RDBMSNativeEngine) Utility.getEngine(Constants.THEMING_DB);
		initialize();
		initialized = true;
	}

	private static void initialize() throws SQLException {
		String[] colNames = null;
		String[] types = null;
		/*
		 * Currently used
		 */
		
		// ADMIN_THEME
		colNames = new String[] { "id", "theme_name", "theme_map", "is_active" };
		types = new String[] { "varchar(255)", "varchar(255)", "clob", "boolean" };
		themeDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("ADMIN_THEME", colNames, types));

		// commit the changes
		themeDb.commit();
	}
	
	/**
	 * Determine if the theme db is present to be able to set custom themes
	 * @return
	 */
	public static boolean isInitalized() {
		return AbstractThemeUtils.initialized;
	}
}
