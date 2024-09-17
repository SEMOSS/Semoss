package prerna.theme;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public abstract class AbstractThemeUtils {

	private static final Logger classLogger = LogManager.getLogger(AbstractThemeUtils.class);

	static boolean initialized = false;
	static RDBMSNativeEngine themeDb;
	
	/**
	 * Only used for static references
	 */
	AbstractThemeUtils() {
		
	}
	
	public static void loadThemingDatabase() throws Exception {
		themeDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.THEMING_DB);
		ThemeOwlCreator owlCreator = new ThemeOwlCreator(themeDb);
		if(owlCreator.needsRemake()) {
			owlCreator.remakeOwl();
		}
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
		AbstractSqlQueryUtil queryUtil = themeDb.getQueryUtil();
		
		colNames = new String[] { "ID", "THEME_NAME", "THEME_MAP", "IS_ACTIVE" };
		types = new String[] { "varchar(255)", "varchar(255)", queryUtil.getClobDataTypeName(), queryUtil.getBooleanDataTypeName() };
		if(queryUtil.allowsIfExistsTableSyntax()) {
			themeDb.insertData(queryUtil.createTableIfNotExists("ADMIN_THEME", colNames, types));
		} else {
			if(!queryUtil.tableExists(themeDb.getConnection(), "ADMIN_THEME", themeDb.getDatabase(), themeDb.getSchema())) {
				themeDb.insertData(queryUtil.createTable("ADMIN_THEME", colNames, types));
			}
		}

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
	
	static List<Map<String, Object>> flushRsToMap(IRawSelectWrapper wrapper) {
		List<Map<String, Object>> result = new Vector<Map<String, Object>>();
		while(wrapper.hasNext()) {
			IHeadersDataRow headerRow = wrapper.next();
			String[] headers = headerRow.getHeaders();
			Object[] values = headerRow.getValues();
			Map<String, Object> map = new HashMap<String, Object>();
			for(int i = 0; i < headers.length; i++) {
				if(values[i] instanceof java.sql.Clob) {
					try {
						map.put(headers[i], IOUtils.toString(((java.sql.Clob) values[i]).getAsciiStream()));
					} catch (IOException | SQLException e) {
						classLogger.error(Constants.STACKTRACE, e);
						throw new IllegalArgumentException("Error occurred trying to read theme map");
					}
				} else {
					map.put(headers[i], values[i]);
				}
			}
			result.add(map);
		}
		return result;
	}
	
	static Object flushRsToObject(IRawSelectWrapper wrapper) {
		Object obj = null;
		if(wrapper.hasNext()) {
			obj = wrapper.next().getValues()[0];
			if(obj instanceof java.sql.Clob) {
				try {
					obj = IOUtils.toString(((java.sql.Clob) obj).getAsciiStream());
				} catch (IOException | SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return obj;
	}
}
