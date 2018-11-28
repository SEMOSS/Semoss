package prerna.theme;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.IOUtils;

import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
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
						e.printStackTrace();
						throw new IllegalArgumentException("Error occured trying to read theme map");
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
					e.printStackTrace();
				}
			}
		}
		return obj;
	}
}
