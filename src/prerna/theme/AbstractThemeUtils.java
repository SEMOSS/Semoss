package prerna.theme;

import java.io.IOException;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Pair;
import org.json.JSONObject;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.theme.BlocksThemeUtils;

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
		String[] adminThemeColNames = null;
		String[] adminThemeTypes = null;
		String[] blocksTemplateTypes = null;
		/*
		 * Currently used
		 */
		
		// ADMIN_THEME
		AbstractSqlQueryUtil queryUtil = themeDb.getQueryUtil();
		
		adminThemeColNames = new String[] { "ID", "THEME_NAME", "THEME_MAP", "IS_ACTIVE" };
		adminThemeTypes = new String[] { "varchar(255)", "varchar(255)", queryUtil.getClobDataTypeName(), queryUtil.getBooleanDataTypeName() };
		if(queryUtil.allowsIfExistsTableSyntax()) {
			themeDb.insertData(queryUtil.createTableIfNotExists(ThemeDbTable.ADMIN_THEME.getThemeDbTableName(), adminThemeColNames, adminThemeTypes));
		} else {
			if(!queryUtil.tableExists(themeDb.getConnection(), ThemeDbTable.ADMIN_THEME.getThemeDbTableName(), themeDb.getDatabase(), themeDb.getSchema())) {
				themeDb.insertData(queryUtil.createTable(ThemeDbTable.ADMIN_THEME.getThemeDbTableName(), adminThemeColNames, adminThemeTypes));
			}
		}
		
		// BLOCKS_TEMPLATE
		
		blocksTemplateTypes = BlocksThemeUtils.getThemeColTypes(queryUtil);
		if(queryUtil.allowsIfExistsTableSyntax()) {
			themeDb.insertData(queryUtil.createTableIfNotExists(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), BlocksThemeUtils.BLOCK_COLUMN_NAMES, blocksTemplateTypes));
		} else {
			if(!queryUtil.tableExists(themeDb.getConnection(), ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), themeDb.getDatabase(), themeDb.getSchema())) {
				themeDb.insertData(queryUtil.createTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(), BlocksThemeUtils.BLOCK_COLUMN_NAMES, blocksTemplateTypes));
				populateBlocksTemplateTable(BlocksThemeUtils.BLOCK_COLUMN_NAMES, blocksTemplateTypes, queryUtil);
			}
		}
			if (!BlocksThemeUtils.getBlockNames().containsAll(BlocksThemeUtils.BASE_BLOCKS)) {
				populateBlocksTemplateTable(BlocksThemeUtils.BLOCK_COLUMN_NAMES, blocksTemplateTypes, queryUtil);
			}

		// commit the changes
		themeDb.commit();
	}

	private static void populateBlocksTemplateTable(String[] blocksTemplateColNames, String[] blocksTemplateTypes,
		AbstractSqlQueryUtil queryUtil) throws SQLException {
		
			classLogger.info("Rebuilding BlocksTemplate Table");
		
			//delete the contents of the table
			themeDb.removeData("DELETE FROM " + ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName());
			
			for (Object[] entry : BlocksThemeUtils.BLOCKS_DEFAULT_ENTRIES) {
				themeDb.insertData(queryUtil.insertIntoTable(ThemeDbTable.BLOCKS_TEMPLATE.getThemeDbTableName(),
						blocksTemplateColNames, blocksTemplateTypes, entry));
			}
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
