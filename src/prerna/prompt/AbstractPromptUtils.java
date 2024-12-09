package prerna.prompt;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
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
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class AbstractPromptUtils {
	
	private static final Logger classLogger = LogManager.getLogger(AbstractPromptUtils.class);

	static boolean initialized = false;
	static RDBMSNativeEngine promptDb;
	
	/**
	 * Only used for static references
	 */
	AbstractPromptUtils() {
		
	}
	
	public static void loadPromptDatabase() throws Exception {
		promptDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.PROMPT_DB);
		PromptOwlCreator owlCreator = new PromptOwlCreator(promptDb);
		if(owlCreator.needsRemake()) {
			owlCreator.remakeOwl();
		}
		initialize();
		initialized = true;
	}
	
	private static void initialize() throws SQLException {
		String database = promptDb.getDatabase();
		String schema = promptDb.getSchema();
		String[] colNames = null;
		String[] types = null;
		Object[] defaultValues = null;
		/*
		 * Currently used
		 */
		
		// ADMIN_THEME
		AbstractSqlQueryUtil queryUtil = promptDb.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();
		
		// PROMPT STUFF
		colNames = new String[] { "ID", "TITLE", "CONTEXT", "VERSION", "INTENT", "CREATED_BY", "DATE_CREATED", "IS_LATEST" };
		types = new String[] { "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE_NAME, INTEGER_DATATYPE_NAME, "VARCHAR(255)",  "VARCHAR(255)",
				TIMESTAMP_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME };
		if (allowIfExistsTable) {
			promptDb.insertData(queryUtil.createTableIfNotExists("PROMPT", colNames, types));
		} else {
			// see if table exists
			if (!queryUtil.tableExists(promptDb.getConnection(), "PROMPT", database, schema)) {
				// make the table
				promptDb.insertData(queryUtil.createTable("PROMPT", colNames, types));
			}
		}
		
		// PROMPTMETA
		// check if column exists
		colNames = new String[] { "PROMPT_ID", "METAKEY", "METAVALUE", "METAORDER" };
		types = new String[] { "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE_NAME, INTEGER_DATATYPE_NAME };
		if (allowIfExistsTable) {
			String sql = queryUtil.createTableIfNotExists("PROMPTMETA", colNames, types);
			classLogger.info("Running sql " + sql);
			promptDb.insertData(sql);
		} else {
			// see if table exists
			if (!queryUtil.tableExists(promptDb.getConnection(), "PROMPTMETA", database, schema)) {
				// make the table
				String sql = queryUtil.createTable("PROMPTMETA", colNames, types);
				classLogger.info("Running sql " + sql);
				promptDb.insertData(sql);
			}
		}

		if (allowIfExistsIndexs) {
			String sql = queryUtil.createIndexIfNotExists("PROMPTMETA_PROMPT_ID_INDEX", "PROMPTMETA", "PROMPT_ID");
			classLogger.info("Running sql " + sql);
			promptDb.insertData(sql);
		} else {
			// see if index exists
			if (!queryUtil.indexExists(promptDb, "PROMPTMETA_PROMPT_ID_INDEX", "PROMPTMETA", database, schema)) {
				String sql = queryUtil.createIndex("PROMPTMETA_PROMPT_ID_INDEX", "PROMPTMETA", "PROMPT_ID");
				classLogger.info("Running sql " + sql);
				promptDb.insertData(sql);
			}
		}
		
		// "ENGINEMETAKEYS", "PROJECTMETAKEYS", "INSIGHTMETAKEYS"
		List<String> metaKeyTableNames = Arrays.asList(Constants.PROMPT_METAKEYS);
		for(String tableName : metaKeyTableNames) {
			// all have the same columns and default values
			colNames = new String[] { "METAKEY", "SINGLEMULTI", "DISPLAYORDER", "DISPLAYOPTIONS", "DEFAULTVALUES"};
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", INTEGER_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(500)"};
			defaultValues = new Object[]{null, null, null, true, false};
			if(allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists(tableName, colNames, types);
				classLogger.info("Running sql " + sql);
				promptDb.insertData(sql);
			} else {
				// see if table exists
				if(!queryUtil.tableExists(promptDb.getConnection(), tableName, database, schema)) {
					// make the table
					String sql = queryUtil.createTable(tableName, colNames, types);
					classLogger.info("Running sql " + sql);
					promptDb.insertData(sql);
				}
			}
			// check all the columns we want are there
			{
				List<String> allCols = queryUtil.getTableColumns(promptDb.getConnection(), tableName, database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if(!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '" + col + "' is not present in current list of columns: " + allCols.toString());
						String addColumnSql = queryUtil.alterTableAddColumn(tableName, col, types[i]);
						classLogger.info("Running sql " + addColumnSql);
						promptDb.insertData(addColumnSql);
					}
				}
			}
			// see if there are any default values
			{
				IRawSelectWrapper wrapper = null;
				try {
					wrapper = WrapperManager.getInstance().getRawWrapper(promptDb, "select count(*) from " + tableName);
					if(wrapper.hasNext()) {
						int numrows = ((Number) wrapper.next().getValues()[0]).intValue();
						if(numrows < 6) {
							promptDb.removeData("DELETE FROM " + tableName + " WHERE 1=1");
							int order = 0;
							promptDb.insertData(queryUtil.insertIntoTable(tableName, colNames, types, new Object[]{Constants.MARKDOWN, "single", order++, "markdown", null}));
							promptDb.insertData(queryUtil.insertIntoTable(tableName, colNames, types, new Object[]{"description", "single", order++, "textarea", null}));
							promptDb.insertData(queryUtil.insertIntoTable(tableName, colNames, types, new Object[]{"tag", "multi", order++, "multi-typeahead", null}));
							promptDb.insertData(queryUtil.insertIntoTable(tableName, colNames, types, new Object[]{"domain", "multi", order++, "multi-typeahead", null}));
							promptDb.insertData(queryUtil.insertIntoTable(tableName, colNames, types, new Object[]{"data classification", "multi", order++, "select-box", "CONFIDENTIAL,FOUO,INTERNAL ONLY,IP,PII,PHI,PUBLIC,RESTRICTED"}));
							promptDb.insertData(queryUtil.insertIntoTable(tableName, colNames, types, new Object[]{"data restrictions", "multi", order++, "select-box", "CONFIDENTIAL ALLOWED,FOUO ALLOWED,INTERNAL ALLOWED,IP ALLOWED,PII ALLOWED,PHI ALLOWED,RESTRICTED ALLOWED"}));
						}
					}
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
				} finally {
					if(wrapper != null) {
						try {
							wrapper.close();
						} catch(IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
			}
		}
		
		// commit the changes
		promptDb.commit();
	}
	
	/**
	 * Determine if the theme db is present to be able to set custom themes
	 * @return
	 */
	public static boolean isInitalized() {
		return AbstractPromptUtils.initialized;
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


