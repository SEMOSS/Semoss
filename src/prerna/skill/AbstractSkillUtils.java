package prerna.skill;

import java.sql.Connection;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.SystemEngineRegistry;
import prerna.util.sql.AbstractSqlQueryUtil;

public abstract class AbstractSkillUtils {

	private static final Logger classLogger = LogManager.getLogger(AbstractSkillUtils.class);

	static Gson skillGson = new GsonBuilder().disableHtmlEscaping().create();

	/**
	 * Only used for static references
	 */
	AbstractSkillUtils() {

	}

	public static void loadSkillDatabase() throws Exception {
		IRDBMSEngine skillDb = SystemEngineRegistry.getSkillDb();
		SkillOwlCreator owlCreator = new SkillOwlCreator(skillDb);
		if (owlCreator.needsRemake()) {
			owlCreator.remakeOwl();
		}
		initialize();
	}

	public static void initialize() throws Exception {
		IRDBMSEngine skillDb = SystemEngineRegistry.getSkillDb();
		String database = skillDb.getDatabase();
		String schema = skillDb.getSchema();
		Connection conn = skillDb.getConnection();
		try {
			String[] colNames = null;
			String[] types = null;

			AbstractSqlQueryUtil queryUtil = skillDb.getQueryUtil();
			boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
			boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();
			final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
			final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
			final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
			final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();

			// SKILL
			colNames = new String[] { "SKILL_ID", "SKILL_NAME", "DESCRIPTION", "CONTENT", "VERSION", "TAGS",
					"OWNER_ID", "PROJECT_ID", "CREATED_AT", "UPDATED_AT", "IS_ACTIVE" };
			types = new String[] { "VARCHAR(255)", "VARCHAR(255)", CLOB_DATATYPE_NAME, CLOB_DATATYPE_NAME,
					INTEGER_DATATYPE_NAME, CLOB_DATATYPE_NAME, "VARCHAR(255)", "VARCHAR(255)",
					TIMESTAMP_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME, BOOLEAN_DATATYPE_NAME };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("SKILL", colNames, types);
				classLogger.info("Running sql {}", sql);
				skillDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "SKILL", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("SKILL", colNames, types);
					classLogger.info("Running sql {}", sql);
					skillDb.insertData(sql);
				}
			}
			// UPDATE TO CHECK ALL COLUMNS!
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "SKILL", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("SKILL", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						skillDb.insertData(addColumnSql);
					}
				}
			}
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("SKILL_SKILL_ID_INDEX", "SKILL", "SKILL_ID");
				classLogger.info("Running sql {}", sql);
				skillDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("SKILL_PROJECT_ID_INDEX", "SKILL", "PROJECT_ID");
				classLogger.info("Running sql {}", sql);
				skillDb.insertData(sql);

				sql = queryUtil.createIndexIfNotExists("SKILL_OWNER_ID_INDEX", "SKILL", "OWNER_ID");
				classLogger.info("Running sql {}", sql);
				skillDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(skillDb, "SKILL_SKILL_ID_INDEX", "SKILL", database, schema)) {
					String sql = queryUtil.createIndex("SKILL_SKILL_ID_INDEX", "SKILL", "SKILL_ID");
					classLogger.info("Running sql {}", sql);
					skillDb.insertData(sql);
				}
				if (!queryUtil.indexExists(skillDb, "SKILL_PROJECT_ID_INDEX", "SKILL", database, schema)) {
					String sql = queryUtil.createIndex("SKILL_PROJECT_ID_INDEX", "SKILL", "PROJECT_ID");
					classLogger.info("Running sql {}", sql);
					skillDb.insertData(sql);
				}
				if (!queryUtil.indexExists(skillDb, "SKILL_OWNER_ID_INDEX", "SKILL", database, schema)) {
					String sql = queryUtil.createIndex("SKILL_OWNER_ID_INDEX", "SKILL", "OWNER_ID");
					classLogger.info("Running sql {}", sql);
					skillDb.insertData(sql);
				}
			}

			// SKILL_VERSION
			colNames = new String[] { "SKILL_ID", "VERSION", "CONTENT", "CHANGE_NOTES", "CREATED_AT" };
			types = new String[] { "VARCHAR(255)", INTEGER_DATATYPE_NAME, CLOB_DATATYPE_NAME,
					CLOB_DATATYPE_NAME, TIMESTAMP_DATATYPE_NAME };
			if (allowIfExistsTable) {
				String sql = queryUtil.createTableIfNotExists("SKILL_VERSION", colNames, types);
				classLogger.info("Running sql {}", sql);
				skillDb.insertData(sql);
			} else {
				// see if table exists
				if (!queryUtil.tableExists(conn, "SKILL_VERSION", database, schema)) {
					// make the table
					String sql = queryUtil.createTable("SKILL_VERSION", colNames, types);
					classLogger.info("Running sql {}", sql);
					skillDb.insertData(sql);
				}
			}
			// UPDATE TO CHECK ALL COLUMNS!
			{
				List<String> allCols = queryUtil.getTableColumns(conn, "SKILL_VERSION", database, schema);
				for (int i = 0; i < colNames.length; i++) {
					String col = colNames[i];
					if (!allCols.contains(col) && !allCols.contains(col.toLowerCase())) {
						classLogger.info("Column '{}' is not present in current list of columns: {}", col, allCols);
						String addColumnSql = queryUtil.alterTableAddColumn("SKILL_VERSION", col, types[i]);
						classLogger.info("Running sql {}", addColumnSql);
						skillDb.insertData(addColumnSql);
					}
				}
			}
			if (allowIfExistsIndexs) {
				String sql = queryUtil.createIndexIfNotExists("SKILL_VERSION_SKILL_ID_INDEX", "SKILL_VERSION", "SKILL_ID");
				classLogger.info("Running sql {}", sql);
				skillDb.insertData(sql);
			} else {
				// see if index exists
				if (!queryUtil.indexExists(skillDb, "SKILL_VERSION_SKILL_ID_INDEX", "SKILL_VERSION", database, schema)) {
					String sql = queryUtil.createIndex("SKILL_VERSION_SKILL_ID_INDEX", "SKILL_VERSION", "SKILL_ID");
					classLogger.info("Running sql {}", sql);
					skillDb.insertData(sql);
				}
			}
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(skillDb, conn, null, null);
		}
	}

}
