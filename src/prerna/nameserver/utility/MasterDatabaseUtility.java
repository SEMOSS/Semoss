package prerna.nameserver.utility;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.OwlSeparatePixelFromConceptual;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.QueryExecutionUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class MasterDatabaseUtility {

	private static final Logger logger = LogManager.getLogger(MasterDatabaseUtility.class);

	// -----------------------------------------   RDBMS CALLS ---------------------------------------

	public static void initLocalMaster() throws Exception {
		IRDBMSEngine database = (IRDBMSEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
		LocalMasterOwlCreator owlCreator = new LocalMasterOwlCreator(database);
		if(owlCreator.needsRemake()) {
			owlCreator.remakeOwl();
		}
		// Update OWL
		OwlSeparatePixelFromConceptual.fixOwl(database.getSmssProp());
		
		Connection conn  = null;
		try {
			conn = database.makeConnection();
			executeInitLocalMaster(database, conn);
		} catch(SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw e;
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(database, conn, null, null);
		}
	}
	
	private static void executeInitLocalMaster(IRDBMSEngine engine, Connection conn) throws SQLException {
		String [] colNames = null;
		String [] types = null;

		String database = engine.getDatabase();
		String schema = engine.getSchema();
		AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
		boolean allowIfExistsTable = queryUtil.allowsIfExistsTableSyntax();
		boolean allowIfExistsIndexs = queryUtil.allowIfExistsIndexSyntax();
		
		final String BOOLEAN_DATATYPE = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE = queryUtil.getDateWithTimeDataType();
		final String CLOB_DATATYPE = queryUtil.getClobDataTypeName();
		
		// since i have major changes
		requireRemakeAndAlter(engine, conn, queryUtil, database, schema, allowIfExistsTable);
		
		// engine table
		colNames = new String[]{"ID", "ENGINENAME", "MODIFIEDDATE", "TYPE"};
		types = new String[]{"varchar(255)", "varchar(255)", TIMESTAMP_DATATYPE, "varchar(255)"};
		if(allowIfExistsTable) {
			String sql = queryUtil.createTableIfNotExists("ENGINE", colNames, types);
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if table exists
			if(!queryUtil.tableExists(engine, "ENGINE", database, schema)) {
				// make the table
				String sql = queryUtil.createTable("ENGINE", colNames, types);
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}
		// add index
		if(allowIfExistsIndexs) {
			String sql = queryUtil.createIndexIfNotExists("ENGINE_ID_INDEX", "ENGINE", "ID");
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if index exists
			if(!queryUtil.indexExists(engine, "ENGINE_ID_INDEX", "ENGINE", database, schema)) {
				String sql =  queryUtil.createIndex("ENGINE_ID_INDEX", "ENGINE", "ID");
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}

		// engine concept table
		colNames = new String[]{"ENGINE", "PARENTSEMOSSNAME", "SEMOSSNAME", "PARENTPHYSICALNAME", "PARENTPHYSICALNAMEID", "PHYSICALNAME", 
				"PHYSICALNAMEID", "PARENTLOCALCONCEPTID", "LOCALCONCEPTID", "IGNORE_DATA", "PK", "ORIGINAL_TYPE", 
				"PROPERTY_TYPE", "ADDITIONAL_TYPE"};
		types = new String[]{"varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)", 
				"varchar(255)", "varchar(255)", "varchar(255)", BOOLEAN_DATATYPE, BOOLEAN_DATATYPE, "varchar(255)", 
				"varchar(255)", "varchar(255)"};
		if(allowIfExistsTable) {
			String sql =  queryUtil.createTableIfNotExists("ENGINECONCEPT", colNames, types);
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if table exists
			if(!queryUtil.tableExists(engine, "ENGINECONCEPT", database, schema)) {
				// make the table
				String sql =  queryUtil.createTable("ENGINECONCEPT", colNames, types);
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}
		// add index
		{
			// 2021-08-11
			if(allowIfExistsIndexs) {
				String sql = queryUtil.dropIndexIfExists("ENGINE_CONCEPT_ENGINE_LOCAL_CONCEPT_ID", "ENGINECONCEPT");
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			} else {
				if(queryUtil.indexExists(engine, "ENGINE_CONCEPT_ENGINE_LOCAL_CONCEPT_ID", "ENGINECONCEPT", database, schema)) {
					String sql = queryUtil.dropIndex("ENGINE_CONCEPT_ENGINE_LOCAL_CONCEPT_ID", "ENGINECONCEPT");
					logger.info("Running sql " + sql);
					executeSql(conn, sql);
				}
			}
		}
		if(allowIfExistsIndexs) {
			List<String> iCols = new ArrayList<>();
			iCols.add("ENGINE");
			iCols.add("LOCALCONCEPTID");
			
			String sql = queryUtil.createIndexIfNotExists("ENGINECONCEPT_ENGINE_LOCALCONCEPTID_INDEX", "ENGINECONCEPT", iCols);
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
			sql = queryUtil.createIndexIfNotExists("ENGINECONCEPT_PHYSICALNAMEID_INDEX", "ENGINECONCEPT", "PHYSICALNAMEID");
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if index exists
			if(!queryUtil.indexExists(engine, "ENGINECONCEPT_ENGINE_LOCALCONCEPTID_INDEX", "ENGINECONCEPT", database, schema)) {
				List<String> iCols = new ArrayList<>();
				iCols.add("ENGINE");
				iCols.add("LOCALCONCEPTID");
				
				String sql = queryUtil.createIndex("ENGINECONCEPT_ENGINE_LOCALCONCEPTID_INDEX", "ENGINECONCEPT", iCols);
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
			if(!queryUtil.indexExists(engine, "ENGINECONCEPT_PHYSICALNAMEID_INDEX", "ENGINECONCEPT", database, schema)) {
				String sql = queryUtil.createIndex("ENGINECONCEPT_PHYSICALNAMEID_INDEX", "ENGINECONCEPT", "PHYSICALNAMEID");
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}

		// concept table
		colNames = new String[]{"LOCALCONCEPTID", "CONCEPTUALNAME", "LOGICALNAME", "DOMAINNAME", "GLOBALID"};
		types = new String[]{"varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)"};
		if(allowIfExistsTable) {
			String sql = queryUtil.createTableIfNotExists("CONCEPT", colNames, types);
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if table exists
			if(!queryUtil.tableExists(engine, "CONCEPT", database, schema)) {
				// make the table
				String sql = queryUtil.createTable("CONCEPT", colNames, types);
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}
		// add index
		if(allowIfExistsIndexs) {
			String sql = queryUtil.createIndexIfNotExists("CONCEPT_ID_INDEX", "CONCEPT", "LOCALCONCEPTID");
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if index exists
			if(!queryUtil.indexExists(engine, "CONCEPT_ID_INDEX", "CONCEPT", database, schema)) {
				String sql = queryUtil.createIndex("CONCEPT_ID_INDEX", "CONCEPT", "LOCALCONCEPTID");
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}

		// relation table
		colNames = new String[]{"ID", "SOURCEID", "TARGETID", "GLOBALID"};
		types = new String[]{"varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)"};
		if(allowIfExistsTable) {
			String sql = queryUtil.createTableIfNotExists("RELATION", colNames, types);
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if table exists
			if(!queryUtil.tableExists(engine, "RELATION", database, schema)) {
				// make the table
				String sql = queryUtil.createTable("RELATION", colNames, types);
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}
		// add index
		if(allowIfExistsIndexs) {
			String sql = queryUtil.createIndexIfNotExists("RELATION_TARGETID_INDEX", "RELATION", "TARGETID");
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
			
			sql = queryUtil.createIndexIfNotExists("RELATION_SOURCEID_INDEX", "RELATION", "SOURCEID");
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if index exists
			if(!queryUtil.indexExists(engine, "RELATION_TARGETID_INDEX", "RELATION", database, schema)) {
				String sql = queryUtil.createIndex("RELATION_TARGETID_INDEX", "RELATION", "TARGETID");
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
			if(!queryUtil.indexExists(engine, "RELATION_SOURCEID_INDEX", "RELATION", database, schema)) {
				String sql = queryUtil.createIndex("RELATION_SOURCEID_INDEX", "RELATION", "SOURCEID");
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}

		// engine relation table
		colNames = new String[]{"ENGINE", "RELATIONID", "INSTANCERELATIONID", "SOURCECONCEPTID", "TARGETCONCEPTID", "SOURCEPROPERTY", "TARGETPROPERTY", "RELATIONNAME"};
		types = new String[]{"varchar(255)", "varchar(255)","varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)"};
		if(allowIfExistsTable) {
			String sql = queryUtil.createTableIfNotExists("ENGINERELATION", colNames, types);
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if table exists
			if(!queryUtil.tableExists(engine, "ENGINERELATION", database, schema)) {
				// make the table
				String sql = queryUtil.createTable("ENGINERELATION", colNames, types);
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}
		// add index
		if(allowIfExistsIndexs) {
			String sql = queryUtil.createIndexIfNotExists("ENGINERELATION_ENGINE_INDEX", "ENGINERELATION", "ENGINE");
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
			
			sql = queryUtil.createIndexIfNotExists("ENGINERELATION_TARGETCONCEPTID_INDEX", "ENGINERELATION", "TARGETCONCEPTID");
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
			
			sql = queryUtil.createIndexIfNotExists("ENGINERELATION_SOURCECONCEPTID_INDEX", "ENGINERELATION", "SOURCECONCEPTID");
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if index exists
			if(!queryUtil.indexExists(engine, "ENGINERELATION_ENGINE_INDEX", "ENGINERELATION", database, schema)) {
				String sql = queryUtil.createIndex("ENGINERELATION_ENGINE_INDEX", "ENGINERELATION", "ENGINE");
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
			if(!queryUtil.indexExists(engine, "ENGINERELATION_TARGETCONCEPTID_INDEX", "ENGINERELATION", database, schema)) {
				String sql = queryUtil.createIndex("ENGINERELATION_TARGETCONCEPTID_INDEX", "ENGINERELATION", "TARGETCONCEPTID");
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
			if(!queryUtil.indexExists(engine, "ENGINERELATION_SOURCECONCEPTID_INDEX", "ENGINERELATION", database, schema)) {
				String sql = queryUtil.createIndex("ENGINERELATION_SOURCECONCEPTID_INDEX", "ENGINERELATION", "SOURCECONCEPTID");
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}

		// kv store
		colNames = new String[]{"K","V"};
		types = new String[]{"varchar(800)", "varchar(800)"};
		if(allowIfExistsTable) {
			String sql = queryUtil.createTableIfNotExists("KVSTORE", colNames, types);
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if table exists
			if(!queryUtil.tableExists(engine, "KVSTORE", database, schema)) {
				// make the table
				String sql = queryUtil.createTable("KVSTORE", colNames, types);
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}

		// concept metadata
		updateMetadataTable(engine, conn, queryUtil, Constants.CONCEPT_METADATA_TABLE, database, schema);
		colNames = new String[] {Constants.LM_PHYSICAL_NAME_ID, Constants.LM_META_KEY, Constants.LM_META_VALUE };
		types = new String[] { "varchar(255)", "varchar(800)", CLOB_DATATYPE };
		if(allowIfExistsTable) {
			String sql = queryUtil.createTableIfNotExists(Constants.CONCEPT_METADATA_TABLE, colNames, types);
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if table exists
			if(!queryUtil.tableExists(engine, Constants.CONCEPT_METADATA_TABLE, database, schema)) {
				// make the table
				String sql = queryUtil.createTable(Constants.CONCEPT_METADATA_TABLE, colNames, types);
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}
		// add index
		if(allowIfExistsIndexs) {
			String sql = queryUtil.createIndexIfNotExists("CONCEPTMETADATA_KEY_INDEX", Constants.CONCEPT_METADATA_TABLE, Constants.LM_META_KEY);
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
			
			sql = queryUtil.createIndexIfNotExists("CONCEPTMETADATA_PHYSICALNAMEID_INDEX", Constants.CONCEPT_METADATA_TABLE, Constants.LM_PHYSICAL_NAME_ID);
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if index exists
			if(!queryUtil.indexExists(engine, "CONCEPTMETADATA_KEY_INDEX", Constants.CONCEPT_METADATA_TABLE, database, schema)) {
				String sql = queryUtil.createIndex("CONCEPTMETADATA_KEY_INDEX", Constants.CONCEPT_METADATA_TABLE, Constants.LM_META_KEY );
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
			if(!queryUtil.indexExists(engine, "CONCEPTMETADATA_PHYSICALNAMEID_INDEX", Constants.CONCEPT_METADATA_TABLE,  database, schema)) {
				String sql = queryUtil.createIndex("CONCEPTMETADATA_PHYSICALNAMEID_INDEX", Constants.CONCEPT_METADATA_TABLE, Constants.LM_PHYSICAL_NAME_ID);
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}

		// x-ray config
		colNames = new String[]{"FILENAME", "CONFIG" };
		types = new String[]{"varchar(800)", CLOB_DATATYPE };
		if(allowIfExistsTable) {
			String sql = queryUtil.createTableIfNotExists("XRAYCONFIGS", colNames, types);
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if table exists
			if(!queryUtil.tableExists(engine, "XRAYCONFIGS", database, schema)) {
				// make the table
				String sql = queryUtil.createTable("XRAYCONFIGS", colNames, types);
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}

		// bitly
		colNames = new String[]{"FANCY", "EMBED"};
		types = new String[]{"varchar(255)", "varchar(8000)" };
		if(allowIfExistsTable) {
			String sql = queryUtil.createTableIfNotExists("BITLY", colNames, types);
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if table exists
			if(!queryUtil.tableExists(engine, "BITLY", database, schema)) {
				// make the table
				String sql = queryUtil.createTable("BITLY", colNames, types);
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}
		
		
		// metamodel position
		colNames = new String[] {"ENGINEID", "TABLENAME", "XPOS", "YPOS"};
		types = new String[] {"VARCHAR(255)", "VARCHAR(255)", "FLOAT", "FLOAT"};
		if(allowIfExistsTable) {
			String sql = queryUtil.createTableIfNotExists("METAMODELPOSITION", colNames, types);
			logger.info("Running sql " + sql);
			executeSql(conn, sql);
		} else {
			// see if table exists
			if(!queryUtil.tableExists(engine, "METAMODELPOSITION", database, schema)) {
				// make the table
				String sql = queryUtil.createTable("METAMODELPOSITION", colNames, types);
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
		}
		
		// this is just because of previous errors
		// TODO: remove this after a few builds when its no longer needed
		// added on 2020-06-04
		executeSql(conn, "update concept set LOGICALNAME = lower (LOGICALNAME)");
	}

	@Deprecated
	private static void requireRemakeAndAlter(IRDBMSEngine engine, 
			Connection conn, 
			AbstractSqlQueryUtil queryUtil, 
			String database, 
			String schema, 
			boolean allowIfExistsTable) throws SQLException {
		boolean require = false;
		if(!queryUtil.tableExists(conn, "ENGINECONCEPT", database, schema)) {
			require = true;
		} else {
			List<String> allColumns = queryUtil.getTableColumns(conn, "ENGINECONCEPT", database, schema);
			if( !(allColumns.contains("PARENTSEMOSSNAME") || allColumns.contains("parentsemossname")) ) {
				require = true;
			}
		}
		
		// just delete and let the other methods remake the tables
		if(require) {
			if(allowIfExistsTable) {
				executeSql(conn, queryUtil.dropTableIfExists("ENGINE"));
				executeSql(conn, queryUtil.dropTableIfExists("ENGINECONCEPT"));
				executeSql(conn, queryUtil.dropTableIfExists("CONCEPT"));
				executeSql(conn, queryUtil.dropTableIfExists("CONCEPTMETADATA"));
				executeSql(conn, queryUtil.dropTableIfExists("ENGINERELATION"));
				executeSql(conn, queryUtil.dropTableIfExists("RELATION"));
				executeSql(conn, queryUtil.dropTableIfExists("KVSTORE"));
				executeSql(conn, queryUtil.dropTableIfExists("METAMODELPOSITION"));
			} else {
				if(queryUtil.tableExists(engine, "ENGINE", database, schema)) {
					executeSql(conn, queryUtil.dropTable("ENGINE"));
				}
				if(queryUtil.tableExists(engine, "ENGINECONCEPT", database, schema)) {
					executeSql(conn, queryUtil.dropTable("ENGINECONCEPT"));
				}
				if(queryUtil.tableExists(engine, "CONCEPT", database, schema)) {
					executeSql(conn, queryUtil.dropTable("CONCEPT"));
				}
				if(queryUtil.tableExists(engine, "CONCEPTMETADATA", database, schema)) {
					executeSql(conn, queryUtil.dropTable("CONCEPTMETADATA"));
				}
				if(queryUtil.tableExists(engine, "ENGINERELATION", database, schema)) {
					executeSql(conn, queryUtil.dropTable("ENGINERELATION"));
				}
				if(queryUtil.tableExists(engine, "RELATION", database, schema)) {
					executeSql(conn, queryUtil.dropTable("RELATION"));
				}
				if(queryUtil.tableExists(engine, "KVSTORE", database, schema)) {
					executeSql(conn, queryUtil.dropTable("KVSTORE"));
				}
				if(queryUtil.tableExists(engine, "METAMODELPOSITION", database, schema)) {
					executeSql(conn, queryUtil.dropTable("METAMODELPOSITION"));
				}
			}
		}
	}
	
	@Deprecated
	private static void updateMetadataTable(IRDBMSEngine engine, Connection conn, AbstractSqlQueryUtil queryUtil, String tableName, String database, String schema) throws SQLException {
		if(queryUtil.tableExists(engine, tableName, database, schema)) {
			// rename key to metakey and value to metavalue
			List<String> allCols = queryUtil.getTableColumns(conn, tableName, database, schema);
			if(allCols.contains(Constants.KEY) || allCols.contains(Constants.KEY.toLowerCase())) {
				String sql = queryUtil.modColumnName(tableName, Constants.KEY, Constants.LM_META_KEY);
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
			if(allCols.contains(Constants.VALUE) || allCols.contains(Constants.VALUE.toLowerCase())) {
				String sql = queryUtil.modColumnName(tableName, Constants.VALUE, Constants.LM_META_VALUE);
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
				
				sql = queryUtil.modColumnType(tableName, Constants.LM_META_VALUE, queryUtil.getClobDataTypeName());
				logger.info("Running sql " + sql);
				executeSql(conn, sql);
			}
			
			boolean allowIfExists = queryUtil.allowIfExistsModifyColumnSyntax();
			if(queryUtil.allowDropColumn()) {
				if(allowIfExists) {
					String sql = queryUtil.alterTableDropColumnIfExists(tableName, "LOCALCONCEPTID");
					logger.info("Running sql " + sql);
					executeSql(conn, sql);
				} else {
					// check column exists in table
					if(allCols.contains("LOCALCONCEPTID") || allCols.contains("LOCALCONCEPTID".toLowerCase())) {
						String sql = queryUtil.alterTableDropColumnIfExists(tableName, "LOCALCONCEPTID");
						logger.info("Running sql " + sql);
						executeSql(conn, sql);
					}
				}
			}
			if(queryUtil.allowAddColumn()) {
				if(allowIfExists) {
					executeSql(conn, queryUtil.alterTableAddColumnIfNotExists(tableName, "PHYSICALNAMEID", "varchar(255)"));
				} else {
					// check column exists in table
					if(!allCols.contains("PHYSICALNAMEID") && !allCols.contains("PHYSICALNAMEID".toLowerCase())) {
						String sql = queryUtil.alterTableAddColumn(tableName, "PHYSICALNAMEID", "varchar(255)");
						logger.info("Running sql " + sql);
						executeSql(conn, sql);
					}
				}
			}
		}
	}

	private static void executeSql(Connection conn, String sql) throws SQLException {
		try (Statement stmt = conn.createStatement()){
			stmt.execute(sql);
		}
	}

	/**
	 * Return all the logical names for a given conceptual name
	 * @param conceptualName
	 * @return
	 */
	public static List<String> getAllLogicalNamesFromConceptualRDBMS(String conceptualName) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CONCEPT__LOGICALNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPT__CONCEPTUALNAME", "==", conceptualName));
		qs.addOrderBy(new QueryColumnOrderBySelector("CONCEPT__LOGICALNAME"));

		return QueryExecutionUtility.flushToListString(engine, qs);
	}

	/**
	 * Return all the logical names for a given conceptual name
	 * @param conceptualName
	 * @return
	 */
	public static List<String> getAllLogicalNamesFromPixelName(List<String> pixelNames) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CONCEPT__LOGICALNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__SEMOSSNAME", "==", pixelNames));
		qs.addRelation("CONCEPT__LOCALCONCEPTID", "ENGINECONCEPT__LOCALCONCEPTID", "inner.join");
		qs.addOrderBy(new QueryColumnOrderBySelector("CONCEPT__LOGICALNAME"));

		return QueryExecutionUtility.flushToListString(engine, qs);
	}
	
	/**
	 * Return all the logical names for a given conceptual name
	 * @param conceptualName
	 * @return
	 */
	public static List<String> getLocalConceptIdsFromLogicalName(List<String> logicalNames) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CONCEPT__LOCALCONCEPTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPT__LOGICALNAME", "==", logicalNames));

		return QueryExecutionUtility.flushToListString(engine, qs);
	}
	
	/**
	 * Return all the logical names for a given conceptual name
	 * @param conceptualName
	 * @return
	 */
	public static List<String> getLocalConceptIdsFromPixelName(List<String> pixelNames) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CONCEPT__LOCALCONCEPTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__SEMOSSNAME", "==", pixelNames));
		qs.addRelation("CONCEPT__LOCALCONCEPTID", "ENGINECONCEPT__LOCALCONCEPTID", "inner.join");

		return QueryExecutionUtility.flushToListString(engine, qs);
	}
	
	/**
	 * Return all the logical names for a given conceptual name
	 * @param conceptualName
	 * @return
	 */
	public static List<String> getConceptualIdsWithSimilarLogicalNames(List<String> conceptualIds) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CONCEPT__LOCALCONCEPTID"));
		
		SelectQueryStruct subQs = new SelectQueryStruct();
		subQs.addSelector(new QueryColumnSelector("CONCEPT__LOGICALNAME"));
		subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPT__LOCALCONCEPTID", "==", conceptualIds));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPT__LOGICALNAME", "==", subQs, PixelDataType.QUERY_STRUCT));

		return QueryExecutionUtility.flushToListString(engine, qs);
	}
	
	/**
	 * Get a list of arrays containing [table, column, type] for a given database
	 * @param databaseId
	 * @return
	 */
	public static List<Object[]> getAllTablesAndColumns(String databaseId) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PROPERTY_TYPE"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PK"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PHYSICALNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTPHYSICALNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__IGNORE_DATA", "==", false, PixelDataType.BOOLEAN));
		qs.addOrderBy("ENGINECONCEPT__PARENTSEMOSSNAME");
		qs.addOrderBy("ENGINECONCEPT__PK");
		qs.addOrderBy("ENGINECONCEPT__SEMOSSNAME");

		List<Object[]> ret = new ArrayList<>();

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				boolean isPk = (boolean) data[3];
				if(isPk) {
					data[0] = data[1];
				}
				Object type = data[2];
				if(type != null && (type.equals("DOUBLE") || type.equals("INT"))) {
					data[2] = "NUMBER";
				}
				ret.add(data);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return ret;
	}

	/**
	 * Get a list of arrays containing [table, column, type] for a given database
	 * @param engineId
	 * @return
	 */
	public static List<Object[]> getAllTablesAndColumns(Collection<String> databaseIds) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__ENGINE"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PROPERTY_TYPE"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PK"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseIds));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__IGNORE_DATA", "==", false, PixelDataType.BOOLEAN));
		qs.addOrderBy("ENGINECONCEPT__ENGINE");
		qs.addOrderBy("ENGINECONCEPT__PARENTSEMOSSNAME");
		qs.addOrderBy("ENGINECONCEPT__SEMOSSNAME");

		List<Object[]> ret = new ArrayList<>();

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				boolean isPk = (boolean) data[4];
				if(isPk) {
					data[1] = data[2];
				}
				Object type = data[3];
				if(type != null && (type.equals("DOUBLE") || type.equals("INT"))) {
					data[3] = "NUMBER";
				}
				ret.add(data);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return ret;
	}

	public static List<String[]> getRelationships(Collection<String> databaseIds) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINERELATION__ENGINE"));
		qs.addSelector(new QueryColumnSelector("ENGINERELATION__SOURCEPROPERTY"));
		qs.addSelector(new QueryColumnSelector("ENGINERELATION__TARGETPROPERTY"));
		qs.addSelector(new QueryColumnSelector("ENGINERELATION__RELATIONNAME"));
		if(databaseIds != null && !databaseIds.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINERELATION__ENGINE", "==", databaseIds));
		}

		return QueryExecutionUtility.flushRsToListOfStrArray(engine, qs);
	}

	/**
	 * Get a list of connections for a given logical name
	 * @param localConceptIds 
	 * @param databaseFilter
	 * @return
	 */
	public static List<Map<String, Object>> getDatabaseConnections(List<String> localConceptIds, List<String> databaseFilter) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		List<Map<String, Object>> returnData = new ArrayList<>();
		
		/*
		 * Grab all the matching tables and columns based on the logical names
		 * Once we have those, we will grab all the relationships for the tables
		 * and all the other columns that we can traverse to
		 */

		// store a list of the parent ids to the Object[] of results 
		// that were matches as something
		// that is a possible join
		Map<String, Object[]> parentEquivMap = new HashMap<>();
		Set<String> parentIds = new HashSet<String>();
		List<String> idsForRelationships = new ArrayList<>();
		List<String> idsForProperties = new ArrayList<>();
		
		// this will give me all the tables that have the logical name or 
		// have a column with the logical name 
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTPHYSICALNAMEID"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PHYSICALNAMEID"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PK"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__IGNORE_DATA"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__LOCALCONCEPTID", "==", localConceptIds));
		if(databaseFilter != null && !databaseFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseFilter));
		}
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				Object[] data = row.getValues();
				// for the purposes of query
				// if it is ignore_data (i.e. the table name matches but not a column)
				// i do not know how to join
				// so we will be ignoring those results for right now
				boolean ignore = (boolean) data[5];
				if(ignore) {
					continue;
				}

				String parentName = (String) data[0];
				String parentId = (String) data[1];
				String columnName = (String) data[2];
				String columnId = (String) data[3];
				boolean pk = (boolean) data[4];
				
				if(parentId != null) {
					// let me take your parent (table)
					// and see what i can join to from the parent
					// and add the properties as well
					idsForRelationships.add(parentId);
					idsForProperties.add(parentId);
					// and the join for this parent is the column that matches
					parentEquivMap.put(parentId, new Object[] {parentName, columnName, pk});
					
					// i also want to be able to join to this table directly (this is for rdf/graph)
					parentIds.add(parentId);
				}
				
				if(parentId == null && pk) {
					// if you are a true concept
					// i can join to you directly
					// and attach to your properties
					// or to your relationships
					idsForRelationships.add(columnId);
					idsForProperties.add(columnId);
					// and the join is the concept itself
					parentEquivMap.put(columnId, new Object[] {columnName, columnName, pk});
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		// let me add in all the concepts that are my parent
		qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__ENGINE"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PHYSICALNAMEID"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PROPERTY_TYPE"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PK"));
		if(databaseFilter != null && !databaseFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseFilter));
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__PK", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__IGNORE_DATA", "==", false, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__PHYSICALNAMEID", "==", parentIds));
		qs.addRelation("ENGINE__ID", "ENGINECONCEPT__ENGINE", "inner.join");
		qs.addOrderBy("ENGINE__ENGINENAME");
		qs.addOrderBy("ENGINECONCEPT__PARENTSEMOSSNAME");
		qs.addOrderBy("ENGINECONCEPT__IGNORE_DATA");
		qs.addOrderBy("ENGINECONCEPT__PK");
		qs.addOrderBy("ENGINECONCEPT__SEMOSSNAME");
		
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				Object[] data = row.getValues();

				String engineName = (String) data[0];
				String engineId = (String) data[1];
				String column = (String) data[2];
				String columnId = (String) data[3];
				String type = (String) data[4];
				boolean pk = (boolean) data[5];

				// these will all have column ids based on the query
				// i will just grab the details
				Object[] equivTableCol = parentEquivMap.get(columnId);

				// if we passed the above test, add the valid connection
				Map<String, Object> mapRow = new HashMap<String, Object>();
				mapRow.put("database_id", engineId);
				mapRow.put("database_name", engineName);
				mapRow.put("table", column);
				mapRow.put("pk", pk);
				mapRow.put("dataType", type);
				mapRow.put("type", "property");
				mapRow.put("equivTable", equivTableCol[0]);
				mapRow.put("equivColumn", equivTableCol[1]);
				mapRow.put("equivPk", equivTableCol[2]);
				returnData.add(mapRow);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		// now let me query for all the properties
		qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__ENGINE"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTPHYSICALNAMEID"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PHYSICALNAMEID"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PROPERTY_TYPE"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PK"));
		if(databaseFilter != null && !databaseFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseFilter));
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__PARENTPHYSICALNAMEID", "==", idsForProperties));
		qs.addRelation("ENGINE__ID", "ENGINECONCEPT__ENGINE", "inner.join");
		qs.addOrderBy("ENGINE__ENGINENAME");
		qs.addOrderBy("ENGINECONCEPT__PARENTSEMOSSNAME");
		qs.addOrderBy("ENGINECONCEPT__IGNORE_DATA");
		qs.addOrderBy("ENGINECONCEPT__PK");
		qs.addOrderBy("ENGINECONCEPT__SEMOSSNAME");
		
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				Object[] data = row.getValues();

				String engineName = (String) data[0];
				String engineId = (String) data[1];
				String parent = (String) data[2];
				String parentId = (String) data[3];
				String column = (String) data[4];
				String columnId = (String) data[5];
				String type = (String) data[6];
				boolean pk = (boolean) data[7];
//				boolean ignore = (boolean) data[8];

				// these will all have parent ids based on the query
				// i will just grab the details
				Object[] equivTableCol = parentEquivMap.get(parentId);

				// if the property we get is one where the table will be joined on
				// we have to ignore it
				if(equivTableCol[1].equals(column) && parent.equals(equivTableCol[0]) ) {
					continue;
				}

				// if we passed the above test, add the valid connection
				Map<String, Object> mapRow = new HashMap<>();
				mapRow.put("database_id", engineId);
				mapRow.put("database_name", engineName);
				mapRow.put("table", parent);
				mapRow.put("column", column);
				mapRow.put("pk", pk);
				mapRow.put("dataType", type);
				mapRow.put("type", "property");
				mapRow.put("equivTable", equivTableCol[0]);
				mapRow.put("equivColumn", equivTableCol[1]);
				mapRow.put("equivPk", equivTableCol[2]);
				returnData.add(mapRow);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		// let me find up and downstream connections for my equivalent concepts
		qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		qs.addSelector(new QueryColumnSelector("ENGINERELATION__ENGINE"));
		qs.addSelector(new QueryColumnSelector("ENGINERELATION__SOURCECONCEPTID"));
		qs.addSelector(new QueryColumnSelector("ENGINERELATION__TARGETCONCEPTID"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PK"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__IGNORE_DATA"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PROPERTY_TYPE"));
		qs.addSelector(new QueryColumnSelector("ENGINERELATION__RELATIONNAME"));
		if(databaseFilter != null && !databaseFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINERELATION__ENGINE", "==", databaseFilter));
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINERELATION__SOURCECONCEPTID", "==", idsForRelationships));
		qs.addRelation("ENGINE__ID", "ENGINERELATION__ENGINE", "inner.join");
		qs.addRelation("ENGINERELATION__TARGETCONCEPTID", "ENGINECONCEPT__PHYSICALNAMEID", "inner.join");
		qs.addOrderBy("ENGINERELATION__ENGINE");
		
		Map<String, Object[]> relationshipEquivMap = new HashMap<>();
		
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				Object[] data = row.getValues();
				
				String sourceId = (String) data[2];
				String downstreamId = (String) data[3];
				boolean downstreamIgnore = (boolean) data[7];

				Object[] equivTableCol = parentEquivMap.get(sourceId);
				if(downstreamIgnore) {
					relationshipEquivMap.put(downstreamId, equivTableCol);
					continue;
				}
				
				String databaseName = (String) data[0];
				String databaseId = (String) data[1];
				String downstreamParent = (String) data[4];
				String downstreamName = (String) data[5];
				boolean downstreamPK = (boolean) data[6];
				String type = (String) data[8];
				String relName = (String) data[9];
				
				// the downstream nodes
				// mean that the source is the equivalent concept

				// if we passed the above test, add the valid connection
				Map<String, Object> mapRow = new HashMap<>();
				mapRow.put("database_id", databaseId);
				mapRow.put("database_name", databaseName);
				if(downstreamParent == null) {
					mapRow.put("table", downstreamName);
				} else {
					mapRow.put("table", downstreamParent);
					mapRow.put("column", downstreamName);
				}
				mapRow.put("pk", downstreamPK);
				mapRow.put("dataType", type);
				mapRow.put("type", "downstream");
				mapRow.put("relName", relName);
				mapRow.put("equivTable", equivTableCol[0]);
				mapRow.put("equivColumn", equivTableCol[1]);
				mapRow.put("equivPk", equivTableCol[2]);
				returnData.add(mapRow);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
//		// let me pull all the relationships that are ignore
//		// this means i use the relationship to pull in any query
//		if(!relationshipEquivMap.isEmpty()) {
//			qs = new SelectQueryStruct();
//			qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
//			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__ENGINE"));
//			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
//			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
//			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PROPERTY_TYPE"));
//			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTPHYSICALNAMEID"));
//			if(engineFilter != null && !engineFilter.isEmpty()) {
//				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", engineFilter));
//			}
//			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__PARENTPHYSICALNAMEID", "==", new Vector<String>(relationshipEquivMap.keySet())));
//			qs.addRelation("ENGINE__ID", "ENGINECONCEPT__ENGINE", "inner.join");
//			qs.addOrderBy("ENGINECONCEPT__ENGINE");
//			try {
//				wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
//				while(wrapper.hasNext()) {
//					IHeadersDataRow row = wrapper.next();
//					Object[] data = row.getValues();
//					
//					String engineName = (String) data[0];
//					String engineId = (String) data[1];
//					String parentName = (String) data[2];
//					String name = (String) data[3];
//					String type = (String) data[4];
//					String parentId = (String) data[5];
//		
//					Object[] equivTableCol = relationshipEquivMap.get(parentId);
//					
//					// if we passed the above test, add the valid connection
//					Map<String, Object> mapRow = new HashMap<>();
//					mapRow.put("app_id", engineId);
//					mapRow.put("app_name", engineName);
//					mapRow.put("table", parentName);
//					mapRow.put("column", name);
//					mapRow.put("pk", false);
//					mapRow.put("dataType", type);
//					mapRow.put("type", "downstream");
////					mapRow.put("relName", relName);
//					mapRow.put("equivTable", equivTableCol[0]);
//					mapRow.put("equivColumn", equivTableCol[1]);
//					mapRow.put("equivPk", equivTableCol[2]);
//					returnData.add(mapRow);
//				}
//			} catch (Exception e) {
//				logger.error(Constants.STACKTRACE, e);
//			} finally {
//				if(wrapper != null) {
//					wrapper.cleanUp();
//				}
//			}
//		}
		relationshipEquivMap.clear();
		
		// let me find up and upstream connections for my equivalent concepts
		qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		qs.addSelector(new QueryColumnSelector("ENGINERELATION__ENGINE"));
		qs.addSelector(new QueryColumnSelector("ENGINERELATION__TARGETCONCEPTID"));
		qs.addSelector(new QueryColumnSelector("ENGINERELATION__SOURCECONCEPTID"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PK"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__IGNORE_DATA"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PROPERTY_TYPE"));
		qs.addSelector(new QueryColumnSelector("ENGINERELATION__RELATIONNAME"));
		if(databaseFilter != null && !databaseFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINERELATION__ENGINE", "==", databaseFilter));
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINERELATION__TARGETCONCEPTID", "==", idsForRelationships));
		qs.addRelation("ENGINE__ID", "ENGINERELATION__ENGINE", "inner.join");
		qs.addRelation("ENGINERELATION__SOURCECONCEPTID", "ENGINECONCEPT__PHYSICALNAMEID", "inner.join");
		qs.addOrderBy("ENGINERELATION__ENGINE");
		
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				IHeadersDataRow row = wrapper.next();
				Object[] data = row.getValues();
				
				String targetId = (String) data[2];
				String upstreamId = (String) data[3];
				boolean upstreamIgnore = (boolean) data[7];

				Object[] equivTableCol = parentEquivMap.get(targetId);
				if(upstreamIgnore) {
					relationshipEquivMap.put(upstreamId, equivTableCol);
					continue;
				}
				
				String databaseName = (String) data[0];
				String databaseId = (String) data[1];
				String upstreamParent = (String) data[4];
				String upstreamName = (String) data[5];
				boolean upstreamPK = (boolean) data[6];
				String type = (String) data[8];
				String relName = (String) data[9];
				
				// the downstream nodes
				// mean that the source is the equivalent concept

				// if we passed the above test, add the valid connection
				Map<String, Object> mapRow = new HashMap<>();
				mapRow.put("database_id", databaseId);
				mapRow.put("database_name", databaseName);
				if(upstreamParent == null) {
					mapRow.put("table", upstreamName);
				} else {
					mapRow.put("table", upstreamParent);
					mapRow.put("column", upstreamName);
				}
				mapRow.put("pk", upstreamPK);
				mapRow.put("dataType", type);
				mapRow.put("type", "upstream");
				mapRow.put("relName", relName);
				mapRow.put("equivTable", equivTableCol[0]);
				mapRow.put("equivColumn", equivTableCol[1]);
				mapRow.put("equivPk", equivTableCol[2]);
				returnData.add(mapRow);
			}
		} catch (Exception e1) {
			logger.error(Constants.STACKTRACE, e1);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
//		// let me pull all the relationships that are ignore
//		// this means i use the relationship to pull in any query
//		if(!relationshipEquivMap.isEmpty()) {
//			qs = new SelectQueryStruct();
//			qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
//			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__ENGINE"));
//			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
//			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
//			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PROPERTY_TYPE"));
//			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTPHYSICALNAMEID"));
//			if(engineFilter != null && !engineFilter.isEmpty()) {
//				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", engineFilter));
//			}
//			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__PARENTPHYSICALNAMEID", "==", new Vector<String>(relationshipEquivMap.keySet())));
//			qs.addRelation("ENGINE__ID", "ENGINECONCEPT__ENGINE", "inner.join");
//			qs.addOrderBy("ENGINECONCEPT__ENGINE");
//			try {
//				wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
//				while(wrapper.hasNext()) {
//					IHeadersDataRow row = wrapper.next();
//					Object[] data = row.getValues();
//					
//					String engineName = (String) data[0];
//					String engineId = (String) data[1];
//					String parentName = (String) data[2];
//					String name = (String) data[3];
//					String type = (String) data[4];
//					String parentId = (String) data[5];
//		
//					Object[] equivTableCol = relationshipEquivMap.get(parentId);
//					
//					// if we passed the above test, add the valid connection
//					Map<String, Object> mapRow = new HashMap<>();
//					mapRow.put("app_id", engineId);
//					mapRow.put("app_name", engineName);
//					mapRow.put("table", parentName);
//					mapRow.put("column", name);
//					mapRow.put("pk", false);
//					mapRow.put("dataType", type);
//					mapRow.put("type", "upstream");
////					mapRow.put("relName", relName);
//					mapRow.put("equivTable", equivTableCol[0]);
//					mapRow.put("equivColumn", equivTableCol[1]);
//					mapRow.put("equivPk", equivTableCol[2]);
//					returnData.add(mapRow);
//				}
//			} catch (Exception e) {
//				logger.error(Constants.STACKTRACE, e);
//			} finally {
//				if(wrapper != null) {
//					wrapper.cleanUp();
//				}
//			}
//		}
		relationshipEquivMap.clear();
		
		return returnData;
	}

	/**
	 * Get the metamodel
	 * @param databaseId
	 * @param includeDataTypes
	 * @return
	 */
	public static Map<String, Object> getMetamodelRDBMS(String databaseId, boolean includeDataTypes) {
		// TODO: should setup to return the physical name ids
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		// idHash - physical ID to the name of the node
		Map<String, MetamodelVertex> nodeHash = new HashMap<>();

		Map<String, String> physicalDataTypes = new HashMap<>();
		Map<String, String> dataTypes = new HashMap<>();
		Map<String, String> additionalDataTypes = new HashMap<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PHYSICALNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTPHYSICALNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__IGNORE_DATA"));
		if(includeDataTypes) {
			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__ORIGINAL_TYPE"));
			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PROPERTY_TYPE"));
			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__ADDITIONAL_TYPE"));
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseId));

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();

				String semossName = (String) row[0];
				String physicalName = (String) row[2];
				String parentSemossName = (String) row[1];
				String parentPhysicalName = (String) row[3];
				boolean ignoreData = (boolean) row[4];
				
				MetamodelVertex node = null;
				// if already there, should we still add it ?
				if(parentSemossName != null) {
					// this has a parent
					if(nodeHash.containsKey(parentSemossName)) {
						node = nodeHash.get(parentSemossName);
					} else {
						node = new MetamodelVertex(parentSemossName);
						nodeHash.put(parentSemossName, node);
					}
				} else {
					// this is the parent
					if(nodeHash.containsKey(semossName)) {
						node = nodeHash.get(semossName);
					} else {
						node = new MetamodelVertex(semossName);
						nodeHash.put(semossName, node);
					}
				}

				String uniqueName = semossName;
				if(parentSemossName != null) {
					uniqueName = parentSemossName + "__" + uniqueName;
					node.addProperty(semossName);
				}

				if(includeDataTypes && !ignoreData) {
					if(row[5] != null) {
						String origType = row[5].toString();
						if(origType.contains("TYPE:")) {
							origType = origType.replace("TYPE:", "");
						}
						physicalDataTypes.put(uniqueName, origType);	
					}
					if(row[6] != null) {
						String cleanType = row[6].toString();
						dataTypes.put(uniqueName, cleanType);
					}
					if(row[7] != null) {
						String additionalType = row[7].toString();
						additionalDataTypes.put(uniqueName, additionalType);
					}
				}
			}
		} catch (Exception e1) {
			logger.error(Constants.STACKTRACE, e1);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		Map<String, Map<String, String>> edgeHash = new Hashtable<>();
		qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINERELATION__SOURCEPROPERTY"));
		qs.addSelector(new QueryColumnSelector("ENGINERELATION__TARGETPROPERTY"));
		qs.addSelector(new QueryColumnSelector("ENGINERELATION__RELATIONNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINERELATION__ENGINE", "==", databaseId));
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				String startName = row[0].toString();
				String endName = row[1].toString();
				String relName = row[2].toString();

				Map<String, String> newEdge = new Hashtable<>();
				// need to check to see if the idHash has it else put it in
				newEdge.put("source", startName);
				newEdge.put("target", endName);
				newEdge.put("relation", relName);
				if(relName.contains(".")) {
					String[] split = relName.split("[.]");
					if(split.length == 4) {
						if(startName.equals(split[0])) {
							newEdge.put("sourceColumn", split[1]);
							newEdge.put("targetColumn", split[3]);
						} else {
							newEdge.put("sourceColumn", split[3]);
							newEdge.put("targetColumn", split[1]);
						}
					} else if(split.length == 6) {
						if(startName.equals(split[1])) {
							newEdge.put("sourceColumn", split[2]);
							newEdge.put("targetColumn", split[5]);
						} else {
							newEdge.put("sourceColumn", split[5]);
							newEdge.put("targetColumn", split[2]);
						}
					}
				}
				edgeHash.put(endName + "-" + endName + "-" + relName, newEdge);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		Map<String, Object> finalHash = new Hashtable<>();
		finalHash.put("nodes", nodeHash.values().toArray());
		finalHash.put("edges", edgeHash.values().toArray());
		if(includeDataTypes) {
			finalHash.put("physicalTypes", physicalDataTypes);
			finalHash.put("dataTypes", dataTypes);
			finalHash.put("additionalDataTypes", additionalDataTypes);
		}
		return finalHash;
	}

	/**
	 * Get the properties for a given concept for a specific database
	 * THIS IS THE SAME QUERY AS {@link #getConceptPropertiesRDBMS} BUT DIFFERENT RETURN
	 * @param conceptName
	 * @param engineId
	 * @return
	 */
	public static Map<String, List<String>>  getConceptProperties(List<String> logicalNames, String databaseFilter) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__ENGINE"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTPHYSICALNAMEID"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PHYSICALNAMEID"));
		if(databaseFilter != null && !databaseFilter.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseFilter));
		}
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			// store first and fill in sub query after
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINECONCEPT__PARENTPHYSICALNAMEID", "==", subQs));

			// fill in the sub query with the necessary column output + filters
			subQs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTPHYSICALNAMEID"));
			// we have a sub query again
			SelectQueryStruct subQs2 = new SelectQueryStruct();
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINECONCEPT__LOCALCONCEPTID", "==", subQs2));

			// fill in the second sub query with the necessary column output + filters
			subQs2.addSelector(new QueryColumnSelector("CONCEPT__LOCALCONCEPTID"));
			subQs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPT__CONCEPTUALNAME", "==", logicalNames));
		}
		qs.addOrderBy(new QueryColumnOrderBySelector("ENGINECONCEPT__ENGINE"));
		qs.addOrderBy(new QueryColumnOrderBySelector("ENGINECONCEPT__PK"));
		
		Map<String, List<String>> queryData = new HashMap<>();

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				IHeadersDataRow data = wrapper.next();
				// keeps the id to the concept name
				Object[] row = data.getValues();

				String engineId = (String) row[0];
				String parentName = (String) row[1];
				String parentPhysicalId = (String) row[2];
				String columnName = (String) row[3];
				String columnPhysicalId = (String) row[4];
				
				// get or create the vertex
				List<String> propList = null;
				if(queryData.containsKey(parentName)) {
					propList = queryData.get(parentName);
				} else {
					propList = new ArrayList<>();
					// add to the engine map
					queryData.put(parentName, propList);
				}

				// add the property conceptual name
				propList.add(columnName);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return queryData;
	}

	/**
	 * Get the properties for a given concept across all the databases
	 * THIS IS THE SAME QUERY AS {@link #getConceptProperties} BUT DIFFERENT RETURN
	 * @param conceptName
	 * @param engineId		optional filter for the properties
	 * @return
	 */
	public static Map<String, Object[]> getConceptProperties(List<String> logicalNames, List<String> databaseFilter) {
		// query to get all the physical name ids that tie to the parent
		// and then pull all of their properties

		Map<String, Object[]> returnHash = new TreeMap<>();
		Map<String, Map<String, MetamodelVertex>> queryData = new TreeMap<>();

		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
		{
			// for tabular databases
			// we grab the parent
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__ENGINE"));
			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTPHYSICALNAMEID"));
			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PHYSICALNAMEID"));
			if(databaseFilter != null && !databaseFilter.isEmpty()) {
				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseFilter));
			}
			{
				SelectQueryStruct subQs = new SelectQueryStruct();
				// store first and fill in sub query after
				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__IGNORE_DATA", "==", true, PixelDataType.BOOLEAN));
				qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINECONCEPT__PARENTPHYSICALNAMEID", "==", subQs));
	
				// fill in the sub query with the necessary column output + filters
				// NOTE::: THIS IS THE MAIN DIFFERENCE FROM THE BELOW
				// 			THIS REQUIRED THE PARENT TO BE THE PARENTPHYSICALNAMEID
				//			SINCE THERE IS ALWAYS A PARENT ID IN LOCAL MASTER
				subQs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTPHYSICALNAMEID"));
				// we have a sub query again
				SelectQueryStruct subQs2 = new SelectQueryStruct();
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINECONCEPT__LOCALCONCEPTID", "==", subQs2));
	
				// fill in the second sub query with the necessary column output + filters
				subQs2.addSelector(new QueryColumnSelector("CONCEPT__LOCALCONCEPTID"));
				subQs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPT__CONCEPTUALNAME", "==", logicalNames));
			}
			qs.addOrderBy(new QueryColumnOrderBySelector("ENGINECONCEPT__ENGINE"));
			qs.addOrderBy(new QueryColumnOrderBySelector("ENGINECONCEPT__PK"));
			
			
			IRawSelectWrapper wrapper = null;
			try {
				wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
				while(wrapper.hasNext()) {
					IHeadersDataRow data = wrapper.next();
					// keeps the id to the concept name
					Object[] row = data.getValues();
	
					String databaseId = (String) row[0];
					String parentName = (String) row[1];
					String parentPhysicalId = (String) row[2];
					String columnName = (String) row[3];
					String columnPhysicalId = (String) row[4];
					
					Map<String, MetamodelVertex> databaseMap = null;
					if(queryData.containsKey(databaseId)) {
						databaseMap  = queryData.get(databaseId);
					} else {
						databaseMap = new TreeMap<>();
						// add to query data map
						queryData.put(databaseId, databaseMap);
					}
	
					// get or create the vertex
					MetamodelVertex vert = null;
					if(databaseMap.containsKey(parentName)) {
						vert = databaseMap.get(parentName);
					} else {
						vert = new MetamodelVertex(parentName);
						// add to the engine map
						databaseMap.put(parentName, vert);
					}
	
					// add the property conceptual name
					vert.addProperty(columnName);
				}
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(wrapper != null) {
					try {
						wrapper.close();
					} catch (IOException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		{
			// for graph/RDF databases
			// we grab the node itself where it is not ignored
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__ENGINE"));
			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTPHYSICALNAMEID"));
			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
			qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PHYSICALNAMEID"));
			if(databaseFilter != null && !databaseFilter.isEmpty()) {
				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseFilter));
			}
			{
				SelectQueryStruct subQs = new SelectQueryStruct();
				// store first and fill in sub query after
				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__IGNORE_DATA", "==", false, PixelDataType.BOOLEAN));
				qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINECONCEPT__PARENTPHYSICALNAMEID", "==", subQs));
	
				// fill in the sub query with the necessary column output + filters
				// NOTE::: THIS IS THE MAIN DIFFERENCE FROM THE ABOVE
				// 			THIS REQUIRED THE PARENT TO BE THE PHYSICALNAMEID
				// 			SINCE NODES/CONCEPTS DO NOT HAVE PARENTS IN LOCAL MASTER
				subQs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PHYSICALNAMEID"));
				// we have a sub query again
				SelectQueryStruct subQs2 = new SelectQueryStruct();
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINECONCEPT__LOCALCONCEPTID", "==", subQs2));
	
				// fill in the second sub query with the necessary column output + filters
				subQs2.addSelector(new QueryColumnSelector("CONCEPT__LOCALCONCEPTID"));
				subQs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPT__CONCEPTUALNAME", "==", logicalNames));
			}
			qs.addOrderBy(new QueryColumnOrderBySelector("ENGINECONCEPT__ENGINE"));
			qs.addOrderBy(new QueryColumnOrderBySelector("ENGINECONCEPT__PK"));
			
			IRawSelectWrapper wrapper = null;
			try {
				wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
				while(wrapper.hasNext()) {
					IHeadersDataRow data = wrapper.next();
					// keeps the id to the concept name
					Object[] row = data.getValues();
	
					String databaseId = (String) row[0];
					String parentName = (String) row[1];
					String parentPhysicalId = (String) row[2];
					String columnName = (String) row[3];
					String columnPhysicalId = (String) row[4];
					
					Map<String, MetamodelVertex> databaseMap = null;
					if(queryData.containsKey(databaseId)) {
						databaseMap  = queryData.get(databaseId);
					} else {
						databaseMap = new TreeMap<>();
						// add to query data map
						queryData.put(databaseId, databaseMap);
					}
	
					// get or create the vertex
					MetamodelVertex vert = null;
					if(databaseMap.containsKey(parentName)) {
						vert = databaseMap.get(parentName);
					} else {
						vert = new MetamodelVertex(parentName);
						// add to the engine map
						databaseMap.put(parentName, vert);
					}
	
					// add the property conceptual name
					vert.addProperty(columnName);
				}
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(wrapper != null) {
					try {
						wrapper.close();
					} catch (IOException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		for(String databaseName : queryData.keySet()) {
			returnHash.put(databaseName, queryData.get(databaseName).values().toArray());
		}

		return returnHash;
	}

	/**
	 * Get the list of  connected concepts for a given concept
	 * 
	 * Direction upstream/downstream is always in reference to the node being searched
	 * For example, if the relationship in the direction Title -> Genre
	 * The result would be { upstream -> [Genre] } because Title is upstream of Genre
	 * 
	 * @param conceptType
	 * @return
	 */
	@Deprecated
	public static Map getConnectedConceptsRDBMS(List<String> conceptLogicalNames, List<String> databaseFilters) {
		// I technically need to do 3 queries
		// first one is get the localconceptid / physicalids for all of these
		// second is the upstream
		// third is the downstream
		
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
		
		//select e.enginename, ec.engine, c.logicalname, ec.physicalnameid from concept c, engineconcept ec, engine e where c.logicalname in ('Title') and c.localconceptid=ec.localconceptid and e.id = ec.engine
//		String conceptMasterQuery = "select ec.engine, c.conceptualname, ec.physicalnameid, ec.physicalname "
//				+ "from concept c, engineconcept ec "
//				+ "where c.logicalname in " + conceptString
//				+ (engineFilters != null ? (" and ec.engine in " + engineString) + " " : "")
//				+ "and c.localconceptid=ec.localconceptid";
		
		// id to concept
		Hashtable <String, String> idToName = new Hashtable <>();

		// this is the final return object
		// engine > concept > downstream > items
		// retMap > conceptSpecific > stream
		Map<String, Map> retMap = new TreeMap<>();
		
		{
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.addSelector(new QueryColumnOrderBySelector("ENGINECONCEPT__ENGINE"));
			qs.addSelector(new QueryColumnOrderBySelector("CONCEPT__CONCEPTUALNAME"));
			qs.addSelector(new QueryColumnOrderBySelector("ENGINECONCEPT__PHYSICALNAMEID"));
			qs.addSelector(new QueryColumnOrderBySelector("ENGINECONCEPT__PHYSICALNAME"));
			if(databaseFilters != null && !databaseFilters.isEmpty()) {
				qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseFilters));
			}
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPT__LOGICALNAME", "==", conceptLogicalNames));
			qs.addRelation("CONCEPT__LOCALCONCEPTID", "ENGINECONCEPT__LOCALCONCEPTID", "inner.join");
			
			IRawSelectWrapper wrapper = null;
			try {
				wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
				while(wrapper.hasNext()) {
					Object[] row = wrapper.next().getValues();
					
					String engineId = row[0].toString();
					String conceptualName = row[1].toString();
					String physicalNameId = row[2].toString();
					String equivalentConcept = row[3].toString();
					
					// put the id for future reference
					// no reason why we cannot cache but.. 
					idToName.put(physicalNameId, conceptualName);
	
					Map <String, Object> conceptSpecific = null;
					if(retMap.containsKey(engineId)) {
						conceptSpecific = retMap.get(engineId);
					} else {
						conceptSpecific = new TreeMap<>();
					}
					retMap.put(engineId, conceptSpecific);
	
					Hashtable <String, String> stream = new Hashtable<>();
					stream.put("equivalentConcept", equivalentConcept);
	
					conceptSpecific.put(conceptualName, stream);
					retMap.put(engineId, conceptSpecific);
				}
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(wrapper != null) {
					try {
						wrapper.close();
					} catch (IOException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		//select distinct  e.enginename, 'Title', 'downstream' as downstream,  er.relationname,  c.logicalname , er.engine, er.targetconceptid, ec.physicalname from enginerelation er, engineconcept ec, concept c, engine e where er.sourceconceptid in (select physicalnameid from engineconcept where localconceptid in (select localconceptid from concept where logicalname in ('Title'))) 
		//and ec.physicalnameid=er.targetconceptid and c.localconceptid=ec.localconceptid and e.id=er.engine;

		// now time to run the upstream and downstream queries
//		String downstreamQuery = "select distinct ec.engine, er.sourceconceptid, 'upstream' as upstream, "
//				+ "er.relationname, c.conceptualname , er.engine, er.targetconceptid, ec.physicalname "
//				+ "from enginerelation er, engineconcept ec, concept c "
//				+ "where "
//				+ (engineFilters != null ? (" ec.engine in " + engineString + " and ") : "")
//				+ "er.sourceconceptid in (select physicalnameid from engineconcept where localconceptid in "
//				+ "(select localconceptid from concept where logicalname in " + conceptString + ")) "
//				+ "and ec.physicalnameid=er.targetconceptid and c.localconceptid=ec.localconceptid;";
		
		{
			SelectQueryStruct downQs = new SelectQueryStruct();
			downQs.addSelector(new QueryColumnSelector("ENGINECONCEPT__ENGINE"));
			downQs.addSelector(new QueryColumnSelector("ENGINERELATION__SOURCECONCEPTID"));
			downQs.addSelector(new QueryConstantSelector("upstream"));
			downQs.addSelector(new QueryColumnSelector("ENGINERELATION__RELATIONNAME"));
			downQs.addSelector(new QueryColumnSelector("CONCEPT__CONCEPTUALNAME"));
			downQs.addSelector(new QueryColumnSelector("ENGINERELATION__ENGINE"));
			downQs.addSelector(new QueryColumnSelector("ENGINERELATION__TARGETCONCEPTID"));
			downQs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PHYSICALNAME"));
			if(databaseFilters != null && !databaseFilters.isEmpty()) {
				downQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseFilters));
			}
			{
				SelectQueryStruct subQs = new SelectQueryStruct();
				downQs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINERELATION__SOURCECONCEPTID", "==", subQs));
				
				// fill in sub query selector + filter
				subQs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PHYSICALNAMEID"));
				SelectQueryStruct subQs2 = new SelectQueryStruct();
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINECONCEPT__LOCALCONCEPTID", "==", subQs2));
				
				// fill in second sub query selector + filter
				subQs2.addSelector(new QueryColumnOrderBySelector("CONCEPT__LOCALCONCEPTID"));
				subQs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPT__LOGICALNAME", "==", conceptLogicalNames));
			}
			downQs.addRelation("ENGINECONCEPT__PHYSICALNAMEID", "ENGINERELATION__TARGETCONCEPTID", "inner.join");
			downQs.addRelation("CONCEPT__LOCALCONCEPTID", "ENGINECONCEPT__LOCALCONCEPTID", "inner.join");
	
			IRawSelectWrapper wrapper = null;
			try {
				wrapper = WrapperManager.getInstance().getRawWrapper(engine, downQs);
				while(wrapper.hasNext()) {
					Object[] row = wrapper.next().getValues();
	
					String databaseId = row[0].toString();
					String coreConceptId = row[1].toString();
					String relationName = row[3].toString();
					String streamConceptName = row[4].toString();
					String streamPhysicalName = row[7].toString();
					
					// this is the main concept
					String coreConceptName = idToName.get(coreConceptId);
	
					Map <String, Map> databaseSpecific = retMap.get(databaseId);
					Map <String, Object> conceptSpecific = databaseSpecific.get(coreConceptName);
	
					Set<String> downstreams = new TreeSet<>();
					Set<String> physicalNames = new TreeSet<>();
	
					if(conceptSpecific.containsKey("upstream")) {
						downstreams = (Set<String>)conceptSpecific.get("upstream");
					}
					downstreams.add(streamConceptName);
					
					if(conceptSpecific.containsKey("physical")) {
						physicalNames = (Set<String>)conceptSpecific.get("physical");
					}
					physicalNames.add(streamPhysicalName);
					conceptSpecific.put("upstream", downstreams);
					conceptSpecific.put("physical", physicalNames);
					databaseSpecific.put(coreConceptName, conceptSpecific);
					retMap.put(databaseId, databaseSpecific);
				}
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(wrapper != null) {
					try {
						wrapper.close();
					} catch (IOException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
		
		// now time to run the upstream and downstream queries
//		String upstreamQuery = "select distinct ec.engine, er.targetconceptid, 'downstream' as downstream,  "
//				+ "er.relationname,  c.conceptualname , er.engine, er.sourceconceptid, ec.physicalname "
//				+ "from enginerelation er, engineconcept ec, concept c "
//				+ "where "
//				+ (engineFilters != null ? (" ec.engine in " + engineString + " and ") : "")
//				+ "er.targetconceptid in (select physicalnameid from engineconcept where localconceptid in "
//				+ "(select localconceptid from concept where logicalname in " + conceptString + ")) "
//				+ "and ec.physicalnameid=er.sourceconceptid and c.localconceptid=ec.localconceptid";
		
		{
			SelectQueryStruct upQs = new SelectQueryStruct();
			upQs.addSelector(new QueryColumnSelector("ENGINECONCEPT__ENGINE"));
			upQs.addSelector(new QueryColumnSelector("ENGINERELATION__TARGETCONCEPTID"));
			upQs.addSelector(new QueryConstantSelector("downstream"));
			upQs.addSelector(new QueryColumnSelector("ENGINERELATION__RELATIONNAME"));
			upQs.addSelector(new QueryColumnSelector("CONCEPT__CONCEPTUALNAME"));
			upQs.addSelector(new QueryColumnSelector("ENGINERELATION__ENGINE"));
			upQs.addSelector(new QueryColumnSelector("ENGINERELATION__SOURCECONCEPTID"));
			upQs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PHYSICALNAME"));
			if(databaseFilters != null && !databaseFilters.isEmpty()) {
				upQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseFilters));
			}
			{
				SelectQueryStruct subQs = new SelectQueryStruct();
				upQs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINERELATION__TARGETCONCEPTID", "==", subQs));
				
				// fill in sub query selector + filter
				subQs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PHYSICALNAMEID"));
				SelectQueryStruct subQs2 = new SelectQueryStruct();
				subQs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("ENGINECONCEPT__LOCALCONCEPTID", "==", subQs2));
				
				// fill in second sub query selector + filter
				subQs2.addSelector(new QueryColumnOrderBySelector("CONCEPT__LOCALCONCEPTID"));
				subQs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPT__LOGICALNAME", "==", conceptLogicalNames));
			}
			upQs.addRelation("ENGINECONCEPT__PHYSICALNAMEID", "ENGINERELATION__SOURCECONCEPTID", "inner.join");
			upQs.addRelation("CONCEPT__LOCALCONCEPTID", "ENGINECONCEPT__LOCALCONCEPTID", "inner.join");
	
			IRawSelectWrapper wrapper = null;
			try {
				wrapper = WrapperManager.getInstance().getRawWrapper(engine, upQs);
				while(wrapper.hasNext()) {
					Object[] row = wrapper.next().getValues();
	
					String databaseId = row[0].toString();
					String coreConceptId = row[1].toString();
					String relationName = row[3].toString();
					String streamConceptName = row[4].toString();
					String streamPhysicalName = row[7].toString();
					
					String coreConceptName = idToName.get(coreConceptId);
	
					Map <String, Map> databaseSpecific = retMap.get(databaseId);
					Map <String, Object> conceptSpecific = databaseSpecific.get(coreConceptName);
	
					Set<String> upstreams = new TreeSet<>();
					Set<String> physicalNames = new TreeSet<>();
	
					if(conceptSpecific.containsKey("downstream")) {
						upstreams = (Set<String>)conceptSpecific.get("downstream");
					}
					upstreams.add(streamConceptName);
					
					if(conceptSpecific.containsKey("physical")) {
						physicalNames = (Set<String>)conceptSpecific.get("physical");
					}
					
					physicalNames.add(streamPhysicalName);
					conceptSpecific.put("downstream", upstreams);
					conceptSpecific.put("physical", physicalNames);
					databaseSpecific.put(coreConceptName, conceptSpecific);
					retMap.put(databaseId, databaseSpecific);
				}
			} catch (Exception e) {
				logger.error(Constants.STACKTRACE, e);
			} finally {
				if(wrapper != null) {
					try {
						wrapper.close();
					} catch (IOException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			}
		}
	
		return retMap;
	}
	

	/**
	 * Get the list of unique engine ids
	 * @return
	 */
	public static List<String> getAllDatabaseIds() {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ID"));
		return QueryExecutionUtility.flushToListString(engine, qs);
	}

	/**
	 * Get an engine alias for an id
	 * @return
	 */
	public static String getDatabaseAliasForId(String id) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ID", "==", id));
		return QueryExecutionUtility.flushToString(engine, qs);
	}
	
	/**
	 * Get engine id to engine name map
	 * @return
	 */
	public static Map<String, String> getDatabaseIdToAliasMap() {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ID"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		
		Map<String, String> retMap = new HashMap<>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				retMap.put(row[0] + "", row[1] + "");
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return retMap;
	}

	/**
	 * Get the list of concepts for a given engine
	 * @param databaseId
	 * @return
	 */
	public static Set<String> getConceptsWithinDatabaseRDBMS(String databaseId) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__PK", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseId));
		qs.addOrderBy(new QueryColumnOrderBySelector("ENGINECONCEPT__SEMOSSNAME"));
		return QueryExecutionUtility.flushToSetString(engine, qs, true);
	}
	
	/**
	 * Get the list of concepts for a given engine
	 * @param databaseId
	 * @return
	 */
	public static Collection<String> getSelectorsWithinDatabaseRDBMS(String databaseId) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PK"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__IGNORE_DATA", "==", false, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseId));
		qs.addOrderBy(new QueryColumnOrderBySelector("ENGINECONCEPT__PK", "desc"));
		
		Set<String> selectors = new TreeSet<>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				if(row[0] == null) {
					selectors.add(row[1].toString());
				} else {
					selectors.add(row[0] + "__" + row[1].toString());
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return selectors;
	}

	/**
	 * Get the data type
	 * @param databaseId
	 * @param pixelName
	 * @param parentPixelName
	 * @return
	 */
	public static String getBasicDataType(String databaseId, String pixelName, String parentPixelName) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PROPERTY_TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__SEMOSSNAME", "==", pixelName));
		if(parentPixelName != null && !parentPixelName.isEmpty()) {
			// additional filters
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__PARENTSEMOSSNAME", "==", parentPixelName));
		}

		return QueryExecutionUtility.flushToString(engine, qs);
	}

	/**
	 * Get the additional data type
	 * @param databaseId
	 * @param conceptualName
	 * @param parentConceptualName
	 * @return
	 */
	public static String getAdditionalDataType(String databaseId, String conceptualName, String parentConceptualName) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__ADDITIONAL_TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__SEMOSSNAME", "==", conceptualName));
		if(parentConceptualName != null && !parentConceptualName.isEmpty()) {
			// additional filters
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__PARENTSEMOSSNAME", "==", parentConceptualName));
		}
		
		return QueryExecutionUtility.flushToString(engine, qs);
	}
	
	/**
	 * 
	 * @param databaseId
	 * @return
	 */

	public static Map<String, List<String>> getDatabaseLogicalNames(String databaseId) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
		
		Map<String, List<String>> engineLogicalNames = new HashMap<>();
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PK"));
		qs.addSelector(new QueryColumnSelector("CONCEPTMETADATA__METAVALUE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPTMETADATA__METAKEY", "==", "logical"));
		qs.addRelation("CONCEPTMETADATA__PHYSICALNAMEID", "ENGINECONCEPT__PHYSICALNAMEID", "inner.join");

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				String parentName = (String) row[0];
				String name = (String) row[1];
				boolean pk = (boolean) row[2];
				String logicalName = (String) row[3];

				String uniqueName = name;
				if(!pk) {
					uniqueName = parentName + "__" + name;
				}
				
				List<String> logicalNames = null;
				if(engineLogicalNames.containsKey(uniqueName)) {
					logicalNames = engineLogicalNames.get(uniqueName);
				} else {
					logicalNames = new ArrayList<>();
					// store in the map
					engineLogicalNames.put(uniqueName, logicalNames);
				}
				// add the new value
				logicalNames.add(logicalName);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return engineLogicalNames;
	}

	public static Map<String, String> getDatabaseDescriptions(String databaseId) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
		
		Map<String, String> engineDescriptions = new HashMap<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PK"));
		qs.addSelector(new QueryColumnSelector("CONCEPTMETADATA__METAVALUE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPTMETADATA__METAKEY", "==", "description"));
		qs.addRelation("CONCEPTMETADATA__PHYSICALNAMEID", "ENGINECONCEPT__PHYSICALNAMEID", "inner.join");

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				String parentName = (String) row[0];
				String name = (String) row[1];
				boolean pk = (boolean) row[2];
				String description = (String) row[3];

				String uniqueName = name;
				if(!pk) {
					uniqueName = parentName + "__" + name;
				}
				engineDescriptions.put(uniqueName, description);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return engineDescriptions;
	}

	/**
	 * Get the properties for a given concept
	 * @param conceptName
	 * @param databaseId		the database to get the properties for
	 * @return
	 */
	public static List<String> getSpecificConceptProperties(String parentName, String databaseId) {
		if(databaseId == null || databaseId.isEmpty()) {
			throw new IllegalArgumentException("Must define a valid engine id");
		}
				
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__PARENTSEMOSSNAME", "==", parentName));
		qs.addOrderBy(new QueryColumnOrderBySelector("ENGINECONCEPT__SEMOSSNAME"));
		
		return QueryExecutionUtility.flushToListString(engine, qs);
	}
	
	/**
	 * Get the queryable pixel selectors for the table
	 * If RDBMS this will have all the properties
	 * If RDF/Graph this will have the concept and all its properties
	 * Return will be in TABLE__COLUMN format
	 * @param parentName	String with the name of the concept
	 * @param databaseId		String with the engine to get the selectors for
	 * @return
	 */
	public static List<String> getConceptPixelSelectors(String parentName, String databaseId) {
		if(databaseId == null || databaseId.isEmpty()) {
			throw new IllegalArgumentException("Must define a valid engine id");
		}
				
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseId));
		// grab either
		// 1) all rows where the parent is what is passed in
		// or 
		// 2) the parent itself has data (i.e column name is the parent + pk = true + ignoreData=false)
		OrQueryFilter orFilter = new OrQueryFilter();
		qs.addExplicitFilter(orFilter);
		orFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__PARENTSEMOSSNAME", "==", parentName));
		// add an and filter to the or
		AndQueryFilter andFilter = new AndQueryFilter();
		orFilter.addFilter(andFilter);
		andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__SEMOSSNAME", "==", parentName));
		andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__PK", "==", true, PixelDataType.BOOLEAN));
		andFilter.addFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__IGNORE_DATA", "==", false, PixelDataType.BOOLEAN));
		qs.addOrderBy(new QueryColumnOrderBySelector("ENGINECONCEPT__SEMOSSNAME"));
		
		List<String> retArr = new ArrayList<>();

		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				Object[] row = wrapper.next().getValues();
				if(row[0] != null) {
					retArr.add(row[0] + "__" + row[1]);
				} else {
					retArr.add(row[1].toString());
				}
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return retArr;
	}

	/**
	 * Get the physical concept id for a concept given the engine id + conceptual name
	 * @param databaseId
	 * @param conceptualName
	 * @return
	 */
	public static String getPhysicalConceptId(String databaseId, String conceptualName) {
		// SELECT engineconcept.physicalnameid FROM engineconcept INNER JOIN concept ON concept.localconceptid=engineconcept.localconceptid WHERE engineconcept.engine='' AND concept.conceptualname='Title'
		//		String query = "SELECT engineconcept.physicalnameid "
		//				+ "FROM engineconcept "
		//				+ "INNER JOIN concept ON concept.localconceptid=engineconcept.localconceptid "
		//				+ "WHERE engineconcept.engine='" + engineId + "' "
		//				+ "AND concept.conceptualname='" + conceptualName + "';";

		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PHYSICALNAMEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPT__CONCEPTUALNAME", "==", conceptualName));
		qs.addRelation("CONCEPT__LOCALCONCEPTID", "ENGINECONCEPT__LOCALCONCEPTID", "inner.join");

		return QueryExecutionUtility.flushToString(engine, qs);
	}
	
	/**
	 * Get the physical concept id for a concept given the engine id + pixel name
	 * @param databaseId
	 * @param conceptualName
	 * @return
	 */
	public static String getPhysicalConceptIdFromPixelName(String databaseId, String pixelName) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PHYSICALNAMEID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseId));
		if(pixelName.contains("__")) {
			String[] split = pixelName.split("__");
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__PARENTSEMOSSNAME", "==", split[0]));
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__SEMOSSNAME", "==", split[1]));
		} else {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__SEMOSSNAME", "==", pixelName));
		}

		return QueryExecutionUtility.flushToString(engine, qs);
	}

	/**
	 * Get concept metadata value for a key
	 * 
	 * @param databaseId
	 * @param concept
	 * @param key
	 * @return
	 */
	public static String getMetadataValue(String databaseId, String concept, String key) {
		//		String query = "select " + Constants.VALUE + " from " + Constants.CONCEPT_METADATA_TABLE
		//				+ " where localconceptid in (select localconceptid from concept "
		//				+ "where localconceptid in (select localconceptid from engineconcept "
		//				+ "where engine='" + engineId + "') "
		//				+ "and conceptualname='" + concept + "') and " + Constants.KEY + "='" + key + "';";

		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector(Constants.CONCEPT_METADATA_TABLE + "__" + Constants.VALUE));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter(Constants.CONCEPT_METADATA_TABLE + "__" + Constants.KEY, "==", key));
		{
			SelectQueryStruct subQs = new SelectQueryStruct();
			// store first and fill in sub query after
			qs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery(Constants.CONCEPT_METADATA_TABLE + "__LOCALCONCEPTID", "==", subQs));

			// fill in the sub query with the necessary column output + filters
			subQs.addSelector(new QueryColumnSelector("CONCEPT__LOCALCONCEPTID"));
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPT__CONCEPTUALNAME", "==", concept));
			// we have a sub query again
			SelectQueryStruct subQs2 = new SelectQueryStruct();
			subQs.addExplicitFilter(SimpleQueryFilter.makeColToSubQuery("CONCEPT__LOCALCONCEPTID", "==", subQs2));

			// fill in the second sub query with the necessary column output + filters
			subQs2.addSelector(new QueryColumnSelector("ENGINECONCEPT__LOCALCONCEPTID"));
			subQs2.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseId));
		}

		return QueryExecutionUtility.flushToString(engine, qs);
	}


	/**
	 * Get all engine alias to id combinations
	 * @return
	 */
	public static List<String> getDatabaseIdsForAlias(String alias) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__ID"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ENGINENAME", "==", alias));
		
		return QueryExecutionUtility.flushToListString(engine, qs);
	}

	/**
	 * Get an engine type for an id
	 * @return
	 */
	public static String getDatabaseTypeForId(String id) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINE__TYPE"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINE__ID", "==", id));
		return QueryExecutionUtility.flushToString(engine, qs);
	}
	
	/**
	 * Try to reconcile and get the database id
	 * @param databaseId
	 * @return
	 */
	public static String testDatabaseIdIfAlias(String databaseId) {
		List<String> databaseIds = MasterDatabaseUtility.getDatabaseIdsForAlias(databaseId);
		if(databaseIds.size() == 1) {
			// actually received a database name
			databaseId = databaseIds.get(0);
		} else if(databaseIds.size() > 1) {
			throw new IllegalArgumentException("There are 2 databases with the name " + databaseId + ". Please pass in the correct id to know which source you want to load from");
		}

		// i guess the input was the actual id
		return databaseId;
	}

	/**
	 * Get a list of the conceptual names 
	 * @param databaseFilters optional filter based on engines
	 * @return
	 */
	public static Collection<String> getAllConceptualNames(Collection<String> databaseFilters) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CONCEPT__CONCEPTUALNAME"));
		QueryFunctionSelector fun = new QueryFunctionSelector();
		fun.addInnerSelector(new QueryColumnSelector("CONCEPT__CONCEPTUALNAME"));
		fun.setFunction(QueryFunctionHelper.LOWER);
		fun.setAlias("LNAME");
		qs.addSelector(fun);
		if(databaseFilters != null && !databaseFilters.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseFilters));
			qs.addRelation("ENGINECONCEPT", "CONCEPT", "inner.join");
		}
		qs.addOrderBy(new QueryColumnOrderBySelector("LNAME"));

		return QueryExecutionUtility.flushToListString(engine, qs);
	}
	
	
	/**
	 * Get a list of the conceptual names that are primary keys for a db
	 * @param databaseId
	 * @return
	 */
	public static Collection<String> getPKColumnsWithData(String databaseId) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseId));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__PK", "==", true, PixelDataType.BOOLEAN));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__IGNORE_DATA", "==", false, PixelDataType.BOOLEAN));

		return QueryExecutionUtility.flushToListString(engine, qs);
	}

	/**
	 * Get the conceptual names for a collection of physical name ids
	 * @param physicalNameIds
	 * @return
	 */
	public static List<String> getConceptualNamesFromPhysicalIds(List<String> physicalNameIds) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CONCEPT__CONCEPTUALNAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__PHYSICALNAMEID", "==", physicalNameIds));
		qs.addRelation("CONCEPT__LOCALCONCEPTID", "ENGINECONCEPT__LOCALCONCEPTID", "inner.join");
		
		return QueryExecutionUtility.flushToListString(engine, qs);
	}
	
	/**
	 * Get connections to other datasources based on similar conceptual names
	 * @param conceptualNames
	 * @param dbFilters
	 * @return
	 */
	public static List<String[]> getConceptualConnections(List<String> conceptualNames, Collection<String> dbFilters) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		List<String[]> results = new ArrayList<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__ENGINE"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PK"));
		if(dbFilters != null && !dbFilters.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", dbFilters));
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPT__CONCEPTUALNAME", "==", conceptualNames));
		qs.addRelation("CONCEPT__LOCALCONCEPTID", "ENGINECONCEPT__LOCALCONCEPTID", "inner.join");
		
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				
				String[] row = new String[2];
				row[0] = data[0].toString();
				if((Boolean) data[3]) {
					row[1] = data[1].toString();
				} else {
					row[1] = data[2] + "__" + data[1];
				}

				results.add(row);
			}
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}		
		
		return results;
	}
	
	/**
	 * Get the CLP model
	 * @param conceptualNames
	 * @param databaseFilters
	 * @return
	 */
	public static List<String[]> getConceptualToLogicalToPhysicalModel(List<String> conceptualNames, Collection<String> databaseFilters) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);

		List<String[]> results = new ArrayList<>();

		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("CONCEPT__CONCEPTUALNAME"));
		qs.addSelector(new QueryColumnSelector("CONCEPT__LOGICALNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__SEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PARENTSEMOSSNAME"));
		qs.addSelector(new QueryColumnSelector("ENGINECONCEPT__PK"));
		qs.addSelector(new QueryColumnSelector("ENGINE__ENGINENAME"));
		if(databaseFilters != null && !databaseFilters.isEmpty()) {
			qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ENGINECONCEPT__ENGINE", "==", databaseFilters));
		}
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("CONCEPT__CONCEPTUALNAME", "==", conceptualNames));
		qs.addRelation("CONCEPT__LOCALCONCEPTID", "ENGINECONCEPT__LOCALCONCEPTID", "inner.join");
		qs.addRelation("ENGINECONCEPT__ENGINE", "ENGINE__ID", "inner.join");
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				Object[] data = wrapper.next().getValues();
				
				String[] row = new String[4];
				row[0] = data[0].toString();
				row[1] = data[1].toString();
				if((Boolean) data[4]) {
					row[2] = data[2].toString();
				} else {
					row[2] = data[3] + "__" + data[2];
				}
				row[3] = data[5].toString();

				results.add(row);
			}
		} catch(Exception e) {
			logger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return results;
	}

	/**
	 * 
	 * @param sourceDB
	 * @param targetDB
	 * @return
	 */
	public static Map<String, List<String>> databaseTranslator(String sourceDB, String targetDB) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = null;
		try {
			conn = engine.makeConnection();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		// select logicalname from concept where conceptualname='MovieBudget'
		// and conceptualname != logicalname
		// select distinct c.conceptualname, ec.physicalname from concept c,
		// engineconcept ec, engine e where ec.localconceptid=c.localconceptid
		// and ec.physicalname in ('Title', 'Actor');
		Map<String, List<String>> map = new HashMap<>();
		ResultSet rs = null;
		Statement stmt = null;
		try {
			String query = "SELECT e.engineName as sourceEngine, c.conceptualName as sourceConceptual, ec.physicalName as sourcePhysical, c.logicalName, "
					+ "targetEngine, targetConceptual, targetPhysical from engine e, engineconcept ec, concept c  "
					+ "INNER JOIN (SELECT e.engineName as targetEngine, c.conceptualName as targetConceptual, "
					+ "ec.physicalName as targetPhysical, c.logicalName as targetLogical "
					+ "from engine e, engineconcept ec, concept c WHERE e.id=ec.engine and ec.localConceptID = c.localConceptID and e.id = '"
					+ targetDB + "' " + "and c.conceptualName != c.logicalName) ON c.logicalName = targetLogical "
					+ "WHERE e.id=ec.engine and ec.localConceptID = c.localConceptID and e.id = '" + sourceDB
					+ "' and c.conceptualName != c.logicalName";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				String sourceEngine = rs.getString(1);
				String sourceConceptual = rs.getString(2);
				String sourcePhysical = rs.getString(3);
				String logicalName = rs.getString(4);
				String targetEngine = rs.getString(5);
				String targetConceptual = rs.getString(6);
				String targetPhysical = rs.getString(7);

				List<String> targetPhysicals = new ArrayList<>();
				if (map.containsKey(sourcePhysical)) {
					targetPhysicals = map.get(sourcePhysical);
				}
				targetPhysicals.add(targetPhysical);

				map.put(sourcePhysical, targetPhysicals);
			}
		} catch (Exception ex) {
			logger.error(Constants.STACKTRACE, ex);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, stmt, rs);
		}
		return map;
	}
	
	
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////


	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	//	/*
	//	 * LEGACY LOGIC NAME ALTERATIONS
	//	 * NOW ALTERATIONS GO THROUGH THE OWL AND THEN ARE RELOADED INTO THE LOCAL MASTER
	//	 */
	//	
	//	
	//
	//	public static boolean deleteMetaValue(String engineName, String concept, String key) {
	//		boolean deleted = false;
	//		String localConceptID = MasterDatabaseUtility.getLocalConceptID(engineName, concept);
	//		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
	//		Connection conn = engine.makeConnection();
	//		Statement stmt = null;
	//		int count = 0;
	//		try {
	//			String deleteQuery = "DELETE FROM " + Constants.CONCEPT_METADATA_TABLE + " WHERE "
	//					+ Constants.PHYSICAL_NAME_ID + " = \'" + localConceptID + "\' and " + Constants.KEY + " = \'" + key
	//					+ "\';";
	//			stmt = conn.createStatement();
	//			count = stmt.executeUpdate(deleteQuery);
	//			if (count > 0) {
	//				deleted = true;
	//			}
	//		} catch (Exception ex) {
	//			ex.printStackTrace();
	//		} finally {
	//			closeStreams(stmt, null);
	//		}
	//
	//		return deleted;
	//	}
	//
	//	public static boolean deleteMetaValue(String engineName, String concept, String key, String value) {
	//		boolean deleted = false;
	//		String localConceptID = MasterDatabaseUtility.getPhysicalConceptId(engineName, concept);
	//		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
	//		Connection conn = engine.makeConnection();
	//		Statement stmt = null;
	//		int count = 0;
	//		try {
	//			String deleteQuery = "DELETE FROM " + Constants.CONCEPT_METADATA_TABLE + " WHERE "
	//					+ Constants.PHYSICAL_NAME_ID + " = \'" + localConceptID + "\' and " + Constants.KEY + " = \'" + key
	//					+ "\' and " + Constants.VALUE + " = \'" + value + "\';";
	//			stmt = conn.createStatement();
	//			count = stmt.executeUpdate(deleteQuery);
	//			if (count > 0) {
	//				deleted = true;
	//			}
	//		} catch (Exception ex) {
	//			ex.printStackTrace();
	//		} finally {
	//			try {
	//				if (stmt != null) {
	//					stmt.close();
	//				}
	//			} catch (SQLException e) {
	//				e.printStackTrace();
	//			}
	//		}
	//
	//		return deleted;
	//	}
	//	
	//	
	//	/**
	//	 * Adds logical name to concept from engine
	//	 * 
	//	 * @param engineId
	//	 * @param concept
	//	 * @param logicalName
	//	 * @return
	//	 */
	//	public static boolean addLogicalName(String engineId, String concept, String logicalName) {
	//		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
	//		Connection masterConn = engine.makeConnection();
	//		Statement stmt = null;
	//		ResultSet rs = null;
	//		int size = 0;
	//		try {
	//			String duplicateQueryCheck = "select localconceptid, conceptualname, logicalname, "
	//					+ "domainname, globalid from concept "
	//					+ "where localconceptid in (select localconceptid from engineconcept "
	//					+ "where engine='" + engineId + "') "
	//					+ "and conceptualname='" + concept + "' and logicalname='" + logicalName + "';";
	//			stmt = masterConn.createStatement();
	//			rs = stmt.executeQuery(duplicateQueryCheck);
	//			if (rs != null) {
	//				rs.beforeFirst();
	//				rs.last();
	//				size = rs.getRow();
	//			}
	//		} catch (SQLException e) {
	//			e.printStackTrace();
	//		} finally {
	//			closeStreams(stmt, rs);
	//		}
	//		
	//		try {
	//			if (size == 0) {
	//				String sourceLogicalInfo = "select localconceptid, conceptualname, logicalname, "
	//						+ "domainname, globalid from concept "
	//						+ "where localconceptid in (select localconceptid from engineconcept "
	//						+ "where engine='" + engineId + "') "
	//						+ "and conceptualname='" + concept + "'";
	//				if (stmt == null || stmt.isClosed()) {
	//					stmt = masterConn.createStatement();
	//				}
	//				rs = stmt.executeQuery(sourceLogicalInfo);
	//				while (rs.next()) {
	//					String localConceptID = rs.getString(1);
	//					String conceptualName = rs.getString(2);
	//					String oldLogicalName = rs.getString(3);
	//					String domainName = rs.getString(4);
	//					String globalID = rs.getString(5);
	//					if (conceptualName.equals(concept)) {
	//						// insert target CN as logical name
	//						String insertString = "insert into concept " + "values('" + localConceptID + "', '"
	//								+ conceptualName + "', '" + logicalName + "\', \'" + domainName + "', '"
	//								+ globalID.toString() + "');";
	//						int validInsert = masterConn.createStatement().executeUpdate(insertString);
	//						if (validInsert > 0) {
	//							try {
	//								engine.commitRDBMS();
	//								return true;
	//							} catch (Exception e) {
	//								e.printStackTrace();
	//							}
	//						}
	//					}
	//				}
	//			} else {
	//				return true;
	//			}
	//		} catch (SQLException e) {
	//			e.printStackTrace();
	//		} finally {
	//			closeStreams(stmt, rs);
	//		}
	//		return false;
	//	}
	//
	//	/**
	//	 * Removes logical name for a concept from an engine
	//	 * 
	//	 * @param engineName
	//	 * @param concept
	//	 * @param logicalName
	//	 * @return success
	//	 */
	//	public static boolean removeLogicalName(String engineName, String concept, String logicalName) {
	//		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
	//		Connection masterConn = engine.makeConnection();
	//		Statement stmt = null;
	//		
	//		try {
	//			String deleteQuery = "delete from concept "
	//					+ "where localconceptid in (select localconceptid from engineconcept "
	//					+ "where engine='" + engineName + "')"
	//					+ "and conceptualname='" + concept + "' and logicalname='" + logicalName + "'";
	//			stmt = masterConn.createStatement();
	//			int updateCount = stmt.executeUpdate(deleteQuery);
	//			if (updateCount == 1) {
	//				return true;
	//			}
	//		} catch (SQLException e) {
	//			e.printStackTrace();
	//		} finally {
	//			closeStreams(stmt, null);
	//		}
	//		return false;
	//	}
	//
	//	/**
	//	 * Get logical names for a specific engine and concept
	//	 * 
	//	 * @param engineId
	//	 * @param concept
	//	 * @return logicalNames
	//	 */
	//	public static List<String> getLogicalNames(String engineId, String concept) {
	//		List<String> logicalNames = new ArrayList<String>();
	//		
	//		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
	//		Connection masterConn = engine.makeConnection();
	//		Statement stmt = null;
	//		ResultSet rs = null;
	//
	//		try {
	//			String query = "select logicalname from concept "
	//					+ "where localconceptid in (select localconceptid from engineconcept "
	//					+ "where engine='" + engineId + "')"
	//					+ "and conceptualname='" + concept + "'";
	//			
	//			stmt = masterConn.createStatement();
	//			rs = stmt.executeQuery(query);
	//			while (rs.next()) {
	//				String logicalName = rs.getString(1);
	//				logicalNames.add(logicalName);
	//			}
	//		} catch (SQLException e) {
	//			e.printStackTrace();
	//		} finally {
	//			closeStreams(stmt, rs);
	//		}
	//		return logicalNames;
	//	}
	//	

	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

	/*
	 * X-RAY Stuff
	 */


	/**
	 * Returns Xray config files
	 * 
	 * @return
	 */
	public static HashMap<String, Object> getXrayConfigList() {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = null;
		try {
			conn = engine.makeConnection();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		Statement stmt = null;
		ResultSet rs = null;
		HashMap<String, Object> configMap = new HashMap<>();
		try {
			String query = "select distinct filename FROM xrayconfigs;";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			ArrayList<HashMap<String, Object>> configList = new ArrayList<>();
			while (rs.next()) {
				HashMap<String, Object> rsMap = new HashMap<>();
				String fileName = rs.getString(1);
				rsMap.put("fileName", fileName);
				configList.add(rsMap);
			}
			configMap.put("configList", configList);
		} catch (SQLException ex) {
			// Don't print stack trace... xrayConfigList table is missing if no config file exists
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, stmt, rs);
		}
		return configMap;
	}

	/**
	 * Gets the xray config file
	 * 
	 * @param filename
	 * @return
	 */
	
	public static String getXrayConfigFile(String filename) {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = null;
		try {
			conn = engine.makeConnection();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		Statement stmt = null;
		ResultSet rs = null;
		String configFile = "";
		try {
			String query = "select config from xrayconfigs where filename = \'" + filename + "\';";
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				configFile = rs.getString(1);
			}
		} catch (Exception ex) {
			logger.error(Constants.STACKTRACE, ex);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, stmt, rs);
		}
		return configFile;
	}


	/**
	 * specific format for xray merging db.tablename for nodes
	 * 
	 * @param engineName
	 * @return
	 */
	public static Map<String, Object> getXrayExisitingMetamodelRDBMS(String engineName) {
		// this needs to be moved to the name server
		// and this needs to be based on local master database
		// need this to be a simple OWL data
		// I dont know if it is worth it to load the engine at this point ?
		// or should I just load it ?
		// need to get local master and pump out the metamodel

		// need to get all the concepts first
		// get the edges next

		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = null;
		try {
			conn = engine.makeConnection();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		Statement stmt = null;
		ResultSet rs = null;
		// creates e-c-p node names for fe to parse
		String delim = "-";
		Map<String, Object> finalHash = new Hashtable<>();

		// idHash - physical ID to the name of the node
		Hashtable<String, String> idHash = new Hashtable<>();
		Hashtable<String, MetamodelVertex> nodeHash = new Hashtable<>();

		String nodeQuery = "SELECT c.conceptualname, ec.physicalname, ec.localconceptid, ec.physicalnameid, ec.parentphysicalid, ec.property FROM "
				+ "engineconcept ec, concept c, engine e " + "WHERE ec.engine=e.id " + "AND e.enginename=? "
				+ "AND c.localconceptid=ec.localconceptid ORDER BY ec.property";
		try(PreparedStatement statement = conn.prepareStatement(nodeQuery)){
//			String nodeQuery = "select c.conceptualname, ec.physicalname, ec.localconceptid, ec.physicalnameid, ec.parentphysicalid, ec.property from "
//					+ "engineconcept ec, concept c, engine e " + "where ec.engine=e.id " + "and e.enginename='"
//					+ engineName + "' " + "and c.localconceptid=ec.localconceptid order by ec.property";
			
			statement.setString(1, engineName );
			//stmt = conn.createStatement();
			//rs = stmt.executeQuery(nodeQuery);
			rs = statement.executeQuery();
			while (rs.next()) {
				String conceptualName = rs.getString(1);
				String physicalName = rs.getString(2);
				String physicalId = rs.getString(4);
				String parentPhysicalId = rs.getString(5);

				// sets the physical id to conceptual name
				idHash.put(physicalId, conceptualName);

				MetamodelVertex node = null;

				// gets the conceptual name
				String conceptName = idHash.get(physicalId);

				// because it is ordered by property, this would already be
				// there
				String parentName = idHash.get(parentPhysicalId);

				// if already there, should we still add it ?
				if (nodeHash.containsKey(engineName + delim + parentName))
					node = nodeHash.get(engineName + delim + parentName);
				else {
					node = new MetamodelVertex(engineName + delim + parentName);
					nodeHash.put(engineName + delim + conceptualName, node);
				}

				//				if (!conceptName.equalsIgnoreCase(parentName)) {
				// might be this or might not be
				// node.addProperty(engineName+ "." +conceptName);
				node.addProperty(conceptName);
				//				}
			}
		} catch (SQLException ex) {
			logger.error(Constants.STACKTRACE, ex);
		} finally {
			// do not close the stmt
			// reuse it below
			ConnectionUtils.closeAllConnectionsIfPooling(null, null, null, rs);
		}

		String edgeQuery = "SELECT er.sourceconceptid, er.targetconceptid FROM ENGINERELATION er, engine e WHERE e.id=er.engine AND "
				+ "e.enginename = ?";
		try(PreparedStatement statement = conn.prepareStatement(edgeQuery)) {
			// get the edges next
			// SELECT er.sourceconceptid, er.targetconceptid FROM ENGINERELATION
			// er, engine e where e.id = er.engine and e.enginename = 'Mv1'
//			String edgeQuery = "SELECT er.sourceconceptid, er.targetconceptid FROM ENGINERELATION er, engine e where e.id = er.engine and "
//					+ "e.enginename = '" + engineName + "'";
			
			
			statement.setString(1, engineName);
			if (stmt == null) {
				stmt = conn.createStatement();
			}
			//rs = stmt.executeQuery(edgeQuery);
			rs = statement.executeQuery();

			Hashtable<String, Hashtable> edgeHash = new Hashtable<>();
			while (rs.next()) {
				String startId = rs.getString(1);
				String endId = rs.getString(2);

				Hashtable newEdge = new Hashtable();
				// need to check to see if the idHash has it else put it in
				String sourceName = idHash.get(startId);
				String targetName = idHash.get(endId);
				newEdge.put("source", engineName + delim + sourceName + delim + sourceName);
				newEdge.put("target", engineName + delim + targetName + delim + targetName);

				// if(nodeHash.containsKey(toId))

				boolean foundNode = true;
				if (!nodeHash.containsKey(engineName + delim + sourceName)) {
					foundNode = false;
					logger.debug("Unable to find node " + sourceName);
				}
				if (!nodeHash.containsKey(engineName + delim + targetName)) {
					foundNode = false;
					logger.debug("Unable to find node " + targetName);
				}

				if (foundNode) {
					edgeHash.put(engineName + delim + sourceName + delim + sourceName + delim + engineName + delim+ targetName + delim + targetName, newEdge);
				}
			}
			finalHash.put("nodes", nodeHash);
			finalHash.put("edges", edgeHash);

		} catch (SQLException ex) {
			logger.error(Constants.STACKTRACE, ex);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, stmt, rs);
		}

		return finalHash;
	}
	
    /**
     * Get the date for a given engine
     *
     * @param engineId
     * @return
     */
    public static Date getEngineDate(String engineId) {
        java.util.Date retDate = null;
        IRDBMSEngine engine = (IRDBMSEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
        	conn = engine.makeConnection();
            String query = "select modifieddate from engine e where e.id = '" + engineId + "'";
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);
            while (rs.next()) {
                java.sql.Timestamp modDate = rs.getTimestamp(1);
                retDate = new java.util.Date(modDate.getTime());
            }
        } catch (Exception ex) {
            logger.error(Constants.STACKTRACE, ex);
        } finally {
			ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, stmt, rs);
		}
        return retDate;
    }
    
    /**
     * 
     * @param databaseId
     * @param positions
     */
    public static void saveMetamodelPositions(String databaseId, Map<String, Object> positions) {
        IRDBMSEngine engine = (IRDBMSEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
        AbstractSqlQueryUtil queryUtil = engine.getQueryUtil();
        Connection conn = null;
        Savepoint savepoint = null;
        try {
        	conn = engine.getConnection();
        	if(!conn.getAutoCommit()) {
        		savepoint = conn.setSavepoint("mm_position_" + Utility.getRandomString(5));
        	}
        	saveMetamodelPositions(databaseId, positions, conn);
        	if(!conn.getAutoCommit()) {
        		conn.commit();
        	}
        } catch(Exception e) {
        	logger.error(Constants.STACKTRACE, e);
        	try {
        		if(savepoint != null) {
        			conn.rollback(savepoint);
        		}
			} catch (SQLException e1) {
	        	logger.error(Constants.STACKTRACE, e);
			}
        } finally {
        	if(savepoint != null && !queryUtil.savePointAutoRelease()) {
        		try {
					conn.releaseSavepoint(savepoint);
				} catch (SQLException e) {
		        	logger.error(Constants.STACKTRACE, e);
				}
        	}
        	ConnectionUtils.closeAllConnectionsIfPooling(engine, conn, null, null);
        }
    }
    
    /**
     * It is your responsibility to close the connection object if connection pooling and using this method
     * @param databaseId
     * @param positions
     * @param conn
     * @throws Exception
     */
    public static void saveMetamodelPositions(String databaseId, Map<String, Object> positions, Connection conn) throws Exception {
    	String removeExisting = "DELETE FROM METAMODELPOSITION where ENGINEID = ?";
    	String insertStatement = "INSERT INTO METAMODELPOSITION VALUES (?, ?, ?, ?)";
        
        PreparedStatement remove = null;
        PreparedStatement add = null;
    	try {
    		remove = conn.prepareStatement(removeExisting);
    		remove.setString(1, databaseId);
    		remove.execute();

    		add = conn.prepareStatement(insertStatement);
    		for (String x : positions.keySet()) {
    			int i = 1;
    			add.setString(i++, databaseId);
    			add.setString(i++, x);
    			Map<String, Object> topLeft = (Map<String, Object>) positions.get(x);
    			Float left = convertToFloat(topLeft.get("left"));
    			Float top = convertToFloat(topLeft.get("top"));
    			add.setFloat(i++, left);
    			add.setFloat(i++, top);
    			add.addBatch();
    		}
    		
    		add.executeBatch();
    	} catch (Exception e) {
    		logger.error("Could save metamodel positions", e);
    		throw e;
    	} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(null, null, remove, null);
			ConnectionUtils.closeAllConnectionsIfPooling(null, null, add, null);
    	}
    }
    
    public static Map<String, Object> getMetamodelPositions(String databaseId) {
        IRDBMSEngine engine = (IRDBMSEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB_NAME);
    	SelectQueryStruct qs = new SelectQueryStruct();
    	qs.addSelector(new QueryColumnSelector("METAMODELPOSITION__TABLENAME"));
    	qs.addSelector(new QueryColumnSelector("METAMODELPOSITION__XPOS"));
    	qs.addSelector(new QueryColumnSelector("METAMODELPOSITION__YPOS"));
    	qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("METAMODELPOSITION__ENGINEID", "==", databaseId));
    	
    	List<Object[]> objs = QueryExecutionUtility.flushRsToListOfObjArray(engine, qs);
    	
    	Map<String, Object> map = new HashMap<>();
    	for (Object[] x : objs) {
    		String tn = (String) x[0];
    		Map<String, Object> position = new HashMap<>();
    		position.put("left", x[1]);
    		position.put("top", x[2]);
    		map.put(tn, position);
    	}
    	return map;
    }

	private static Float convertToFloat(Object object) {
		if (object instanceof Double) {
			Double db = (Double) object;
			return db.floatValue();
		} else if (object instanceof Integer) {
			return ((Integer) object).floatValue();
		} else {
			return Float.valueOf(object.toString());
		}
	}

	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////

//	public static void main(String[] args) throws Exception {
//		TestUtilityMethods.loadAll("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
//
//		List<String> pixelNames = new Vector<>();
//		pixelNames.add("Studio");
//		List<String> ids = getLocalConceptIdsFromPixelName(pixelNames);
//		
//		Gson gson = new GsonBuilder()
//				.disableHtmlEscaping()
//				.excludeFieldsWithModifiers(Modifier.STATIC, Modifier.TRANSIENT)
//				.setPrettyPrinting()
//				.create();
//
//		List<String> values = null;
//		logger.debug(gson.toJson(getDatabaseConnections(ids, values)));
//		
////		System.out.println(gson.toJson(getPKColumnsWithData("2da0688f-fc35-4427-aba5-7bd7b7ac9472"))); 
////		System.out.println(gson.toJson(getPKColumnsWithData("67b6499d-03b2-463f-9169-396f4cce8955"))); 
////		System.out.println(gson.toJson(getPKColumnsWithData("3cbd547f-9ff9-43bc-9b59-a4d170c45b26"))); 
//		
//	}

}