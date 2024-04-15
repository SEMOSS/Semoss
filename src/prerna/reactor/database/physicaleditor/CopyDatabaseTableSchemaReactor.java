package prerna.reactor.database.physicaleditor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class CopyDatabaseTableSchemaReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CopyDatabaseTableSchemaReactor.class);
	private static final String CLASS_NAME = CopyDatabaseTableSchemaReactor.class.getName();

	private static final String SOURCE_DATABASE = "sourceDatabase";
	private static final String SOURCE_TABLE = "sourceTable";
	
	private static final String TARGET_DATABASE = "targetDatabase";
	private static final String TARGET_TABLE = "targetTable";
	
	private static final String IGNORE_OWL = "ignoreOWL";

	public CopyDatabaseTableSchemaReactor() {
		this.keysToGet = new String[]{ 
				SOURCE_DATABASE,
				SOURCE_TABLE,
				TARGET_DATABASE, 
				TARGET_TABLE, 
				ReactorKeysEnum.OVERRIDE.getKey(), 
				IGNORE_OWL
			};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = this.getLogger(CLASS_NAME);

		organizeKeys();
		
		User user = this.insight.getUser();
		// throw error is user doesn't have rights to export data
		if(AbstractSecurityUtils.adminSetExporter() && !SecurityQueryUtils.userIsExporter(user)) {
			AbstractReactor.throwUserNotExporterError();
		}
		
		// grab the source
		String sourceDatabaseId = this.keyValue.get(SOURCE_DATABASE);
		sourceDatabaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), sourceDatabaseId);
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), sourceDatabaseId)) {
			throw new IllegalArgumentException("Database " + sourceDatabaseId + " does not exist or user does not have view access to the database");
		}
		String sourceTable = this.keyValue.get(SOURCE_TABLE);

		// grab the target
		String targetDatabaseId = this.keyValue.get(TARGET_DATABASE);
		targetDatabaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), targetDatabaseId);
		if(!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), targetDatabaseId)) {
			throw new IllegalArgumentException("Database " + targetDatabaseId + " does not exist or user does not have edit access to the database");
		}
		String targetTable = this.keyValue.get(TARGET_TABLE);

		IDatabaseEngine sourceDatabase = Utility.getDatabase(sourceDatabaseId);
		if(!(sourceDatabase instanceof IRDBMSEngine)) {
			throw new IllegalArgumentException("This operation only works on relational databases");
		}
		IRDBMSEngine sourceRDBMS = (IRDBMSEngine) sourceDatabase;
		
		IDatabaseEngine targetDatabase = Utility.getDatabase(targetDatabaseId);
		if(!(targetDatabase instanceof IRDBMSEngine)) {
			throw new IllegalArgumentException("This operation only works on relational databases");
		}
		IRDBMSEngine targetRDBMS = (IRDBMSEngine) targetDatabase;
		
		boolean override = Boolean.parseBoolean( this.keyValue.get(ReactorKeysEnum.OVERRIDE.getKey())+"" );
		boolean ignoreOWL = Boolean.parseBoolean( this.keyValue.get(IGNORE_OWL)+"" );
		
		// grab the details from the source
		LinkedHashMap<String, String> sourceDetails = getColumnDetails(sourceRDBMS, sourceTable);
		// write it to the target
		String createTable = targetRDBMS.getQueryUtil().createTable(targetTable, sourceDetails);
		// only attempt to drop if the table already exists
		if(override && targetRDBMS.getQueryUtil().tableExists(targetRDBMS, targetTable, targetRDBMS.getDatabase(), targetRDBMS.getSchema())) {
			try {
				targetRDBMS.removeData(targetRDBMS.getQueryUtil().dropTable(targetTable));
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error occured creating the table in the target. Detailed message = " + e.getMessage());
			}
		}
		try {
			targetRDBMS.insertData(createTable);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occured creating the table in the target. Detailed message = " + e.getMessage());
		}
		
		if(ignoreOWL) {
			logger.info("Ignoring any OWL modifications");
		} else {
			long start = System.currentTimeMillis();
			logger.info("Start to add the new exisitng concept from the OWL");
			try (WriteOWLEngine owlEngine = targetDatabase.getOWLEngineFactory().getWriteOWL()){
				ClusterUtil.pullOwl(targetDatabaseId, owlEngine);

				// if we are overwriting the existing value
				// we need to grab the current concept/columns
				// and we need to delete them
				// then do the add new table logic
				{
					long start2 = System.currentTimeMillis();
					logger.info("Need to first remove the exisitng concept from the OWL");
					owlEngine.removeConcept(targetTable);
					long end2 = System.currentTimeMillis();
					logger.info("Finished removing concept from the OWL. Total time = "+((end2-start2)/1000)+" seconds");
				}
				// choose the first column as the prim key
				owlEngine.addConcept(targetTable, null, null);
				// add all properties
				for(String columnName : sourceDetails.keySet()) {
					owlEngine.addProp(targetTable, columnName, sourceDetails.get(columnName), null);
				}
				
				try {
					logger.info("Persisting engine metadata and synchronizing with local master");
					owlEngine.export();
					Utility.synchronizeEngineMetadata(targetDatabaseId);
					// also push to cloud
					ClusterUtil.pushOwl(targetDatabaseId, owlEngine);
					EngineSyncUtility.clearEngineCache(targetDatabaseId);
					logger.info("Finished persisting engine metadata and synchronizing with local master");
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}

			} catch (InterruptedException | IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
			long end = System.currentTimeMillis();
			logger.info("Finished adding concept to the OWL. Total time = "+((end-start)/1000)+" seconds");
		}
		// push the target
		ClusterUtil.pushEngine(targetDatabaseId);

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE_ADDITION, PixelOperationType.FORCE_SAVE_DATA_EXPORT);
	}
	
	/**
	 * 
	 * @param databaseId
	 * @param table
	 * @return
	 */
	private LinkedHashMap<String, String> getColumnDetails(IRDBMSEngine rdbms, String table) {
		AbstractSqlQueryUtil queryUtil = rdbms.getQueryUtil();
		Connection con = null;
		try {
			con = rdbms.getConnection();
			// the final map
			LinkedHashMap<String, String> columnDetails = queryUtil.getAllTableColumnTypesSimple(con, table, rdbms.getDatabase(), rdbms.getSchema());
			return columnDetails;
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occured getting the physical table structure. Detailed message = " + e.getMessage());
		} finally {
			if(rdbms.isConnectionPooling()) {
				ConnectionUtils.closeConnection(con);
			}
		}
	}
	
}
