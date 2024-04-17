package prerna.reactor.database.physicaleditor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
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
	private static final String NAME_CONVERSION_KEY = "nameConversions";
	private static final String TYPE_CONVERSION_KEY = "typeConversions";
	private static final String FUZZY_CONVERSION_KEY = "fuzzyConversions";

	public CopyDatabaseTableSchemaReactor() {
		this.keysToGet = new String[]{ 
				SOURCE_DATABASE,
				SOURCE_TABLE,
				TARGET_DATABASE, 
				TARGET_TABLE, 
				ReactorKeysEnum.OVERRIDE.getKey(), 
				IGNORE_OWL,
				NAME_CONVERSION_KEY,
				TYPE_CONVERSION_KEY,
				FUZZY_CONVERSION_KEY
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
		
		Map<String, String> nameConversionMap = getConversions(NAME_CONVERSION_KEY);
		Map<String, String> typeConversionMap = getConversions(TYPE_CONVERSION_KEY);
		Map<String, String> fuzzyConversionMap = getConversions(FUZZY_CONVERSION_KEY);

		boolean override = Boolean.parseBoolean( this.keyValue.get(ReactorKeysEnum.OVERRIDE.getKey())+"" );
		boolean ignoreOWL = Boolean.parseBoolean( this.keyValue.get(IGNORE_OWL)+"" );
		
		// grab the details from the source
		LinkedHashMap<String, String> finalTypes = new LinkedHashMap<>();
		LinkedHashMap<String, String> sourceDetails = getColumnDetails(sourceRDBMS, sourceTable);
		REPLACEMENT_LOOP : for(String name : sourceDetails.keySet()) {
			String currentType = sourceDetails.get(name);
			// we do in order
			// did the person give this name a new type
			// is the exact type getting a new type
			// or are we doing a fuzzy replacement
			// finally, no change, set as normal
			
			// by name
			if(nameConversionMap != null){
				if(nameConversionMap.containsKey(name)) {
					String newType = nameConversionMap.get(name);
					finalTypes.put(name, newType);
					continue REPLACEMENT_LOOP;
				}
			}

			// by type
			if(typeConversionMap != null) {
				if(typeConversionMap.containsKey(currentType)) {
					String newType = typeConversionMap.get(currentType);
					finalTypes.put(name, newType);
					continue REPLACEMENT_LOOP;
				}
			}
			
			// fuzzy matching
			if(fuzzyConversionMap != null) {
				for(String thisFuzzy : fuzzyConversionMap.keySet()) {
					String replacement = fuzzyConversionMap.get(thisFuzzy);
					
					// see if this fuzzy match hits the current type
					Pattern pattern = Pattern.compile(thisFuzzy, Pattern.CASE_INSENSITIVE);
					// test the matcher with the current type
					Matcher matcher = pattern.matcher(currentType);
					boolean matchFound = matcher.find();
					if(matchFound) {
						String newType = currentType.replace(matcher.group(), replacement);
						finalTypes.put(name, newType);
						continue REPLACEMENT_LOOP;
					}
				}
			}
			
			// nothing matched
			// just add
			finalTypes.put(name, currentType);
		}
		
		// write it to the target
		String createTable = targetRDBMS.getQueryUtil().createTable(targetTable, finalTypes);
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
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		if(ignoreOWL) {
			logger.info("Ignoring any OWL modifications");
		} else {
			try {
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
			} catch (Exception e2) {
				classLogger.error(Constants.STACKTRACE, e2);
				noun.addAdditionalReturn(NounMetadata.getWarningNounMessage("Physical database changed but unable to update OWL. Detailed message = " + e2.getMessage()));
			}
		}
		// push the target
		ClusterUtil.pushEngine(targetDatabaseId);

		return noun;
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
	
	/**
	 * 
	 * @return
	 */
	private Map<String, String> getConversions(String key) {
		GenRowStruct mapGrs = this.store.getNoun(key);
		if(mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if(mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, String>) mapInputs.get(0).getValue();
			}
		}
		return null;
	}
	
}
