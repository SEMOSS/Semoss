package prerna.sablecc2.reactor.database.physicaleditor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityDatabaseUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.impl.util.Owler;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.masterdatabase.SyncDatabaseWithLocalMasterReactor;
import prerna.util.Constants;
import prerna.util.EngineSyncUtility;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class ModifyDatabaseStructureReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(ModifyDatabaseStructureReactor.class);
	
	public static final String ADDITIONS = "add";
	
	public ModifyDatabaseStructureReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey(), ADDITIONS};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		if(AbstractSecurityUtils.securityEnabled()) {
			databaseId = SecurityQueryUtils.testUserDatabaseIdForAlias(this.insight.getUser(), databaseId);
			if(!SecurityDatabaseUtils.userCanEditDatabase(this.insight.getUser(), databaseId)) {
				throw new IllegalArgumentException("Database" + databaseId + " does not exist or user does not have access to database");
			}
		} else {
			databaseId = MasterDatabaseUtility.testDatabaseIdIfAlias(databaseId);
			if(!MasterDatabaseUtility.getAllDatabaseIds().contains(databaseId)) {
				throw new IllegalArgumentException("Database " + databaseId + " does not exist");
			}
		}
		
		
		// table > column > type
		Map<String, Map<String, String>> updates = getAdditions();
		
		IEngine engine = Utility.getEngine(databaseId);
		if(!(engine instanceof IRDBMSEngine)) {
			throw new IllegalArgumentException("This operation only works on relational databases");
		}
		ClusterUtil.reactorPullOwl(databaseId);

		Set<String> tableExists = new HashSet<>();

		IRDBMSEngine rdbmsDb = (IRDBMSEngine) engine;
		AbstractSqlQueryUtil queryUtil = rdbmsDb.getQueryUtil();
		Map<String, String> typeConversionMap = queryUtil.getTypeConversionMap();
		
		try {
			Connection conn = rdbmsDb.getConnection();
			String database = rdbmsDb.getDatabase();
			String schema = rdbmsDb.getSchema();
			
			// first run a validation on the input
			for(String tableName : updates.keySet()) {
				if(queryUtil.tableExists(engine, tableName, database, schema)) {
					// we are altering - make sure everything is valid
					
					List<String> currentColumns = queryUtil.getTableColumns(conn, tableName, database, schema);
					Set<String> currentColumnsLower = currentColumns.stream().map(s -> s.toLowerCase()).collect(Collectors.toSet());
					
					Set<String> newColumns = updates.get(tableName).keySet();
					Set<String> newColumnsLower = newColumns.stream().map(s -> s.toLowerCase()).collect(Collectors.toSet());
					
					Set<String> overlap = newColumnsLower.stream().filter(s -> currentColumnsLower.contains(s)).collect(Collectors.toSet());
					if(!overlap.isEmpty()) {
						throw new IllegalArgumentException("The following column names already exist: " + overlap);
					}

					tableExists.add(tableName);
				}
			}
		} catch(SQLException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error validating the input. Detailed message = " + e.getMessage());
		}
		
		// create an owler to track the meta modifications
		Owler owler = new Owler(rdbmsDb);
		
		StringBuilder errorMessages = new StringBuilder();
		// now do the operations
		for(String tableName : updates.keySet()) {

			Map<String, String> finalColumnUpdates = new HashMap<>(updates.size());

			Map<String, String> columnUpdates = updates.get(tableName);
			for(String column : columnUpdates.keySet()) {

				String columnType = columnUpdates.get(column).toUpperCase();
				if(typeConversionMap.containsKey(columnType)) {
					columnType = typeConversionMap.get(columnType);
				}

				finalColumnUpdates.put(column, columnType);
			}

			String query = null;
			if(tableExists.contains(tableName)) {
				query = queryUtil.alterTableAddColumns(tableName, finalColumnUpdates);
			} else {
				// make new table
				query = queryUtil.createTable(tableName, finalColumnUpdates);
			}
			try {
				rdbmsDb.insertData(query);
				
				// add to the owl
				owler.addConcept(tableName, null, null);
				for(String column : finalColumnUpdates.keySet()) {
					String columnType = finalColumnUpdates.get(column);
					owler.addProp(tableName, column, columnType);
				}
			} catch(Exception e) {
				logger.error(Constants.STACKTRACE, e);
				errorMessages.append("Error executing query = '" + query +"' with detailed error = " + e.getMessage() + ". ");
			}
		}
		
		// now push the OWL and sync
		try {
			owler.export();
			SyncDatabaseWithLocalMasterReactor syncWithLocal = new SyncDatabaseWithLocalMasterReactor();
			syncWithLocal.setInsight(this.insight);
			syncWithLocal.setNounStore(this.store);
			syncWithLocal.In();
			syncWithLocal.execute();
		} catch (IOException e) {
			logger.error(Constants.STACKTRACE, e);
			NounMetadata noun = new NounMetadata(false, PixelDataType.BOOLEAN);
			noun.addAdditionalReturn(getError("Error occurred savig the metadata file with the executed changes"));
			if(errorMessages.length() > 0) {
				noun.addAdditionalReturn(getError(errorMessages.toString()));
			}
			return noun;
		}
		EngineSyncUtility.clearEngineCache(databaseId);
		ClusterUtil.reactorPushOwl(databaseId);
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		if(errorMessages.length() > 0) {
			noun.addAdditionalReturn(getError(errorMessages.toString()));
		}
		
		return noun;
	}

	private Map<String, Map<String, String>> getAdditions() {
		GenRowStruct mapGrs = this.store.getNoun(ADDITIONS);
		if(mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if(mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Map<String, String>>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if(mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Map<String, String>>) mapInputs.get(0).getValue();
		}

		throw new IllegalArgumentException("Must define the map containing {tablename:{columnname:datatype}} for the additions");
	}
	
}
