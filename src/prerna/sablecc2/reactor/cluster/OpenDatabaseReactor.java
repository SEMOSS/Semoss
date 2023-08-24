package prerna.sablecc2.reactor.cluster;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class OpenDatabaseReactor extends AbstractReactor {
	
	public OpenDatabaseReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		
		if(databaseId == null || databaseId.isEmpty()) {
			throw new IllegalArgumentException("Must input an database id");
		}
		
		if(databaseId.equals("NEWSEMOSSAPP")) {
			Map<String, Object> returnMap = new HashMap<String, Object>();
			returnMap.put("database_name", "NEWSEMOSSAPP");
			returnMap.put("database_id", databaseId);
			returnMap.put("database_type", IDatabaseEngine.DATABASE_TYPE.APP.toString());
			returnMap.put("database_subtype", "");
			returnMap.put("database_cost", "");	
			return new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPEN_DATABASE);
		}
		
		// make sure valid id for user
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
		if( !(SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId) 
				|| SecurityEngineUtils.engineIsDiscoverable(databaseId)
				)
			){
			// you dont have access
			throw new IllegalArgumentException("Database does not exist or user does not have access to the database");
		}
		
		IDatabaseEngine engine = Utility.getDatabase(databaseId);
		if(engine == null) {
			throw new IllegalArgumentException("Could not find or load database = " + databaseId);
		}

		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("database_name", engine.getEngineName());
		returnMap.put("database_id", engine.getEngineId());
		Object[] typeAndCost = SecurityEngineUtils.getEngineTypeAndSubTypeAndCost(engine.getSmssProp());
		returnMap.put("database_type", typeAndCost[0].toString());
		returnMap.put("database_subtype", typeAndCost[1]);
		returnMap.put("database_cost", typeAndCost[2]);	

		return new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPEN_DATABASE);
	}

}