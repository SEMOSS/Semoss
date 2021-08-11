package prerna.sablecc2.reactor.cluster;

import java.util.HashMap;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityDatabaseUtils;
import prerna.auth.utils.SecurityUpdateUtils;
import prerna.engine.api.IEngine;
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
			returnMap.put("database_type", IEngine.ENGINE_TYPE.APP.toString());
			returnMap.put("database_cost", "");	
			return new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPEN_DATABASE);
		}
		
		if(AbstractSecurityUtils.securityEnabled()) {
			// make sure valid id for user
			if(!SecurityDatabaseUtils.userCanViewDatabase(this.insight.getUser(), databaseId)) {
				// you dont have access
				throw new IllegalArgumentException("Database does not exist or user does not have access to database");
			}
		}
		
		IEngine engine = Utility.getEngine(databaseId);
		if(engine == null) {
			throw new IllegalArgumentException("Could not find or load database = " + databaseId);
		}

		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("database_name", engine.getEngineName());
		returnMap.put("database_id", engine.getEngineId());
		String[] typeAndCost = SecurityUpdateUtils.getDatabaseTypeAndCost(engine.getProp());
		returnMap.put("database_type", typeAndCost[0]);	
		returnMap.put("database_cost", typeAndCost[1]);	

		return new NounMetadata(returnMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPEN_DATABASE);
	}

}