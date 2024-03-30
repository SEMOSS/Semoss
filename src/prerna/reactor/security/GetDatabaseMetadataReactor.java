package prerna.reactor.security;

import java.util.List;
import java.util.Map;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

@Deprecated
public class GetDatabaseMetadataReactor extends AbstractReactor {
	
	public GetDatabaseMetadataReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.META_KEYS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		
		if(databaseId == null || databaseId.isEmpty()) {
			throw new IllegalArgumentException("Must input an database id");
		}
		
		List<Map<String, Object>> baseInfo = null;
		// make sure valid id for user
		databaseId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), databaseId);
		if(SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), databaseId)) {
			// user has access!
			baseInfo = SecurityEngineUtils.getUserEngineList(this.insight.getUser(), databaseId, null);
		} else if(SecurityEngineUtils.engineIsDiscoverable(databaseId)) {
			baseInfo = SecurityEngineUtils.getDiscoverableEngineList(databaseId, null);
		} else {
			// you dont have access
			throw new IllegalArgumentException("Database does not exist or user does not have access to the database");
		}
		
		if(baseInfo == null || baseInfo.isEmpty()) {
			throw new IllegalArgumentException("Could not find any database data");
		}
		
		// we filtered to a single database
		Map<String, Object> databaseInfo = baseInfo.get(0);
		databaseInfo.putAll(SecurityEngineUtils.getAggregateEngineMetadata(databaseId, getMetaKeys(), false));
		// append last engine update
		databaseInfo.put("last_updated", MasterDatabaseUtility.getEngineDate(databaseId));
		return new NounMetadata(databaseInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.DATABASE_INFO);
	}
	
	private List<String> getMetaKeys() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.META_KEYS.getKey());
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		
		return null;
	}

}
