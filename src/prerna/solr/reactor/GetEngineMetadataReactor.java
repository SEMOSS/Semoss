package prerna.solr.reactor;

import java.util.Date;
import java.util.List;
import java.util.Map;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetEngineMetadataReactor extends AbstractReactor {
	
	public GetEngineMetadataReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.META_KEYS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		
		if(engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Must input an engine id");
		}
		
		List<Map<String, Object>> baseInfo = null;
		if(AbstractSecurityUtils.securityEnabled()) {
			// make sure valid id for user
			engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);
			if(SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
				// user has access!
				baseInfo = SecurityEngineUtils.getUserEngineList(this.insight.getUser(), engineId, null);
			} else if(SecurityEngineUtils.engineIsDiscoverable(engineId)) {
				baseInfo = SecurityEngineUtils.getDiscoverableEngineList(engineId, null);
			} else {
				// you dont have access
				throw new IllegalArgumentException("Engine does not exist or user does not have access to the database");
			}
		}
		
		if(baseInfo == null || baseInfo.isEmpty()) {
			throw new IllegalArgumentException("Could not find any engine metadata");
		}
		
		// we filtered to a single database
		Map<String, Object> databaseInfo = baseInfo.get(0);
		databaseInfo.putAll(SecurityEngineUtils.getAggregateEngineMetadata(engineId, getMetaKeys(), false));
		// append last engine update
		{
			Date eDate = MasterDatabaseUtility.getEngineDate(engineId);
			if(eDate != null) {
				databaseInfo.put("last_updated", MasterDatabaseUtility.getEngineDate(engineId));
			}
		}
		return new NounMetadata(databaseInfo, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.ENGINE_INFO);
	}
	
	private List<String> getMetaKeys() {
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.META_KEYS.getKey());
		if(grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}
		
		return null;
	}

}
