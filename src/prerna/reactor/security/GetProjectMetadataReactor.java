package prerna.reactor.security;

import java.util.Date;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.masterdatabase.utility.MasterDatabaseUtility;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetProjectMetadataReactor extends AbstractReactor {
	
	public GetProjectMetadataReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.META_KEYS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		
		if(projectId == null || projectId.isEmpty()) {
			throw new IllegalArgumentException("Must input an project id");
		}
		
		User user = this.insight.getUser();
		
		List<Map<String, Object>> baseInfo = null;
		// make sure valid id for user
		if(SecurityProjectUtils.userCanViewProject(user, projectId)) {
			// user has access!
			baseInfo = SecurityProjectUtils.getUserProjectList(user, projectId);
		} else if(SecurityProjectUtils.projectIsDiscoverable(projectId)) {
			baseInfo = SecurityProjectUtils.getDiscoverableProjectList(projectId, null);
		} else {
			// you dont have access
			throw new IllegalArgumentException("Engine does not exist or user does not have access to the database");
		}
		
		if(baseInfo == null || baseInfo.isEmpty()) {
			throw new IllegalArgumentException("Could not find any engine metadata");
		}
		
		// we filtered to a single database
		Map<String, Object> databaseInfo = baseInfo.get(0);
		databaseInfo.putAll(SecurityProjectUtils.getAggregateProjectMetadata(projectId, getMetaKeys(), false));
		// append last engine update
		{
			Date eDate = MasterDatabaseUtility.getEngineDate(projectId);
			if(eDate != null) {
				databaseInfo.put("last_updated", MasterDatabaseUtility.getEngineDate(projectId));
			}
		}
		
		// see if there is any pending request to this engine
		int pendingRequest = SecurityProjectUtils.getUserPendingAccessRequest(user, projectId);
		if(pendingRequest > 0) {
			databaseInfo.put("pending_access_request", pendingRequest);
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
