package prerna.auth.utils.reactors.admin;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminEngineInfoReactor extends AbstractReactor {
	
	public AdminEngineInfoReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.META_KEYS.getKey()};
	}

	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		
		if(engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Must input an engine id");
		}
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), engineId);

		List<Map<String, Object>> baseInfo = adminUtils.getAllEngineSettings(Arrays.asList(engineId), null, null, null, null, null);
		if(baseInfo == null || baseInfo.isEmpty()) {
			throw new IllegalArgumentException("Could not find any engine data");
		}
		
		// we filtered to a single database
		Map<String, Object> databaseInfo = baseInfo.get(0);
		databaseInfo.putAll(SecurityEngineUtils.getAggregateEngineMetadata(engineId, getMetaKeys(), true));
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