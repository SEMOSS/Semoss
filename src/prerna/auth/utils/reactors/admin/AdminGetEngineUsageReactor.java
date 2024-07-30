package prerna.auth.utils.reactors.admin;

import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AdminGetEngineUsageReactor extends AbstractReactor {

	
	public AdminGetEngineUsageReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
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
		Map<String, Object> retMap = ModelInferenceLogsUtils.getEngineUsageFromModelInferenceLogs(engineId);
		return new NounMetadata(retMap, PixelDataType.MAP);
	}

}
