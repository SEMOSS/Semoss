package prerna.reactor.security;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetEngineDependenciesReactor extends AbstractSetMetadataReactor {
	
	public GetEngineDependenciesReactor() {
		this.keysToGet = new String[]{ ReactorKeysEnum.SPACE.getKey() };
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String userId = this.insight.getUserId();
		String engineId = this.keyValue.get(ReactorKeysEnum.SPACE.getKey());
		IEngine engine = null;
		boolean isEngine = true;
		try {
			engine = Utility.getEngine(engineId);
		} catch (Exception ex) {
			isEngine = false;
		}
		if (engine == null) {
			engine = Utility.getProject(engineId);
			isEngine = false;
		}

		String sourceEngineType = engine.getCatalogType().name();
		String sourecEngineId = engine.getEngineId();
		if (isEngine) {
			if (!SecurityEngineUtils.userCanViewEngine(user, sourecEngineId)) {
				throw new IllegalArgumentException(
						"The user does not have access to view this engine or engine id is invalid");
			}
		} else {
			if (!SecurityProjectUtils.userCanViewProject(user, sourecEngineId)) {
				throw new IllegalArgumentException(
						"The user does not have access to view this project or project id is invalid");
			}
		}
		return new NounMetadata(
				SecurityProjectUtils.getEngineDependencyDetails(sourecEngineId, sourceEngineType, userId),
				PixelDataType.MAP);
	}
	
	@Override
	public String getReactorDescription() {
		return "Set the engine dependencies for a engines";
	}
	
}
