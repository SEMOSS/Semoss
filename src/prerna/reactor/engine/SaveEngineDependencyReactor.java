package prerna.reactor.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class SaveEngineDependencyReactor extends AbstractReactor {

	public SaveEngineDependencyReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SPACE.getKey(), "depEngineIds" };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String SourceEngineId = this.keyValue.get(ReactorKeysEnum.SPACE.getKey());

		if (!SecurityEngineUtils.userCanEditEngine(user, SourceEngineId)) {
			throw new IllegalArgumentException(
					"Engine " + SourceEngineId + " does not exist or user does not have access to the engine.");
		}
		List<String> depEngineIds = getListString("depEngineIds");

		IEngine sourceEngine = Utility.getEngine(SourceEngineId);
		String sourceEngineType = sourceEngine.getCatalogType().name();

		List<Map<String, Object>> depEngines = new ArrayList<>();
		for (String depEngineId : depEngineIds) {
			Map<String, Object> depEngine = new HashMap<>();
			depEngine.put("ENGINEID", depEngineId);
			IEngine engine = null;
			try {
				engine = Utility.getEngine(depEngineId);
				depEngine.put("ENGINETYPE", engine.getCatalogType().name());
			} catch (Exception ex) {
				// ignore
			}
			if (engine == null) {
				engine = Utility.getProject(depEngineId);
				depEngine.put("ENGINETYPE", IEngine.CATALOG_TYPE.PROJECT.name());
			}
			depEngines.add(depEngine);
		}

		SecurityProjectUtils.updateEngineDependencies(user, SourceEngineId, sourceEngineType, depEngines);
		return new NounMetadata(true, PixelDataType.MAP);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.JSON.getKey())) {
			return "Saves dependent engines  for the specified engine.";
		}
		return super.getDescriptionForKey(key);
	}

}
