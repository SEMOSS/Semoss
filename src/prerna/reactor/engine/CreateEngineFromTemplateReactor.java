package prerna.reactor.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CreateEngineFromTemplateReactor extends AbstractEngineFileReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateEngineFromTemplateReactor.class);

	public CreateEngineFromTemplateReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.MAP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		validateUserAndEngineAccess(user);

		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException("Engine not accessible or does not exist: " + engineId);
		}

		try {

			// Generate new engine ID
			String newEngineId = UUID.randomUUID().toString();
			String currentEnginePath = getSpecificEngineBaseFolder(engineId);
			 //copying the current engine function to new one
		    Utility.copyAndLoadEngine(engineId, newEngineId, currentEnginePath);
			return new NounMetadata(newEngineId,PixelDataType.CONST_STRING);
		} catch (Exception e) {
			classLogger.error("Execution failed for temporary Python Function Engine", e);
			throw new RuntimeException("Failed to run temporary Python engine: " + e.getMessage(), e);
		}
	}

	private Map<String, Object> getMap() {
		Map<String, Object> parameterValues = new HashMap<>();

		GenRowStruct mapGrs = this.store.getNoun(this.keysToGet[1]);
		if (mapGrs != null && !mapGrs.isEmpty()) {
			for (int i = 0; i < mapGrs.size(); i++) {
				NounMetadata noun = mapGrs.getNoun(i);
				parameterValues.putAll((Map<String, Object>) noun.getValue());
			}
		} else {
			List<Object> mapValues = curRow.getValuesOfType(PixelDataType.MAP);
			if (mapValues != null && !mapValues.isEmpty()) {
				for (int i = 0; i < mapValues.size(); i++) {
					parameterValues.putAll((Map<String, Object>) mapValues.get(i));
				}
			}
		}

		return parameterValues;
	}

}
