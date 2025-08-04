package prerna.reactor.engine;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ReplaceAndCleanupEngineReactor extends AbstractEngineFileReactor {
	private static final Logger classLogger = LogManager.getLogger(ReplaceAndCleanupEngineReactor.class);

	public ReplaceAndCleanupEngineReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.MAP.getKey(), "newEngineId" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String currentEngineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		String newEngineId = this.keyValue.get("newEngineId");
		validateUserAndEngineAccess(user);

		if (!SecurityEngineUtils.userCanViewEngine(user, currentEngineId)) {
			throw new IllegalArgumentException("Engine not accessible or does not exist: " + currentEngineId);
		}

		try {

			String currentEnginePath = getSpecificEngineBaseFolder(currentEngineId);
			String newEnginePath = getSpecificEngineBaseFolder(newEngineId);
			Utility.replaceAllEditedAssetsFromTempEngine(currentEnginePath, newEnginePath);
			Utility.deleteTempEngine(newEnginePath);
			return new NounMetadata("Engine changes have been successfully applied and temporary engine deleted.",
					PixelDataType.CONST_STRING);

		} catch (Exception e) {
			classLogger.error("Execution failed during engine finalization", e);
			throw new RuntimeException("Failed to finalize and clean up temp engine: " + e.getMessage(), e);
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
