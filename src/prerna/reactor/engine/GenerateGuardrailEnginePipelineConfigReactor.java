package prerna.reactor.engine;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.GsonBuilder;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class GenerateGuardrailEnginePipelineConfigReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GenerateGuardrailEnginePipelineConfigReactor.class);

	private static final String PIPELINE_FILE_NAME = "pipeline.json";

	public GenerateGuardrailEnginePipelineConfigReactor() {
		this.keysToGet = new String[] {
				ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.MAP.getKey()
		};
		this.keyRequired = new int[] { 1, 1};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);

		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Engine '" + engineId + "' does not exist or the user does not have edit access.");
		}

		IEngine.CATALOG_TYPE catalogType = SecurityEngineUtils.getEngineType(engineId);

		Map<?, ?> pipelineMap = getMap(ReactorKeysEnum.MAP.getKey());
		if (pipelineMap == null || pipelineMap.isEmpty()) {
			throw new IllegalArgumentException("Pipeline map input is missing or empty.");
		}

		IEngine engine = Utility.getEngine(engineId, catalogType, true);
		if (engine == null) {
			throw new IllegalArgumentException(
					"Could not load engine '" + engineId + "' of type '" + catalogType + "'.");
		}

		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(
				catalogType, engineId, engine.getEngineName());

		File assetDir = new File(assetsFolder);
		if (!assetDir.exists()) {
			assetDir.mkdirs();
		}

		File pipelineFile = new File(assetDir, PIPELINE_FILE_NAME);
		writePipelineFile(pipelineFile, pipelineMap);

		String successMessage = "Pipeline written successfully to: " + pipelineFile.getAbsolutePath();
		classLogger.info(successMessage);

		return new NounMetadata(pipelineMap, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	private void writePipelineFile(File file, Map<?, ?> pipelineMap) {
		String json = new GsonBuilder()
				.disableHtmlEscaping()
				.setPrettyPrinting()
				.create()
				.toJson(pipelineMap);

		try (FileWriter fw = new FileWriter(file)) {
			fw.write(json);
		} catch (IOException e) {
			classLogger.error("Failed to write pipeline.json to " + file.getAbsolutePath(), e);
			throw new IllegalStateException(
					"Failed to write pipeline.json: " + e.getMessage(), e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Writes a pipeline.json file to the assets folder of the specified engine.";
	}
}
