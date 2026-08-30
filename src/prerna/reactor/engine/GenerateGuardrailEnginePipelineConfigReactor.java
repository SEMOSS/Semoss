package prerna.reactor.engine;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.GsonBuilder;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IGuardrailReactorFunctionEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class GenerateGuardrailEnginePipelineConfigReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GenerateGuardrailEnginePipelineConfigReactor.class);

	private static final String PIPELINE_FILE_NAME = "pipeline.json";
	private static final String DEFAULT_REACTOR_CLASS = "prerna.reactor.interceptor.GenericGuardrailInputOutputReactor";

	// structural keys that are NOT direct parameters for the guardrail engine
	private static final Set<String> STRUCTURAL_KEYS = new HashSet<>(
			Arrays.asList("methodName", "input", "output"));

	public GenerateGuardrailEnginePipelineConfigReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.MAP.getKey() };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		if (user == null) {
			throw new SemossPixelException("User must be signed into an account in order to use this reactor");
		}
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (engineId == null || (engineId = engineId.trim()).isEmpty()) {
			throw new SemossPixelException("Must input an engine id");
		}

		engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);

		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new SemossPixelException(
					"Engine '" + engineId + "' does not exist or the user does not have edit access.");
		}

		IEngine.CATALOG_TYPE engineType = SecurityEngineUtils.getEngineType(engineId);

		List<Map<String, Object>> pipelineMapObj = getInputFieldMap();
		if (pipelineMapObj == null || pipelineMapObj.isEmpty()) {
			throw new SemossPixelException("Pipeline map input is missing.");
		}

		Map<String, Object> finalPipelineSchema = buildPipelineSchemaFromList(pipelineMapObj);

		IEngine engine = Utility.getEngine(engineId, engineType, true);
		if (engine == null) {
			throw new SemossPixelException(
					"Could not load engine '" + engineId + "' of type '" + engineType + "'.");
		}

		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engineType, engineId, engine.getEngineName());
		File assetDir = new File(assetsFolder);
		if (!assetDir.exists()) {
			assetDir.mkdirs();
		}

		File pipelineFile = new File(assetDir, PIPELINE_FILE_NAME);
		writePipelineFile(pipelineFile, finalPipelineSchema);

		classLogger.info("Pipeline written successfully to: {}", pipelineFile.getAbsolutePath());

		return new NounMetadata(finalPipelineSchema, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getInputFieldMap() {
		List<Map<String, Object>> result = new ArrayList<>();
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.MAP.getKey());
		if (grs != null && !grs.isEmpty()) {
			List<NounMetadata> mapNouns = grs.getNounsOfType(PixelDataType.MAP);
			if (mapNouns != null && !mapNouns.isEmpty()) {
				for (NounMetadata noun : mapNouns) {
					Object value = noun.getValue();
					if (value instanceof Map) {
						result.add((Map<String, Object>) value);
					}
				}
			}
		}
		return result;
	}

	private Map<String, Object> buildPipelineSchemaFromList(List<Map<String, Object>> pipelineList) {
		Map<String, Object> finalSchema = new LinkedHashMap<>();
		Map<String, Object> pipelines = new LinkedHashMap<>();

		for (Map<String, Object> pipelineMap : pipelineList) {
			Object methodNameObj = pipelineMap.get("methodName");
			if (methodNameObj == null) {
				throw new SemossPixelException("Pipeline map is missing the 'methodName' key.");
			}
			String methodName = String.valueOf(methodNameObj);

			Map<String, Object> pipelineConfig = new LinkedHashMap<>();
			pipelineConfig.put("input", buildGuardrailList((List<?>) pipelineMap.get("input")));
			pipelineConfig.put("output", buildGuardrailList((List<?>) pipelineMap.get("output")));
			pipelines.put(methodName, pipelineConfig);
		}

		finalSchema.put("pipelines", pipelines);
		return finalSchema;
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> buildGuardrailList(List<?> guardrailList) {
		List<Map<String, Object>> interceptorList = new ArrayList<>();
		if (guardrailList == null || guardrailList.isEmpty()) {
			return interceptorList;
		}

		for (Object guardrailObj : guardrailList) {
			if (guardrailObj == null) {
				continue;
			}

			String guardrailEngineId;
				guardrailEngineId = String.valueOf(guardrailObj);
				
			IGuardrailReactorFunctionEngine engine = Utility.getGuardrailEngine(guardrailEngineId);
			 if (engine == null) {
				 throw new SemossPixelException("Could not load guardrail engine '" + guardrailEngineId + "'.");
			 }

			Map<String, Object> interceptor = new LinkedHashMap<>();
			interceptor.put("reactorClass", DEFAULT_REACTOR_CLASS);
			interceptor.put("guardrailEngineName", engine.getEngineName());
			Map<String, Object> params = new LinkedHashMap<>();
			params.put("blockOnGuardrailFailure", true);
			params.put("guardrailEngineId", guardrailEngineId);
			params.put("directParameters", engine.getKeysAndValuesToGet());

			Map<String, Object> inputMapping = new LinkedHashMap<>();
			inputMapping.put("prompt", "arg0");
			params.put("inputMapping", inputMapping);

			interceptor.put("params", params);
			interceptorList.add(interceptor);
		}

		return interceptorList;
	}

	private void writePipelineFile(File file, Map<?, ?> pipelineMap) {
		String json = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create().toJson(pipelineMap);
		try (FileWriter fw = new FileWriter(file)) {
			fw.write(json);
		} catch (IOException e) {
			classLogger.error("Failed to write pipeline.json to {}", file.getAbsolutePath(), e);
			throw new SemossPixelException("Failed to write pipeline.json: " + e.getMessage(), e);
		}
	}

	@Override
	public String getReactorDescription() {
		return "Writes a validated pipeline.json file to the assets folder of the specified engine.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The engine id";
		} else if (key.equals(ReactorKeysEnum.MAP.getKey())) {
			return "The pipeline map containing methodName, input, and output guardrail configurations";
		}
		return super.getDescriptionForKey(key);
	}
}