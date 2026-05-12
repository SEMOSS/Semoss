package prerna.reactor.engine;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.GsonBuilder;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.engine.api.IEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

public class GenerateGuardrailEnginePipelineConfigReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GenerateGuardrailEnginePipelineConfigsReactor.class);

	private static final String PIPELINE_FILE_NAME = "pipeline.json";

	private static final String DEFAULT_REACTOR_CLASS = "prerna.reactor.interceptor.GenericGuardrailInputOutputReactor";

	public GenerateGuardrailEnginePipelineConfigReactor() {

		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.MAP.getKey() };

		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {

		organizeKeys();

		User user = this.insight.getUser();

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());

		if (engineId == null || engineId.isEmpty()) {
			throw new IllegalArgumentException("Must input an engine id");
		}

		engineId = SecurityQueryUtils.testUserEngineIdForAlias(user, engineId);

		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {

			throw new IllegalArgumentException(
					"Engine '" + engineId + "' does not exist or the user does not have edit access.");
		}

		IEngine.CATALOG_TYPE engineType = SecurityEngineUtils.getEngineType(engineId);

		List<Map<String, Object>> pipelineMapObj = getInputFieldMap();
		
		if (pipelineMapObj == null) {
			throw new IllegalArgumentException("Pipeline map input is missing.");
		}

		Map<String, Object> finalPipelineSchema;

		if (pipelineMapObj instanceof List) {
			finalPipelineSchema = buildPipelineSchemaFromList((List<Map<String, Object>>) pipelineMapObj);
		}  else {
			throw new IllegalArgumentException("Invalid pipeline map input type.");
		}

		IEngine engine = Utility.getEngine(engineId, engineType, true);

		if (engine == null) {

			throw new IllegalArgumentException(
					"Could not load engine '" + engineId + "' of type '" + engineType + "'.");
		}

		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(engineType, engineId,
				engine.getEngineName());

		File assetDir = new File(assetsFolder);

		if (!assetDir.exists()) {
			assetDir.mkdirs();
		}

		File pipelineFile = new File(assetDir, PIPELINE_FILE_NAME);

		writePipelineFile(pipelineFile, finalPipelineSchema);

		String successMessage = "Pipeline written successfully to: " + pipelineFile.getAbsolutePath();

		classLogger.info(successMessage);

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
			String methodName = String.valueOf(pipelineMap.get("methodName"));
			Map<String, Object> pipelineConfig = new LinkedHashMap<>();

			pipelineConfig.put("input", buildGuardrailList((List<?>) pipelineMap.get("input")));
			pipelineConfig.put("output", buildGuardrailList((List<?>) pipelineMap.get("output")));

			pipelines.put(methodName, pipelineConfig);
		}

		finalSchema.put("pipelines", pipelines);
		return finalSchema;
	}

	private Map<String, Object> buildPipelineSchema(Map<String, Object> pipelineMap) {

		Map<String, Object> finalSchema = new LinkedHashMap<>();
		Map<String, Object> pipelines = new LinkedHashMap<>();

		// Extract methodName from the pipelineMap
		Object methodNameObj = pipelineMap.get("methodName");
		if (methodNameObj == null) {
			throw new IllegalArgumentException("Pipeline map is missing the 'methodName' key.");
		}

		String methodName = String.valueOf(methodNameObj);
		Map<String, Object> pipelineConfig = new LinkedHashMap<>();

		// Build input and output configurations
		pipelineConfig.put("input", buildGuardrailList((List<?>) pipelineMap.get("input")));
		pipelineConfig.put("output", buildGuardrailList((List<?>) pipelineMap.get("output")));

		// Add the pipeline configuration to the pipelines map
		pipelines.put(methodName, pipelineConfig);

		// Add pipelines to the final schema
		finalSchema.put("pipelines", pipelines);
		return finalSchema;
	}

	private List<Map<String, Object>> buildGuardrailList(List<?> guardrailIds) {

		List<Map<String, Object>> interceptorList = new ArrayList<>();

		if (guardrailIds == null || guardrailIds.isEmpty()) {
			return interceptorList;
		}

		for (Object guardrailIdObj : guardrailIds) {

			if (guardrailIdObj == null) {
				continue;
			}

			String guardrailEngineId = String.valueOf(guardrailIdObj);

			Map<String, Object> interceptor = new LinkedHashMap<>();

			interceptor.put("reactorClass", DEFAULT_REACTOR_CLASS);

			Map<String, Object> params = new LinkedHashMap<>();

			params.put("blockOnGuardrailFailure", true);

			params.put("guardrailEngineId", guardrailEngineId);

			
            // Read directParameters from .smss file
			
			Map<String, Object> directParameters = getDirectParametersFromSmss(guardrailEngineId);

			params.put("directParameters", directParameters);

			Map<String, Object> inputMapping = new LinkedHashMap<>();

			inputMapping.put("prompt", "arg0");

			params.put("inputMapping", inputMapping);

			interceptor.put("params", params);

			interceptorList.add(interceptor);
		}

		return interceptorList;
	}

	private Map<String, Object> getDirectParametersFromSmss(String guardrailEngineId) {

		Map<String, Object> directParameters = new LinkedHashMap<>();

		try {

			IEngine.CATALOG_TYPE catalogType = SecurityEngineUtils.getEngineType(guardrailEngineId);

			IEngine guardrailEngine = Utility.getEngine(guardrailEngineId, catalogType, true);

			if (guardrailEngine == null) {
				return directParameters;
			}

			Properties smssProperties = guardrailEngine.getSmssProp();

			if (smssProperties == null) {
				return directParameters;
			}

			/*
			 * threshold
			 */
			String threshold = smssProperties.getProperty("DEFAULT_THRESHOLD");

			if (threshold != null && !threshold.trim().isEmpty()) {

				try {

					directParameters.put("threshold", Double.parseDouble(threshold));

				} catch (Exception e) {

					classLogger.warn("Invalid threshold value for guardrail engine: " + guardrailEngineId);
				}
			}

			/*
			 * labels
			 */
			String labels = smssProperties.getProperty("NER_LABELS");

			if (labels != null && !labels.trim().isEmpty()) {

				List<String> labelList = Arrays.stream(labels.split(",")).map(String::trim).filter(s -> !s.isEmpty())
						.collect(Collectors.toList());

				directParameters.put("labels", labelList);
			}

		} catch (Exception e) {

			classLogger.error("Failed to read directParameters from .smss for engine: " + guardrailEngineId, e);
		}

		return directParameters;
	}

	private void writePipelineFile(File file, Map<?, ?> pipelineMap) {

		String json = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create().toJson(pipelineMap);

		try (FileWriter fw = new FileWriter(file)) {

			fw.write(json);

		} catch (IOException e) {

			classLogger.error("Failed to write pipeline.json to " + file.getAbsolutePath(), e);

			throw new IllegalStateException("Failed to write pipeline.json: " + e.getMessage(), e);
		}
	}

	@Override
	public String getReactorDescription() {

		return "Writes a validated pipeline.json file " + "to the assets folder of the specified engine.";
	}
}