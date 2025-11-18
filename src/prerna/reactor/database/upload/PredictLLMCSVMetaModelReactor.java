package prerna.reactor.database.upload;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.reactor.AbstractReactor;
import prerna.reactor.masterdatabase.util.GenerateMetamodelLayout;
import prerna.reactor.model.LLMReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.LLMConstants;
import prerna.util.UploadInputUtility;

public class PredictLLMCSVMetaModelReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PredictLLMCSVMetaModelReactor.class);

	private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

	public PredictLLMCSVMetaModelReactor() {

		this.keysToGet = new String[] { UploadInputUtility.FILE_PATH, ReactorKeysEnum.ENGINE.getKey(),
				UploadInputUtility.SPACE, UploadInputUtility.DELIMITER, UploadInputUtility.ROW_COUNT };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		User user = this.insight.getUser();

		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Model " + engineId + " does not exist or user does not have access to this model");
		}
		Map<String, Object> paramMap = new HashMap<String, Object>();
		Map<String, Map<String, Double>> nodePositionMap = new HashMap<String, Map<String, Double>>();
		List<Object> listMap = new ArrayList<Object>();
		// get csv file path
		String[] filesPath = UploadInputUtility.getFilesPath(this.store, this.insight);
		int count = 0;
		for (String path : filesPath) {
			File file = new File(path);
			if (!file.exists()) {
				throw new IllegalArgumentException("Unable to locate file");
			}

			// get delimiter
			String delimiter = UploadInputUtility.getDelimiter(this.store);
			char delim = delimiter.charAt(0);

			// set csv file helper
			CSVFileHelper helper = new CSVFileHelper();
			helper.setDelimiter(delim);
			helper.parse(path);
			String fileName = file.getName();

			String[] columnHeaders = helper.getHeaders();
			Map<String, SemossDataType> dataTypeMap = new LinkedHashMap<String, SemossDataType>();
			Map<String, String> additionalDataTypeMap = new LinkedHashMap<String, String>();

			// predict datatypes and additional types
			Object[][] dataTypes = helper.predictTypes();
			int size = columnHeaders.length;
			for (int colIdx = 0; colIdx < size; colIdx++) {
				Object[] prediction = dataTypes[colIdx];
				dataTypeMap.put(columnHeaders[colIdx], (SemossDataType) prediction[0]);
				if (prediction[1] != null) {
					additionalDataTypeMap.put(columnHeaders[colIdx], (String) prediction[1]);
				}
			}
			Map<String, Object> schema = new HashMap<String, Object>();
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("tableName", fileName);
			map.put("columnHeadersDataTypeMap", dataTypeMap);
			count = count + 1;
			schema.put("Schema" + count + ":", map);
			listMap.add(schema);
		}
		count = 0;
		String json = GSON.toJson(listMap);
		List<String> csvFiles = List.of(filesPath);
		// Read CSV file content
		try {
			// User prompt
			StringBuilder userPrompt = new StringBuilder();
			userPrompt.append("Schemas: \r\n").append(json).append("\r\n").append(
					"Task: Apply the system prompt rules to produce the JSON output (tables, relationships, nodeProp). Always follow the system prompt rules exactly.");

			// Structured output schema from LLM
			String outputSchema = LLMConstants.outputSchemaCSV;
			paramMap = new ObjectMapper().readValue(outputSchema, Map.class);// getParamMap();
			if (paramMap == null) {
				paramMap = new HashMap<String, Object>();
			}

			paramMap.put("temperature", 0.3);
			paramMap.put("max_tokens", 5000);

			// System prompt
			String context = LLMConstants.systemPromptLLMForCSV;

			LLMReactor llmReactor = new LLMReactor();
			NounStore outputNouns = new NounStore("Predict Metamodel LLM ");
			GenRowStruct grs = new GenRowStruct();
			grs.add(new NounMetadata(engineId, PixelDataType.CONST_STRING));
			outputNouns.addNoun(ReactorKeysEnum.ENGINE.getKey(), grs);

			grs = new GenRowStruct();
			grs.add(new NounMetadata(userPrompt.toString(), PixelDataType.CONST_STRING));
			outputNouns.addNoun(ReactorKeysEnum.COMMAND.getKey(), grs);
			llmReactor.setNounStore(outputNouns);

			grs = new GenRowStruct();
			grs.add(new NounMetadata(context, PixelDataType.CONST_STRING));
			outputNouns.addNoun(ReactorKeysEnum.CONTEXT.getKey(), grs);
			llmReactor.setNounStore(outputNouns);

			grs = new GenRowStruct();
			grs.add(new NounMetadata(paramMap, PixelDataType.MAP));
			outputNouns.addNoun(ReactorKeysEnum.PARAM_VALUES_MAP.getKey(), grs);
			llmReactor.setNounStore(outputNouns);

			llmReactor.setInsight(this.insight);

			NounMetadata resultNoun = llmReactor.execute();
			Map<String, Object> response = (Map<String, Object>) resultNoun.getValue();
			
			String result = (String) response.get("response");
			Map<String, Object> responseMap = new ObjectMapper().readValue(result, Map.class);
			if (responseMap.get("relation") != null && responseMap.get("nodeProp") != null
					&& !((List<?>) responseMap.get("relation")).isEmpty()
					&& !((List<?>) responseMap.get("nodeProp")).isEmpty()) {
				List<Map<String, Object>> relationMapList = (List<Map<String, Object>>) responseMap.get("relation");
				 Map<String, List<String>> nodeProp = convertToNodeProp((List<Map<String, Object>>) responseMap.get("nodeProp")); 
				nodePositionMap = GenerateMetamodelLayout.generateMetamodelPredictionLayout(nodeProp, relationMapList);
				responseMap.put(Constants.POSITION_PROP, nodePositionMap);
			} else {
				responseMap.put(Constants.POSITION_PROP, nodePositionMap);
			}

			responseMap.put("fileLocations", csvFiles);
			json = GSON.toJson(responseMap);

			return new NounMetadata(json, PixelDataType.JSON_OBJECT, PixelOperationType.OPERATION);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			classLogger.error(e.getMessage());
		}

		return null;

	}
	
	public static Map<String, List<String>> convertToNodeProp(List<Map<String, Object>> inputList) {

        return inputList.stream()
                .collect(Collectors.toMap(
                        entry -> entry.get("tableName").toString().toLowerCase() + "s",   // key
                        entry -> {
                            @SuppressWarnings("unchecked")
                            List<String> cols = (List<String>) entry.get("columns");
                            return cols;    // value
                        }
                ));
    }
}
