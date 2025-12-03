package prerna.reactor.database.upload;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

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
import prerna.util.UploadInputUtility;

public class PredictLLMCSVMetaModelReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PredictLLMCSVMetaModelReactor.class);

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
		List<Object> csvListMap = new ArrayList<Object>();
		Map<String, Object> csvFileMap = new HashMap<String, Object>();

		// get csv file path
		String[] filesPath = UploadInputUtility.getFilesPath(this.store, this.insight);
		int i = 0;
		for (String path : filesPath) {
			Map<String, Object> map = new HashMap<String, Object>();
			Map<String, Object> userPromptInputSchema = new HashMap<String, Object>();
			Map<String, Object> fileMetaMap = new HashMap<String, Object>();
			File file = new File(path);
			if (!file.exists()) {
				throw new IllegalArgumentException("Unable to locate the file");
			}

			// get delimiter
			String delimiter = UploadInputUtility.getDelimiter(this.store);
			char delim = delimiter.charAt(0);

			// set csv file helper
			CSVFileHelper helper = new CSVFileHelper();
			helper.setDelimiter(delim);
			helper.parse(path);
			String fileName = FilenameUtils.removeExtension(file.getName());

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

			// get data from csv to predict types
			List<String[]> data = new ArrayList<>(500);
			String[] cells = null;
			int count = 1;
			// predict meta model from limit row count
			int limit = 500;
			// get end row count
			boolean getEndRowCount = UploadInputUtility.getRowCount(this.store);
			while ((cells = helper.getNextRow()) != null) {
				if (count <= limit) {
					data.add(cells);
					count++;
				} else {
					// if we need to get total number of rows from csv continue
					if (getEndRowCount) {
						count++;
					} else {
						break;
					}
				}

			}
			int endRow = count;

			fileMetaMap.put("startCount", 2);
			if (getEndRowCount) {
				fileMetaMap.put("endCount", endRow);
			}
			// store auto modified header names
			fileMetaMap.put("headerModifications", helper.getChangedHeaders());
			fileMetaMap.put("tableName", fileName);
			fileMetaMap.put("additionalDataTypes", additionalDataTypeMap);

			csvFileMap.put(fileName, fileMetaMap);
			map.put("fileName", fileName);
			map.put("columnHeadersDataTypeMap", dataTypeMap);
			i = i + 1;
			userPromptInputSchema.put("Schema" + i + ":", map);
			csvListMap.add(userPromptInputSchema);
		}

		String json = GSON.toJson(csvListMap);
		List<String> csvFiles = List.of(filesPath);
		// Read CSV file content
		try {
			// User prompt
			StringBuilder userPrompt = new StringBuilder();
			userPrompt.append("Schemas: \r\n").append(json).append("\r\n").append(
					"Task: Apply the system prompt rules to produce the JSON output (tables, relationships, nodeProp). Always follow the system prompt rules exactly.");

			// Structured output schema from LLM
			String outputSchema = PredictLLMConstants.outputSchemaCSV;
			paramMap = new ObjectMapper().readValue(outputSchema, Map.class);// getParamMap();
			if (paramMap == null) {
				paramMap = new HashMap<String, Object>();
			}

			paramMap.put("temperature", 0.3);
			paramMap.put("max_tokens", 5000);

			// System prompt
			String context = PredictLLMConstants.systemPromptLLMForCSV;

			// Initializing and calling LLM Reactor
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

			// Executing LLM Reactor for predicting meta model by using LLM for CSV's
			NounMetadata resultNoun = llmReactor.execute();

			// Response from the LLM Model
			Map<String, Object> response = (Map<String, Object>) resultNoun.getValue();

			String result = (String) response.get("response");
			Map<String, Object> responseMap = new ObjectMapper().readValue(result, Map.class);
			if (responseMap.get("tables") != null) {
				List<Map<String, Object>> tables = (List<Map<String, Object>>) responseMap.get("tables");
				for (Map<String, Object> table : tables) {
					String llmFilename = (String) table.get("fileName");
					Map<String, Object> mapFileObj = (Map<String, Object>) csvFileMap.get(llmFilename);
					if (csvFileMap.containsKey(llmFilename)) {
						table.putAll(mapFileObj);
					}
				}
			}

			if (responseMap.get("relation") != null && responseMap.get("nodeProp") != null
					&& !((List<?>) responseMap.get("relation")).isEmpty()
					&& !((List<?>) responseMap.get("nodeProp")).isEmpty()) {
				List<Map<String, Object>> relationMapList = (List<Map<String, Object>>) responseMap.get("relation");
				Map<String, List<String>> nodeProp = convertToNodeProp(
						(List<Map<String, Object>>) responseMap.get("nodeProp"));
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

		return inputList.stream().collect(Collectors.toMap(entry -> entry.get("tableName").toString().toLowerCase(), // key
				entry -> {
					@SuppressWarnings("unchecked")
					List<String> cols = (List<String>) entry.get("columns");
					return cols; // value
				}));
	}
}
