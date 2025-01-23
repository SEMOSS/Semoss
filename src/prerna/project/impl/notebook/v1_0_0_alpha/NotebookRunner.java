package prerna.project.impl.notebook.v1_0_0_alpha;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import prerna.om.Insight;
import prerna.project.impl.notebook.INotebookRunner;
import prerna.sablecc2.NotebookExecution;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.gson.GsonUtility;

public class NotebookRunner implements INotebookRunner {

	private static final Logger classLogger = LogManager.getLogger(NotebookWriter.class);

	private JsonObject blocksFileJson = null;
	
	@Override
	public JsonElement getBlocksFileJson() {
		return this.blocksFileJson;
	}
	
	@Override
	public void setBlocksFileJson(JsonElement blocksFileJson) {
		try {
			this.blocksFileJson = blocksFileJson.getAsJsonObject();
		} catch(IllegalStateException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("The json is not of the valid format for this version.", e);
		}
	}
	
	@Override
	public NotebookExecution executeNotebook(Insight insight, Map<String, String> inputReplacements) {
		Gson gson = GsonUtility.getDefaultGson();
		
		PixelRunner runner = new PixelRunner();
		
		Set<String> outputVariables = new HashSet<>();
		Map<String, Object> outputVariableMap = new HashMap<>();
		
		// grab all the values for replacement
		Map<String, String> idToVariable = new HashMap<>();
		Map<String, String> replacements = new HashMap<>();
		JsonObject variables = blocksFileJson.getAsJsonObject("variables");
		for(String varName : variables.keySet()) {
			JsonObject varMap = variables.get(varName).getAsJsonObject();
			if(varMap.has("value")) {
				replacements.put(varName, varMap.get("value").getAsString());
			} else {
				String cellType = varMap.get("type").getAsString();
				if(cellType.equalsIgnoreCase("cell")) {
					String cellId = varMap.get("cellId").getAsString();
					idToVariable.put(cellId, varName);
				} else {
					String pointer = varMap.get("to").getAsString();
					idToVariable.put(pointer, varName);
				}
			}
			
			boolean isOutput = false;
			if(varMap.has("isOutput")) {
				isOutput = varMap.get("isOutput").getAsBoolean();
			}
			if(isOutput) {
				outputVariables.add(varName);
			}
		}
		// add user defined replacements
		if(inputReplacements != null) {
			replacements.putAll(inputReplacements);
		}
		
		// determine the order of execution
		// we will use the executionOrder
		// otherwise, lets hope the order is correct from the notebook 
		// keyset list
		Collection<String> notebookNames = null;
		JsonObject blocksQueryMap = blocksFileJson.getAsJsonObject("queries");
		JsonArray executionOrder = blocksFileJson.getAsJsonArray("executionOrder");
		if(executionOrder == null || executionOrder.isEmpty()) {
			notebookNames = blocksQueryMap.keySet();
		} else {
			notebookNames = new ArrayList<>();
			for(JsonElement ele : executionOrder) {
				notebookNames.add(ele.getAsString());
			}
		}
		
		for(String notebookName : notebookNames) {
			// these are from the blocks json
			JsonObject blocksNotebook = blocksQueryMap.getAsJsonObject(notebookName);
			String notebookId = blocksNotebook.get("id").getAsString();
			
			// store the final output for the notebook
			Object lastResultValue = null;
			String lastResultReplacement = null;
			
			// loop through all the cells in the notebook
			List<JsonElement> blocksCells = blocksNotebook.getAsJsonArray("cells").asList();
			for(JsonElement blocksCellEle : blocksCells) {
				JsonObject blocksCellObj = blocksCellEle.getAsJsonObject();
				String cellId = blocksCellObj.get("id").getAsString();
				
				JsonObject blocksParam = blocksCellObj.getAsJsonObject("parameters");
				
				String blockType = blocksParam.get("type").getAsString();
				String blockValue = blocksParam.get("code").getAsString();
				
				String pixel = null;
				if(blockType.equalsIgnoreCase("py")) {
					pixel = "Py(\"<encode>"+blockValue+"</encode>\");";
				} else if(blockType.equals("r")) {
					pixel = "R(\"<encode>"+blockValue+"</encode>\");";
				} else {
					// you are pixel 
					pixel = blockValue;
				}
				
				String finalPixel = performReplacements(pixel, replacements);
				insight.runPixel(runner, finalPixel);
				
				List<NounMetadata> allResults = runner.getResults();
				NounMetadata lastResult = allResults.get(allResults.size()-1);
				lastResultValue = lastResult.getValue();

				// we want to keep this logic to match the FE replacement logic
				List<PixelOperationType> opTypes = lastResult.getOpType();
				if(opTypes.contains(PixelOperationType.CODE_EXECUTION)
						|| opTypes.contains(PixelOperationType.VECTOR)) {
					lastResultValue = ((List) lastResult.getValue()).get(0);
					if(lastResultValue instanceof NounMetadata) {
						lastResultValue = ((NounMetadata) lastResultValue).getValue();
					}
				}
				
				lastResultReplacement = gson.toJson(lastResultValue);
				
				// store cellId to value
				if(idToVariable.containsKey(cellId)) {
					String pointer = idToVariable.get(cellId);
					replacements.put(pointer, lastResultReplacement);
					replacements.put(pointer+".value", lastResultReplacement);
					if(outputVariables.contains(pointer)) {
						outputVariableMap.put(pointer, lastResultValue);
					}
				}
			}
			// store notebookId to last cell value
			if(idToVariable.containsKey(notebookId)) {
				String pointer = idToVariable.get(notebookId);
				replacements.put(pointer, lastResultReplacement);
				replacements.put(pointer+".value", lastResultReplacement);
				if(outputVariables.contains(pointer)) {
					outputVariableMap.put(pointer, lastResultValue);
				}
			}
		}
		
		NotebookExecution execution = new NotebookExecution();
		execution.setRunner(runner);
		execution.setVariableOutput(outputVariableMap);
		return execution;
	}
	
	private String performReplacements(String pixel, Map<String, String> replacements) {
		for(String replaceKey : replacements.keySet()) {
			pixel = pixel.replace("{{"+replaceKey+"}}", replacements.get(replaceKey));
		}
		return pixel;
	}

	@Override
	public Map<String, String> getBlocksEngineDependencies() {
		JsonElement blocks = this.getBlocksFileJson();
		Map<String, JsonElement> depsMap = blocks.getAsJsonObject().get("dependencies").getAsJsonObject().asMap();
		Map<String, String> engineMap = getEngineIdsFromDepsField(depsMap);
		return engineMap;
	}
	
	private Map<String, String> getEngineIdsFromDepsField(Map<String, JsonElement> depsMap) {
		Map<String, String> engineMap = new HashMap<>();
		if (depsMap != null) {
			for (Map.Entry<String, JsonElement> entry : depsMap.entrySet()) {
				String key = entry.getKey();
				JsonElement value = entry.getValue();
				if (value.isJsonPrimitive() && value.getAsJsonPrimitive().isString()) {
					String stringValue = value.getAsString();
					if (isValidEngine(key, stringValue)) {
						engineMap.put(key, stringValue);
					}
				}
			}
		}
		return engineMap;
	}
	
	/**
	 * Current assumptions are that dependency types delimited by type--id, valid engine types are model, database and vector only, and value is a uuid string
	 * @param key
	 * @param string
	 * @return
	 */
	private boolean isValidEngine(String key, String value) {
		String[] keyParts = key.split("--");
		if (keyParts.length < 2) {
			return false;
		}
		String descriptor = keyParts[0];
		Set<String> validTypes = new HashSet<>(Arrays.asList("model", "database", "vector"));
		if (!validTypes.contains(descriptor)) {
			return false;
		}
		try {
			UUID.fromString(value);
			return true;
		} catch (IllegalArgumentException e) {
			return false;
		}
	}

}
