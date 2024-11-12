package prerna.project.impl.notebook.v1_0_0_alpha;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import prerna.om.Insight;
import prerna.project.impl.notebook.INotebookRunner;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

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
	public PixelRunner executeNotebook(Insight insight) {
		PixelRunner runner = new PixelRunner();
		
		// grab all the values for replacement
		Map<String, String> replacements = new HashMap<>();
		JsonObject variables = blocksFileJson.getAsJsonObject("variables");
		for(String varName : variables.keySet()) {
			JsonObject varMap = variables.get(varName).getAsJsonObject();
			if(varMap.has("value")) {
				replacements.put(varName, varMap.get("value").getAsString());
			}
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
			
			NounMetadata lastResult = null;
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
				lastResult = allResults.get(allResults.size()-1);
				
				//TODO: need a way to grab the response based on the op type
				//for example, py/r return an array 
				
				// store cellId to value
				replacements.put(cellId, lastResult.getValue()+"");
			}
			// store notebookId to last cell value
			replacements.put(notebookId, lastResult.getValue()+"");
		}
		
		return runner;
	}
	
	private String performReplacements(String pixel, Map<String, String> replacements) {
		for(String replaceKey : replacements.keySet()) {
			pixel = pixel.replace("{{"+replaceKey+"}}", replacements.get(replaceKey));
		}
		return pixel;
	}

}
