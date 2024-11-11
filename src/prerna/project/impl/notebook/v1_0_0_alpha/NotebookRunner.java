package prerna.project.impl.notebook.v1_0_0_alpha;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import prerna.project.impl.notebook.INotebookRunner;
import prerna.sablecc2.PixelRunner;
import prerna.util.Constants;
import prerna.util.Utility;

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
	public PixelRunner executeNotebook() {
		PixelRunner runner = new PixelRunner();
		
		JsonObject blocksQueryMap = blocksFileJson.getAsJsonObject("queries");
		for(String notebookName : blocksQueryMap.keySet()) {
			// these are from the blocks json
			JsonObject blocksNotebook = blocksQueryMap.getAsJsonObject(notebookName);
			List<JsonElement> blocksCells = blocksNotebook.getAsJsonArray("cells").asList();
			
			for(JsonElement blocksCell : blocksCells) {
				JsonObject blocksParam = blocksCell.getAsJsonObject().getAsJsonObject("parameters");
				
				String blockType = blocksParam.get("type").getAsString();
				String blockValue = blocksParam.get("code").getAsString();
				
				String cell_type = null;
				String id = Utility.getRandomString(8);
				String source = blockValue;
				
				if(blockType.equalsIgnoreCase("py") || blockType.equalsIgnoreCase("r")) {
					cell_type = "code";
				} else if(blockType.equalsIgnoreCase("markdown")) {
					cell_type = "raw";
				} else {
					cell_type = "markdown";
				}
				
				JsonObject cellObject = new JsonObject();
				cellObject.addProperty("cell_type", cell_type);
				cellObject.addProperty("id", id);
				// will add empty metadata for now
				cellObject.add("metadata", new JsonObject());
				JsonArray sourceEle = new JsonArray();
				sourceEle.add(source);
				cellObject.add("source", sourceEle);
				
			}
		}
		
		return runner;
	}

}
