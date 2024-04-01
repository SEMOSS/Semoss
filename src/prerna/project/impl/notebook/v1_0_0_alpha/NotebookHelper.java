package prerna.project.impl.notebook.v1_0_0_alpha;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonWriter;

import prerna.project.impl.notebook.INotebookBuilder;
import prerna.util.Constants;
import prerna.util.Utility;


public class NotebookHelper implements INotebookBuilder {

	private static final Logger classLogger = LogManager.getLogger(NotebookHelper.class);

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
	public List<File> createNotebooks(File writeDir) {
		List<File> notebookList = new ArrayList<>();
		Gson gson = new GsonBuilder().setPrettyPrinting().disableHtmlEscaping().create();

		try {
			FileUtils.cleanDirectory(writeDir); 

			JsonObject blocksQueryMap = blocksFileJson.getAsJsonObject("queries");
			for(String notebookName : blocksQueryMap.keySet()) {
				// these are from the blocks json
				JsonObject blocksNotebook = blocksQueryMap.getAsJsonObject(notebookName);
				List<JsonElement> blocksCells = blocksNotebook.getAsJsonArray("cells").asList();
				
				// we now need to move the information from the blocks json
				// into the notebook we are writing
				File writeNotebook = new File(Utility.normalizePath(writeDir.getAbsolutePath() + "/" + notebookName + ".ipynb"));
	
				JsonArray cellsArray = new JsonArray();
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
					
					// now add this to the cells array
					cellsArray.add(cellObject);
				}
				
				JsonObject writeJson = new JsonObject();
				writeJson.add("cells", cellsArray);
				
				// write to the notebook file
				try(JsonWriter writer = gson.newJsonWriter(new FileWriter(writeNotebook))){
					gson.toJson(writeJson, writer);
				}
				// add to list of notebooks
				notebookList.add(writeNotebook);
			}
		} catch(IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred trying to create the notebook for this app");
		}

		return notebookList;
	}
	
}
