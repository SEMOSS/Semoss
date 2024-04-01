package prerna.project.impl.notebook;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class NotebookFactory {

	/**
	 * 
	 * @param projectBlocksF
	 * @return
	 * @throws IOException
	 */
	public static INotebookBuilder getNotebookBuilder(File projectBlocksF) throws IOException {
		JsonObject blocksFileJson = null;
		try (Reader fileReader = new FileReader(projectBlocksF)) {
			blocksFileJson = JsonParser.parseReader(fileReader).getAsJsonObject();
		}
		
		JsonElement versionBlock = blocksFileJson.get("version");
		String version = null;
		if(versionBlock != null) {
			version = versionBlock.getAsString();
		}
		
		INotebookBuilder builder = null;
		if(version == null) {
			builder = new prerna.project.impl.notebook.v1_0_0_alpha.NotebookHelper();
		} else {
			// only really have one, but this is to build out in the future
			builder = new prerna.project.impl.notebook.v1_0_0_alpha.NotebookHelper();
		}
		
		builder.setBlocksFileJson(blocksFileJson);
		
		return builder;
	}
	
}
