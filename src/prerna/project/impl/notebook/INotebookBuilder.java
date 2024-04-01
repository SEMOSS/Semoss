package prerna.project.impl.notebook;

import java.io.File;
import java.util.List;

import com.google.gson.JsonElement;

public interface INotebookBuilder {

	/**
	 * 
	 * @param writeDir
	 * @return
	 */
	List<File> createNotebooks(File writeDir);
	
	/**
	 * 
	 * @param blocksFileJson
	 */
	void setBlocksFileJson(JsonElement blocksFileJson);
	
	/**
	 * 
	 * @return
	 */
	JsonElement getBlocksFileJson();
	
}
