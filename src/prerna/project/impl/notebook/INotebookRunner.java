package prerna.project.impl.notebook;

import com.google.gson.JsonElement;

import prerna.sablecc2.PixelRunner;

public interface INotebookRunner {

	/**
	 * 
	 * @return
	 */
	JsonElement getBlocksFileJson();

	/**
	 * 
	 * @param blocksFileJson
	 */
	void setBlocksFileJson(JsonElement blocksFileJson);

	/**
	 * 
	 * @return
	 */
	PixelRunner executeNotebook();

}
