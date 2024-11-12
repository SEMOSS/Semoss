package prerna.project.impl.notebook;

import com.google.gson.JsonElement;

import prerna.om.Insight;
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
	 * @param insight
	 * @return
	 */
	PixelRunner executeNotebook(Insight insight);

}
