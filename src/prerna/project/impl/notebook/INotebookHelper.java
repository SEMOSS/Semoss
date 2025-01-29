package prerna.project.impl.notebook;

import java.util.Map;

import com.google.gson.JsonElement;

import prerna.om.Insight;
import prerna.sablecc2.NotebookExecution;

public interface INotebookHelper {

	String UNDEFINED_VALUE = "undefined";
	
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
	 * @param inputReplacements
	 * @return
	 */
	NotebookExecution executeNotebook(Insight insight, Map<String, String> inputReplacements);
	
	/**
	 * Gets only engine deps listed in the blocks.json file in the project
	 * @return	Map of the variable name to the engine id
	 */
	Map<String, String> getBlocksEngineDependencies();

}
