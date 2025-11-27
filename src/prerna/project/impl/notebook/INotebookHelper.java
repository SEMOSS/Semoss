package prerna.project.impl.notebook;

import java.util.Map;

import com.google.gson.JsonElement;

import prerna.engine.api.IModelEngine;
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
	 * 
	 * @return Map of the variable name to the engine id
	 */
	Map<String, String> getBlocksEngineDependencies();

	/**
	 * 
	 * @return
	 */
	Map<String, String> getNotebookVariables();

	/**
	 * 
	 * @param filePath
	 * @param model
	 * @param insight
	 * @return Map of the function name to the original notebook cell id
	 */
	Map<String, String> transformNotebookToMcpDriver(String filePath, IModelEngine model, Insight insight);

	/**
	 * 
	 * @param filePath
	 * @param model
	 * @param insight
	 * @param cellId
	 * @return Map of the function name to the original notebook cell id
	 */
	Map<String, String> transformNotebookCellToMcpDriver(String filePath, IModelEngine model, Insight insight,
			String cellId);

}
