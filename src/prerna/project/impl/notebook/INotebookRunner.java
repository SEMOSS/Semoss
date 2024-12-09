package prerna.project.impl.notebook;

import java.util.Map;

import com.google.gson.JsonElement;

import prerna.om.Insight;
import prerna.sablecc2.NotebookExecution;

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
	 * @param inputReplacements
	 * @return
	 */
	NotebookExecution executeNotebook(Insight insight, Map<String, String> inputReplacements);

}
