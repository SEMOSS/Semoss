package prerna.engine.api;

import java.io.File;
import java.util.Map;

public interface ICustomEmbeddingsFunctionEngine extends IFunctionEngine {

	String OUTPUT_CSV_FILEPATH = "outputCsvFilePath";
	String FILE_TO_PROCESS = "fileToProcess";
	String PARAMETERS = "parameters";
	
	/**
	 * 
	 * @param fileToProcess
	 * @return
	 */
	boolean canProcessDocument(File fileToProcess);
	
	/**
	 * 
	 * @param outputCsvFilePath
	 * @param fileToProcess
	 * @param parameters
	 * @return	The number of rows added to the outputCsvFilePath
	 */
	int processDocument(String outputCsvFilePath, File fileToProcess, Map<String, Object> parameters);

}