package prerna.engine.impl.function;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.ICustomEmbeddingsFunctionEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.reactor.frame.gaas.processors.AbstractFileImageProcessor;
import prerna.reactor.frame.gaas.processors.IFileImageProcessor;
import prerna.util.Constants;
import prerna.util.Utility;

public class ImageDescriptionFunctionEngine extends AbstractFunctionEngine implements ICustomEmbeddingsFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(ImageDescriptionFunctionEngine.class);

	private static final String CUSTOM_PROMPT = "CUSTOM_PROMPT";
	
	private String imageEngineId;
	private String imageEnginePrompt = "Describe the image in detail, especially if it is a complicated workflow, process diagram, or detailed image with lots of text. "
			+ "Ensure all major text and components are captured comprehensively. "
			+ "For simpler images without much detail or text, provide a concise 1-2 sentence description.";

	@Override
	public void open(Properties smssProp) throws Exception {
		// preset these - don't need user to define
		smssProp.putIfAbsent(IFunctionEngine.NAME_KEY, "Image Description Function - For Use With Vector Database Engines");
		smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY, "Extract images from the documents and run them through an LLM to summarize the images in addition to the text extraction");
		
		super.open(smssProp);
		// this is the multi modal engine
		this.imageEngineId = this.smssProp.getProperty(Constants.IMAGE_ENGINE_ID);
		
		String prompt = this.smssProp.getProperty(CUSTOM_PROMPT);
		if(prompt != null && !(prompt=prompt.trim()).isEmpty()) {
			this.imageEnginePrompt = prompt;
		}
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		throw new IllegalArgumentException("This function engine is only intended to be executed for custom vector db embeddings");
	}

	@Override
	public boolean canProcessDocument(File fileToProcess) {
		return AbstractFileImageProcessor.getFileProcessor(fileToProcess, null) != null;
	}

	@Override
	public int processDocument(String outputCsvFilePath, File fileToProcess, Map<String, Object> parameters) {
		Insight insight = getInsight(parameters.get(AbstractVectorDatabaseEngine.INSIGHT));

		Map<String, Object> result = null;
		try {
			result = convertFilesToCSV(outputCsvFilePath, fileToProcess);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		int rowsCreated = (int) result.get("rowsInCSV");

		// if we didn't get any rows, return back to abstract
		if (rowsCreated <= 1) {
			return rowsCreated;
		}

		// else continue with the image description generation.
		Map<String, String> imageMap = new HashMap<>();
		imageMap = (Map<String, String>) result.get("imageMap");

		try {
			replaceImageKeysInCsv(outputCsvFilePath, imageMap, imageEngineId, insight);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return rowsCreated;
	}
	
	/**
	 * 
	 * @param outputCsvFilePath
	 * @param fileToProcess
	 * @return Map with two keys - rowsInCSV and imageMap
	 * @throws IOException
	 */
	public Map<String, Object> convertFilesToCSV(String outputCsvFilePath, File fileToProcess) throws IOException {
		VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(outputCsvFilePath);
		Map<String, Object> result = new HashMap<>();
		Map<String, String> imageMap = new HashMap<>();
		try {
			classLogger.info("Processing file : " + fileToProcess.getName());
			IFileImageProcessor processor = AbstractFileImageProcessor.getFileProcessor(fileToProcess, writer);
			if(processor != null) {
				processor.process();
				imageMap = processor.getImageMap();
				classLogger.info("Completed Processing file : " + fileToProcess.getAbsolutePath());
			} else {
				classLogger.info("No file processor for file : " + fileToProcess.getAbsolutePath());
			}
		} catch(NullPointerException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			writer.close();
		}
		
		result.put("rowsInCSV", writer.getRowsInCsv());
		result.put("imageMap", imageMap);
		return result;
	}

	/**
	 * 
	 * @param csvFilePath
	 * @param imageMap
	 * @param imageEngineId
	 * @param insight
	 * @throws IOException
	 */
	private void replaceImageKeysInCsv(String csvFilePath, Map<String, String> imageMap, String imageEngineId, Insight insight) throws IOException {
		List<String> lines = Files.readAllLines(Paths.get(csvFilePath));

		IModelEngine llmEngine = Utility.getModel(imageEngineId);

		Map<String, String> outputMap = new HashMap<>();
		int counter = 1;
		int numImages = imageMap.size();

		for (Map.Entry<String, String> entry : imageMap.entrySet()) {
			classLogger.info("processing image " + counter + " out of " + numImages);

			List<Map<String, Object>> fullPrompt = new ArrayList<Map<String, Object>>() {
				{
					add(new HashMap<String, Object>() {
						{
							put("role", "system");
							put("content", "You are a helpful assistant.");
						}
					});
					add(new HashMap<String, Object>() {
						{
							put("role", "user");
							put("content", new ArrayList<Map<String, Object>>() {
								{
									add(new HashMap<String, Object>() {
										{
											put("type", "text");
											put("text", imageEnginePrompt);
										}
									});
									add(new HashMap<String, Object>() {
										{
											put("type", "image_url");
											put("image_url", new HashMap<String, Object>() {
												{
													put("url", "data:image/png;base64," + entry.getValue());
												}
											});
										}
									});
								}
							});
						}
					});
				}
			};
			Map<String, Object> paramMap = new HashMap<String, Object>();
			paramMap.put("full_prompt", fullPrompt);

			Map<String, Object> llmOutput = llmEngine.ask(null, null, insight, paramMap).toMap();

			String llmOutputStr = (String) llmOutput.get("response");
			llmOutputStr = llmOutputStr.replace("\"", "");
			String imageDescWithAnnot = " -- BEGINNING OF IMAGE DESCRIPTION : " + llmOutputStr + " : END OF IMAGE DESCRIPTION -- ";
			outputMap.put(entry.getKey(), imageDescWithAnnot);
			counter++;
		}

		List<String> updatedLines = new ArrayList<>();
		for (String line : lines) {
			String[] cells = line.split(","); // split the line into cells
			for (int i = 0; i < cells.length; i++) {
				for (Map.Entry<String, String> entry : outputMap.entrySet()) {
					cells[i] = cells[i].replace(entry.getKey(), entry.getValue());
				}
			}
			updatedLines.add(String.join(",", cells)); // join cells back into a line
		}

		Files.write(Paths.get(csvFilePath), updatedLines, StandardCharsets.UTF_8);
	}

	/**
	 * 
	 * @param insightObj
	 * @return
	 */
	protected Insight getInsight(Object insightObj) {
		if (insightObj instanceof String) {
			return InsightStore.getInstance().get((String) insightObj);
		} else {
			return (Insight) insightObj;
		}
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return "IMAGE_PROCESSING";
	}

	@Override
	public void close() throws IOException {
		// nothing to do
	}

}
