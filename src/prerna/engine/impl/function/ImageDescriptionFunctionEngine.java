package prerna.engine.impl.function;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;

import prerna.engine.api.IModelEngine;
import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.reactor.frame.gaas.processors.ImageDocProcessor;
import prerna.reactor.frame.gaas.processors.ImagePDFProcessor;
import prerna.reactor.frame.gaas.processors.ImagePPTProcessor;
import prerna.reactor.frame.gaas.processors.TextFileProcessor;
import prerna.util.Constants;
import prerna.util.Utility;

public class ImageDescriptionFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(ImageDescriptionFunctionEngine.class);
	
	private String imageEngineId;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		// this is the multi modal engine
		this.imageEngineId = this.smssProp.getProperty(Constants.IMAGE_ENGINE_ID);
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {

		String csvFilePath = (String) parameterValues.get("csvPath");
		File file = (File) parameterValues.get("document");
		Map<String, Object> vectorParmaters = (Map<String, Object>) parameterValues.get("parameters");
		Insight insight = getInsight(vectorParmaters.get(AbstractVectorDatabaseEngine.INSIGHT));

		Map<String, Object> result = null;
		try {
			result = convertFilesToCSV(csvFilePath, file);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		int rowsCreated = (int) result.get("rowsInCSV");

		// if we didnt get any rows, return back to abstract
		if (rowsCreated <= 1) {
			return rowsCreated;
		}

		// else continue with the image description generation.
		Map<String, String> imageMap = new HashMap<>();
		imageMap = (Map<String, String>) result.get("imageMap");

		try {
			replaceImageKeysInCsv(csvFilePath, imageMap, imageEngineId, insight);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return rowsCreated;
	}

	/**
	 * 
	 * @param csvFileName
	 * @param file
	 * @return Map with two keys - rowsInCSV and imageMap
	 * @throws IOException
	 */
	public Map<String, Object> convertFilesToCSV(String csvFileName, File file) throws IOException {
		VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(csvFileName);
		Map<String, Object> result = new HashMap<>();
		Map<String, String> imageMap = new HashMap<>();

		try {
			classLogger.info("Starting file conversions ");
			List<String> processedList = new ArrayList<String>();

			// pick up the files and convert them to CSV
			classLogger.info("Processing file : " + file.getName());

			// process this file
			String filetype = FilenameUtils.getExtension(file.getAbsolutePath());
			String mimeType = null;

			// using tika for mime type check since it is more consistent across env + rhel
			// OS and macOS
			TikaConfig config = TikaConfig.getDefaultConfig();
			Detector detector = config.getDetector();
			Metadata metadata = new Metadata();
			metadata.add(TikaCoreProperties.RESOURCE_NAME_KEY, file.getName());
			try (TikaInputStream stream = TikaInputStream.get(new FileInputStream(file))) {
				mimeType = detector.detect(stream, metadata).toString();
			} catch (IOException e) {
				classLogger.error(Constants.ERROR_MESSAGE, e);
			}

			if (mimeType != null) {
				classLogger.info("Processing file : " + file.getName() + " mime type: " + mimeType);
				if (mimeType.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
						|| (mimeType.equalsIgnoreCase("application/x-tika-ooxml")
								&& (filetype.equals("doc") || filetype.equals("docx")))) {
					ImageDocProcessor idp = new ImageDocProcessor(file.getAbsolutePath(), writer, true);
					idp.process();
					imageMap = idp.getImageMap();

					processedList.add(file.getAbsolutePath());

				} else if (mimeType
						.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.presentationml.presentation")
						|| (mimeType.equalsIgnoreCase("application/x-tika-ooxml")
								&& (filetype.equals("ppt") || filetype.equals("pptx")))) {
					// powerpoint

					ImagePPTProcessor ipp = new ImagePPTProcessor(file.getAbsolutePath(), writer, true);
					ipp.process();
					imageMap = ipp.getImageMap();

					processedList.add(file.getAbsolutePath());
				} else if (mimeType.equalsIgnoreCase("application/pdf")) {

					// add an if statement whether want to do images or not
					ImagePDFProcessor pdf = new ImagePDFProcessor(file.getAbsolutePath(), writer);
					pdf.process();
					imageMap = pdf.getImageMap();
					processedList.add(file.getAbsolutePath());

				} else if (mimeType.equalsIgnoreCase("text/plain")) {
					TextFileProcessor text = new TextFileProcessor(file.getAbsolutePath(), writer);
					text.process();
					processedList.add(file.getAbsolutePath());
				} else {
					classLogger.warn("No support exists for parsing mime-type = " + mimeType);
					classLogger.warn("No support exists for parsing mime-type = " + mimeType);
					classLogger.warn("No support exists for parsing mime-type = " + mimeType);
					classLogger.warn("No support exists for parsing mime-type = " + mimeType);
					classLogger.warn("No support exists for parsing mime-type = " + mimeType);
					classLogger.warn("No support exists for parsing mime-type = " + mimeType);
					classLogger.warn("No support exists for parsing mime-type = " + mimeType);
				}
				classLogger.info("Completed Processing file : " + file.getAbsolutePath());

			}
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
											put("text",
													"Describe the image in detail, especially if it is a complicated workflow, process diagram, or detailed image with lots of text. Ensure all major text and components are captured comprehensively. For simpler images without much detail or text, provide a concise 1-2 sentence description.");
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

		Files.write(Paths.get(csvFilePath), updatedLines);
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
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

}
