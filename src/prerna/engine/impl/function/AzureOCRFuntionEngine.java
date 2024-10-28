package prerna.engine.impl.function;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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

import com.azure.ai.formrecognizer.documentanalysis.DocumentAnalysisClient;
import com.azure.ai.formrecognizer.documentanalysis.DocumentAnalysisClientBuilder;
import com.azure.ai.formrecognizer.documentanalysis.models.AnalyzeResult;
import com.azure.ai.formrecognizer.documentanalysis.models.DocumentLine;
import com.azure.ai.formrecognizer.documentanalysis.models.OperationResult;
import com.azure.core.credential.AzureKeyCredential;
import com.azure.core.util.BinaryData;
import com.azure.core.util.polling.SyncPoller;

import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.reactor.export.pdf.PDFUtility;
import prerna.reactor.frame.gaas.processors.ImagePDFProcessor;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;

public class AzureOCRFuntionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AzureOCRFuntionEngine.class);

	private String engineId = null;
	private String engineName = null;
	private String connectionUrl;
	private String apiKey;
	DocumentAnalysisClient documentAnalysisClient = null;
	List<String> output = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.engineId = this.smssProp.getProperty(Constants.ENGINE);
		this.engineName = this.smssProp.getProperty(Constants.ENGINE_ALIAS);
		this.connectionUrl = smssProp.getProperty("URL");
		this.apiKey = smssProp.getProperty("API_KEY");
		if (this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			throw new RuntimeException("Must pass in an access key");
		}
		if (this.apiKey == null || this.apiKey.isEmpty()) {
			classLogger.error("Must pass in a secret key");
			throw new RuntimeException("Must pass in a secret key");
		}

		try {
			this.documentAnalysisClient = new DocumentAnalysisClientBuilder()
					.credential(new AzureKeyCredential(this.apiKey)).endpoint(this.connectionUrl).buildClient();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {

		classLogger.info("Starting ocr function engine");
		List<String> extractedTextFromDoc = new ArrayList<String>();
		StringBuffer extractedTextForeachLine = new StringBuffer();
		boolean flag = false;
		Map<String, Object> result = null;
		int rowsCreated = 0;
		File document = null;
		Insight insight = new Insight();
		Map<String, Object> vectorParmaters = (Map<String, Object>) parameterValues.get("parameters");
		String csvFilePath = (String) parameterValues.get("csvPath");
		boolean isCallFromVectorDB = false;
		if (vectorParmaters == null) {
			String filePath = (String) parameterValues.get("document");
			document = new File(filePath);
		} else {
			document = (File) parameterValues.get("document");
			insight = getInsight(vectorParmaters.get(AbstractVectorDatabaseEngine.INSIGHT));
			isCallFromVectorDB = true;
		}

		try {
			flag = PDFUtility.validatePDImages(document.getAbsolutePath());
		} catch (IOException e) {
			classLogger.error(Constants.ERROR_MESSAGE, e);
			throw new SemossPixelException("Unable to read document");
		}

		if (flag) {
			SyncPoller<OperationResult, AnalyzeResult> analyzeResultPoller = this.documentAnalysisClient
					.beginAnalyzeDocument("prebuilt-read",
							BinaryData.fromFile(document.toPath(), (int) document.length()));
			AnalyzeResult analyzeResult = analyzeResultPoller.getFinalResult();
			analyzeResult.getPages().forEach(documentPage -> {
				// line
				for (DocumentLine documentLine : documentPage.getLines()) {
					extractedTextForeachLine.append(documentLine.getContent());
				}
				extractedTextFromDoc.add(extractedTextForeachLine.toString());
				extractedTextForeachLine.setLength(0);
			});

			output = extractedTextFromDoc;
			// Calls for vector DB
			if (isCallFromVectorDB) {
				try {
					result = convertFilesToCSV(csvFilePath, document);
				} catch (IOException e) {
					classLogger.error(Constants.ERROR_MESSAGE, e);
					throw new SemossPixelException(e.getMessage());
				}
				rowsCreated = (int) result.get("rowsInCSV");
				classLogger.info("Ending ocr function engine for vector db");
				return rowsCreated;
			}
			// Calling from Apps.Except than vector DB calls
			else {
				
				classLogger.info("Ending ocr function engine for app");
				return output;
			}
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
					// TODO : Image Doc processor

				} else if (mimeType
						.equalsIgnoreCase("application/vnd.openxmlformats-officedocument.presentationml.presentation")
						|| (mimeType.equalsIgnoreCase("application/x-tika-ooxml")
								&& (filetype.equals("ppt") || filetype.equals("pptx")))) {
					// powerpoint
					// TODO : Image PPT processor

				} else if (mimeType.equalsIgnoreCase("application/pdf")) {

					ImagePDFProcessor pdf = new ImagePDFProcessor(file.getAbsolutePath(), writer);
					pdf.readTextfromPdf(csvFileName, file, this.output);
					processedList.add(file.getAbsolutePath());

				} else if (mimeType.equalsIgnoreCase("text/plain")) {

					// TODO : Image text processor

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

	@Override
	public void close() throws IOException {
		classLogger.info("Closing the connection");

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
