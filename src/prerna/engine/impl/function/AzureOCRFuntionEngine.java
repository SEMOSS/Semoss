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
import prerna.engine.impl.vector.VectorDatabaseUtils;
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

	public static final String URL = "URL";
	public static final String API_KEY = "API_KEY";
	public static final String PARAMETERS = "parameters";
	public static final String CSVPATH = "csvPath";
	public static final String DOC = "document";
	public static final String PDFENDS = ".pdf";
	public static final String CSVROWS = "rowsInCSV";
	public static final String IMAGEMAP = "imageMap";
	public static final String PREBUILT_READ = "prebuilt-read";

	// Log messages
	public static final String PDF_EXIST = "Pdf Exist";
	public static final String IMAGE_EXIST = "Image Exist";
	public static final String TEXT_EXIST = "Text Exist";
	public static final String ACCESSKEY_MSG = "Must pass in an access key";
	public static final String SECRETKEY_MSG = "Must pass in an secret key";
	public static final String OCRSTART_MSG = "Starting ocr function engine";
	public static final String OCREND_VECTOR_MSG = "Ending ocr function engine for vector db";
	public static final String OCREND_MSG = "Ending ocr function engine for app";
	public static final String IOEXCEPTION_MSG = "IOException from ocr function engine";

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.engineId = this.smssProp.getProperty(Constants.ENGINE);
		this.engineName = this.smssProp.getProperty(Constants.ENGINE_ALIAS);
		this.connectionUrl = smssProp.getProperty(URL);
		this.apiKey = smssProp.getProperty(API_KEY);
		if (this.connectionUrl == null || this.connectionUrl.isEmpty()) {
			throw new RuntimeException(ACCESSKEY_MSG);
		}
		if (this.apiKey == null || this.apiKey.isEmpty()) {
			throw new RuntimeException(SECRETKEY_MSG);
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

		classLogger.info(OCRSTART_MSG);
		List<String> extractedTextFromDoc = new ArrayList<String>();
		StringBuffer extractedTextForeachLine = new StringBuffer();
		boolean flag = false;
		boolean isPdf = false;
		Map<String, Object> result = null;
		int rowsCreated = 0;
		File document = null;
		Insight insight = new Insight();
		Map<String, Object> vectorParmaters = (Map<String, Object>) parameterValues.get(PARAMETERS);
		String csvFilePath = (String) parameterValues.get(CSVPATH);
		boolean isCallFromVectorDB = false;

		if (vectorParmaters == null) {
			String filePath = (String) parameterValues.get(DOC);
			document = new File(filePath);
		} else {
			document = (File) parameterValues.get(DOC);
			insight = getInsight(vectorParmaters.get(AbstractVectorDatabaseEngine.INSIGHT));
			isCallFromVectorDB = true;
		}

		isPdf = document.getName().toLowerCase().endsWith(PDFENDS);

		try {
			if (isPdf) {
				classLogger.info(PDF_EXIST);
				flag = PDFUtility.validatePDImages(document.getAbsolutePath());
				if (!flag) {
					classLogger.info(TEXT_EXIST);
					rowsCreated = VectorDatabaseUtils.convertFilesToCSV(csvFilePath, document);
					return rowsCreated;
				} else {
					classLogger.info(IMAGE_EXIST);
					SyncPoller<OperationResult, AnalyzeResult> analyzeResultPoller = this.documentAnalysisClient
							.beginAnalyzeDocument(PREBUILT_READ,
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
						rowsCreated = (int) result.get(CSVROWS);
						classLogger.info(OCREND_VECTOR_MSG);
						return rowsCreated;
					}
					// Calling from Function and Apps.Except than vector DB calls
					else {
						classLogger.info(OCREND_MSG);
						return output;
					}
				}
			} else {
				rowsCreated = VectorDatabaseUtils.convertFilesToCSV(csvFilePath, document);
				return rowsCreated;
			}
		}

		catch (IOException e) {
			classLogger.info(IOEXCEPTION_MSG);
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
		result.put(CSVROWS, writer.getRowsInCsv());
		result.put(IMAGEMAP, imageMap);
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