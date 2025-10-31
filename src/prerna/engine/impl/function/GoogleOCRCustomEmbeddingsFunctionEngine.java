package prerna.engine.impl.function;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.ICustomEmbeddingsFunctionEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.reactor.export.pdf.PDFUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class GoogleOCRCustomEmbeddingsFunctionEngine extends GoogleOCRFunctionEngine
		implements ICustomEmbeddingsFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(GoogleOCRCustomEmbeddingsFunctionEngine.class);

	@Override
	public void open(Properties smssProp) throws Exception {
		// preset these - don't need user to define
		smssProp.putIfAbsent(IFunctionEngine.NAME_KEY,
				"Google OCR Custom Embeddings - For Use With Vector Database Engines");
		smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY, "Execute Google OCR");
		super.open(smssProp);
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		throw new IllegalArgumentException(
				"This function engine is only intended to be executed for custom vector db embeddings");
	}

	@Override
	public boolean canProcessDocument(File fileToProcess) {
		boolean pdf = fileToProcess.getName().toLowerCase().endsWith(".pdf");
		if (pdf) {
			try {
				return PDFUtility.pdfContainsImages(fileToProcess.getAbsolutePath());
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return false;
	}

	@Override
	public int processDocument(String outputCsvFilePath, File fileToProcess, Map<String, Object> parameters) {
		List<String> extractedTextFromDoc = new ArrayList<String>();
		String fileDir = null;
		Boolean saveFileToStorage = false;
		final String WAITING_INFO = "Waiting for operation to complete...";
		String fileName = fileToProcess.getName();
		try (VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(outputCsvFilePath)) {
			IStorageEngine storageEng = Utility.getStorage(this.googleStorageEngineId);
			boolean pdf = fileName.toLowerCase().endsWith(".pdf");
			saveFileToStorage = Boolean
					.parseBoolean(parameters.get(Constants.CUSTOM_DOCUMENT_PROCESSOR_USE_STORAGE).toString());
			if (pdf) {
				Insight insight = (Insight) parameters.get(Constants.INSIGHT);
				String insightId = insight.getInsightId();
				Insight in = InsightStore.getInstance().get(insightId);
				File instanceDir = new File(Utility.normalizePath(in.getInsightFolder()));

				fileDir = instanceDir + DIR_SEPARATOR + fileName;
				File pdfFilePath = new File(fileDir);
				if (saveFileToStorage) {
					if (!SecurityEngineUtils.userCanEditEngine(insight.getUser(), this.googleStorageEngineId)) {
						throw new IllegalArgumentException("Storage " + this.googleStorageEngineId
								+ " does not exist or user does not have access to this engine");
					}
					Map<String, Object> metadata = new HashMap<>();
					metadata.put("utility", fileName + "- GoogleOCR_functionality");

					storageEng.copyToStorage(fileDir, this.bucketName + DIR_SEPARATOR + this.objectPath + fileName,
							metadata);
					classLogger.info(WAITING_INFO);
					extractedTextFromDoc = getAsyncTextExtraction(pdfFilePath);

					storageEng.deleteFromStorage(DIR_SEPARATOR + this.objectPath + fileName);
				} else {
					if (hasMoreThanPageLimits(pdfFilePath, this.pageLength)) {
						throw new IllegalArgumentException(
								"Unable to process the file because the total number of pages exceeds 5. "
										+ "The file is expected to be saved in storage before processing. "
										+ fileToProcess);
					} else {
						extractedTextFromDoc = getSyncTextExtraction(pdfFilePath);
					}
				}
			} else {
				throw new IllegalArgumentException(
						"Please provide valid input files using \"FILE_PATH\". File types supported is pdf");
			}
			for (int i = 0; i < extractedTextFromDoc.size(); i++) {
				writer.writeRow(fileName, String.valueOf(i + 1), extractedTextFromDoc.get(i));
			}

			return writer.getRowsInCsv();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e);
		}
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.GOOGLE_OCR_CUSTOM_EMBEDDINGS.name();
	}
}