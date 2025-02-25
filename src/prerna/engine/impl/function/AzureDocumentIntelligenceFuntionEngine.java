package prerna.engine.impl.function;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.azure.ai.formrecognizer.documentanalysis.DocumentAnalysisClient;
import com.azure.ai.formrecognizer.documentanalysis.DocumentAnalysisClientBuilder;
import com.azure.ai.formrecognizer.documentanalysis.models.AnalyzeResult;
import com.azure.ai.formrecognizer.documentanalysis.models.DocumentLine;
import com.azure.ai.formrecognizer.documentanalysis.models.DocumentPage;
import com.azure.ai.formrecognizer.documentanalysis.models.OperationResult;
import com.azure.core.credential.AzureKeyCredential;
import com.azure.core.util.BinaryData;
import com.azure.core.util.polling.SyncPoller;

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.ICustomEmbeddingsFunctionEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.reactor.export.pdf.PDFUtility;
import prerna.util.Constants;

public class AzureDocumentIntelligenceFuntionEngine extends AbstractFunctionEngine implements ICustomEmbeddingsFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AzureDocumentIntelligenceFuntionEngine.class);

	private static final String URL = "URL";
	private static final String PREBUILT_READ = "prebuilt-read";

	private String connectionUrl;
	private String apiKey;
	private DocumentAnalysisClient documentAnalysisClient = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		// preset these - don't need user to define
		smssProp.putIfAbsent(IFunctionEngine.NAME_KEY, "Azure Document Intelligence");
		smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY, "Execute Azure Document Intelligence");

		super.open(smssProp);

		this.connectionUrl = smssProp.getProperty(URL);
		this.apiKey = smssProp.getProperty(Constants.API_KEY);
		if (this.connectionUrl == null || (this.connectionUrl=this.connectionUrl.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass in the connection url");
		}
		if (this.apiKey == null || (this.apiKey=this.apiKey.trim()).isEmpty()) {
			throw new IllegalArgumentException("Must pass in the api key");
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
		throw new IllegalArgumentException("This function engine is only intended to be executed for custom vector db embeddings");
	}

	@Override
	public boolean canProcessDocument(File fileToProcess) {
		boolean pdf = fileToProcess.getName().toLowerCase().endsWith(".pdf");
		if(pdf) {
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
		VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(outputCsvFilePath);
		try {
			String source = fileToProcess.getName();
			classLogger.info("Starting to process : " + source);
			SyncPoller<OperationResult, AnalyzeResult> analyzeResultPoller = this.documentAnalysisClient
					.beginAnalyzeDocument(PREBUILT_READ,
							BinaryData.fromFile(fileToProcess.toPath(), 8092));
			AnalyzeResult analyzeResult = analyzeResultPoller.getFinalResult();
			List<DocumentPage> pages = analyzeResult.getPages();
			int numPages = pages.size();
			for(int i = 0; i < numPages; i++) {
				DocumentPage documentPage = pages.get(i);
				String pageNum = documentPage.getPageNumber()+"";
				classLogger.info("Processing page " + pageNum + " of " + numPages + " for " + source);

				// aggregate and write the row
				StringBuffer extractedTextForeachLine = new StringBuffer();
				if(documentPage.getLines() != null) {
					for (DocumentLine documentLine : documentPage.getLines()) {
						extractedTextForeachLine.append(documentLine.getContent()).append(" ");
					}
					writer.writeRow(source, pageNum, extractedTextForeachLine.toString());
				}
			}
		} finally {
			writer.close();
		}
		
		return writer.getRowsInCsv();
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.AZURE_DOCUMENT_INTELLIGENCE.name();
	}

	@Override
	public void close() throws IOException {
		// nothing to do
	}
}
