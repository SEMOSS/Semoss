package prerna.engine.impl.function;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.pdmodel.PDDocument;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.textract.AmazonTextract;
import com.amazonaws.services.textract.AmazonTextractClientBuilder;
import com.amazonaws.services.textract.model.AnalyzeDocumentRequest;
import com.amazonaws.services.textract.model.AnalyzeDocumentResult;
import com.amazonaws.services.textract.model.Block;
import com.amazonaws.services.textract.model.Document;
import com.amazonaws.services.textract.model.DocumentLocation;
import com.amazonaws.services.textract.model.GetDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.GetDocumentTextDetectionResult;
import com.amazonaws.services.textract.model.S3Object;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionResult;

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

public class AWSTextractCustomEmbeddingsFunctionEngine extends AbstractFunctionEngine
		implements ICustomEmbeddingsFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSTextractCustomEmbeddingsFunctionEngine.class);

	private static final String DIR_SEPARATOR = "/";

	private static final String ACCESS_KEY = "ACCESS_KEY";
	private static final String SECRET_KEY = "SECRET_KEY";
	private static final String REGION = "REGION";
	private static final String BUCKETENGINEID = "S3BUCKETENGINEID";
	private static final String SUCCEEDED = "SUCCEEDED";
	private static final String PAGE = "PAGE";
	private static final String LINE = "LINE";
	private static final String STORAGE_PATH = "STORAGE_PATH";
	private static final String PAGE_LENGTH = "PAGE_LENGTH";

	private String accessKey;
	private String secretKey;
	private String region;
	private String storageEngineId;
	private String bucketName;
	private String objectPath;
	private int pageLength = 1;
	private String storagePath;

	private AmazonTextract textractClient = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		// preset these - don't need user to define
		smssProp.putIfAbsent(IFunctionEngine.NAME_KEY, "AWS Textract - For Use With Vector Database Engines");
		smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY, "Execute AWS Textract");

		super.open(smssProp);

		this.accessKey = smssProp.getProperty(ACCESS_KEY);
		this.secretKey = smssProp.getProperty(SECRET_KEY);
		this.region = smssProp.getProperty(REGION);
		this.storageEngineId = smssProp.getProperty(BUCKETENGINEID);
		this.storagePath = smssProp.getProperty(STORAGE_PATH);
		this.pageLength = Integer.parseInt(smssProp.getProperty(PAGE_LENGTH));

		if (this.accessKey == null || this.accessKey.isEmpty()) {
			throw new RuntimeException("Must pass in an access key");
		}
		if (this.secretKey == null || this.secretKey.isEmpty()) {
			throw new RuntimeException("Must pass in a secret key");
		}
		if (this.region == null || this.region.isEmpty()) {
			throw new RuntimeException("Must define the requiredParameters");
		}
		if (this.storageEngineId == null || this.storageEngineId.isEmpty()) {
			throw new RuntimeException("Must pass in a Storage Engine Id for an S3 Bucket");
		}
		try {
			BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
			this.textractClient = AmazonTextractClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(this.region).build();		
			
			this.storagePath = this.storagePath.replace("s3://","");
			int startIndex = this.storagePath.indexOf(DIR_SEPARATOR);
			int endIndex = this.storagePath.lastIndexOf(DIR_SEPARATOR);				
			this.bucketName = this.storagePath.substring(0, startIndex);
						
			if (startIndex < endIndex && startIndex < this.storagePath.length()) {
				this.objectPath = this.storagePath.substring(startIndex+1, endIndex);					
			}else if(startIndex==endIndex && startIndex < this.storagePath.length()) {
				this.objectPath = this.storagePath.substring(startIndex+1, this.storagePath.length());
			}
			
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		File filePath = null;
		Boolean saveFileToStorage = false;
		String fileDir = null;
		Object extractedTextFromDoc = null;
		String documentKeyName = null;
		String folderPath = null;

		if (this.requiredParameters != null && !this.requiredParameters.isEmpty()) {
			Set<String> missingPs = new HashSet<>();
			for (String requiredP : this.requiredParameters) {
				if (!parameterValues.containsKey(requiredP)) {
					missingPs.add(requiredP);
				}
			}
			if (!missingPs.isEmpty()) {
				throw new IllegalArgumentException("Must define required keys = " + missingPs);
			}
		}
		try {
			for (String k : parameterValues.keySet()) {
				if (k.equalsIgnoreCase("FILE_PATH")) {
					filePath = new File(parameterValues.get(k).toString());
				} else if (k.equalsIgnoreCase(Constants.CUSTOM_DOCUMENT_PROCESSOR_NEED_STORAGE)) {
					saveFileToStorage = Boolean.parseBoolean(parameterValues.get(k).toString());
				}
			}
			
			documentKeyName = filePath.getName();
			folderPath = this.objectPath + DIR_SEPARATOR + documentKeyName;
			IStorageEngine storageeng = Utility.getStorage(this.storageEngineId);
			boolean pdf = documentKeyName.toLowerCase().endsWith(".pdf");
			
			if (pdf) {
				Insight insight = getInsight(parameterValues.get("INSIGHT"));
				String insightId = insight.getInsightId();
				Insight in = InsightStore.getInstance().get(insightId);
				File instanceDir = new File(Utility.normalizePath(in.getInsightFolder()));

				fileDir = instanceDir + DIR_SEPARATOR + documentKeyName;
				File pdfFilePath = new File(fileDir);
				if (saveFileToStorage) {
					if (!SecurityEngineUtils.userCanEditEngine(insight.getUser(), this.storageEngineId)) {
						throw new IllegalArgumentException("Storage " + this.storageEngineId
								+ " does not exist or user does not have access to this engine");
					}
					Map<String, Object> metadata = new HashMap<>();
					metadata.put("utility", documentKeyName + "- Textract_functionality");
					storageeng.copyToStorage(fileDir,
							this.bucketName + DIR_SEPARATOR + this.objectPath + filePath.getName(), metadata);
					extractedTextFromDoc = getAsyncTextExtraction(folderPath, this.bucketName);
				} else {
					if (hasMoreThanPageLimits(pdfFilePath)) {
						throw new IllegalArgumentException(
								"Unable to process the file because the total number of pages exceeds 5. "
										+ "The file is expected to be saved in storage before processing. " + filePath);
					} else {
						extractedTextFromDoc = getSyncTextExtraction(pdfFilePath);
					}
				}
			} else {
				throw new IllegalArgumentException(
						"Please provide valid input files using \"FILE_PATH\". File types supported is pdf");
			}
			storageeng.deleteFromStorage(fileDir);
		}catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);	
			throw new IllegalArgumentException(e);
		}
		return extractedTextFromDoc;
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
		VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(outputCsvFilePath);
		List<String> extractedTextFromDoc = new ArrayList<String>();
		String documentKeyName = null;
		String folderPath = null;
		Boolean saveFileToStorage = (Boolean) parameters.get(Constants.CUSTOM_DOCUMENT_PROCESSOR_NEED_STORAGE);		

		try {
			documentKeyName = fileToProcess.getName();
			folderPath = this.objectPath + DIR_SEPARATOR + documentKeyName;

			IStorageEngine storageeng = Utility.getStorage(this.storageEngineId);
			Map<String, Object> metadata = new HashMap<>();
			metadata.put("utility", fileToProcess.getName() + "- Textract_functionality");
			
			if (saveFileToStorage) {				
				storageeng.copyToStorage(fileToProcess.toString(),
						this.bucketName + DIR_SEPARATOR + this.objectPath + fileToProcess.getName(), metadata);

				extractedTextFromDoc = getAsyncTextExtraction(folderPath, this.bucketName);				
			}else {
				if (hasMoreThanPageLimits(fileToProcess)) { 
					throw new IllegalArgumentException(
							"Unable to process the file because the total number of pages exceeds 5. "
									+ "The file is expected to be saved in storage before processing. "
									+ fileToProcess);
				} else {
					extractedTextFromDoc = getSyncTextExtraction(fileToProcess);
				}

			}
			storageeng.deleteFromStorage(fileToProcess.toString());
			
			
			for (int i = 0; i < extractedTextFromDoc.size(); i++) {
				// source, divider, content
				writer.writeRow(documentKeyName, String.valueOf(i + 1), extractedTextFromDoc.get(i));
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);	
			throw new IllegalArgumentException(e);
		} finally {
			writer.close();
		}

		return writer.getRowsInCsv();
	}
	
	private List<String> getSyncTextExtraction(File fileToProcess) throws Exception {
		List<String> extractedTextFromDoc = new ArrayList<String>();
		try {			
			FileInputStream inputStream = new FileInputStream(fileToProcess);
			byte[] fileBytes = new byte[(int) fileToProcess.length()];
			inputStream.read(fileBytes);
			inputStream.close();

			ByteBuffer imageBytes = ByteBuffer.wrap(fileBytes);
			// Prepare document with bytes
			Document document = new Document().withBytes(imageBytes);
			// Request (analyze text, tables, and forms)
			AnalyzeDocumentRequest request = new AnalyzeDocumentRequest().withDocument(document)
					.withFeatureTypes(Arrays.asList("TABLES", "FORMS"));
			
			AnalyzeDocumentResult result = this.textractClient.analyzeDocument(request);

			// Extract text page-wise
			List<String> pageTexts = new ArrayList<>();
			StringBuilder currentPage = new StringBuilder();

			for (Block block : result.getBlocks()) {
				if ("PAGE".equals(block.getBlockType())) {
					if (currentPage.length() > 0) {
						pageTexts.add(currentPage.toString());
						currentPage.setLength(0);
					}
				}
				if ("LINE".equals(block.getBlockType())) {
					currentPage.append(block.getText()).append("\n");
				}
			}
			if (currentPage.length() > 0) {
				pageTexts.add(currentPage.toString());
			}			
			for (int i = 0; i < pageTexts.size(); i++) {
				extractedTextFromDoc.add(pageTexts.get(i));
			}
			
		}catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw (e);
		}
		return extractedTextFromDoc;
	}

	public List<String> getAsyncTextExtraction(String documentPath, String bucketName) {
		List<String> extractedTextFromDoc = new ArrayList<String>();
		try {
			// Create the StartDocumentTextDetection request
			StartDocumentTextDetectionRequest request = new StartDocumentTextDetectionRequest()
					.withDocumentLocation(new DocumentLocation()
							.withS3Object(new S3Object().withBucket(bucketName).withName(documentPath)));

			// Start text detection
			StartDocumentTextDetectionResult result = this.textractClient.startDocumentTextDetection(request);

			// results
			GetDocumentTextDetectionRequest getRequest = new GetDocumentTextDetectionRequest()
					.withJobId(result.getJobId());
			GetDocumentTextDetectionResult getResult;
			String nextToken = null;

			do {
				getRequest.setNextToken(nextToken);
				do {
					getResult = this.textractClient.getDocumentTextDetection(getRequest);
					if (getResult.getJobStatus().equalsIgnoreCase("FAILED")) {
						extractedTextFromDoc.add("Must provide the valid path");
						break;
					}
				} while (!getResult.getJobStatus().equalsIgnoreCase(SUCCEEDED));
				nextToken = getResult.getNextToken();

				for (Block block : getResult.getBlocks()) {
					if (PAGE.equalsIgnoreCase(block.getBlockType())) {
						int pageNumber = block.getPage();
						StringBuilder pageText = new StringBuilder();
						for (Block item : getResult.getBlocks()) {
							if (item.getPage() == pageNumber && LINE.equalsIgnoreCase(item.getBlockType())) {
								pageText.append(item.getText());
							}
						}
						extractedTextFromDoc.add(pageText.toString());
					}
				}
			} while (nextToken != null);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e);
		}
		return extractedTextFromDoc;
	}
	
	public Insight getInsight(Object insightObj) {
		if (insightObj instanceof String) {
			return InsightStore.getInstance().get(insightObj);
		} else {
			return (Insight) insightObj;
		}
	}
	
	public boolean hasMoreThanPageLimits(File pdfPath) throws IOException {  
		try (PDDocument doc = Loader.loadPDF(pdfPath)) {
			if (doc.isEncrypted()) {
				throw new IOException("PDF is encrypted; cannot read page count without password.");
			}
			return doc.getNumberOfPages() > this.pageLength; 
		}
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.AWS_TEXTRACT_CUSTOM_EMBEDDINGS.name();
	}
}
