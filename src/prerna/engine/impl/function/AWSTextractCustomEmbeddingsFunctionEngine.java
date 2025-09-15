package prerna.engine.impl.function;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.textract.TextractClient;
import software.amazon.awssdk.services.textract.model.AnalyzeDocumentRequest;
import software.amazon.awssdk.services.textract.model.AnalyzeDocumentResponse;
import software.amazon.awssdk.services.textract.model.Block;
import software.amazon.awssdk.services.textract.model.BlockType;
import software.amazon.awssdk.services.textract.model.Document;
import software.amazon.awssdk.services.textract.model.DocumentLocation;
import software.amazon.awssdk.services.textract.model.FeatureType;
import software.amazon.awssdk.services.textract.model.GetDocumentTextDetectionRequest;
import software.amazon.awssdk.services.textract.model.GetDocumentTextDetectionResponse;
import software.amazon.awssdk.services.textract.model.JobStatus;
import software.amazon.awssdk.services.textract.model.S3Object;
import software.amazon.awssdk.services.textract.model.StartDocumentTextDetectionRequest;
import software.amazon.awssdk.services.textract.model.StartDocumentTextDetectionResponse;

public class AWSTextractCustomEmbeddingsFunctionEngine extends AbstractFunctionEngine
		implements ICustomEmbeddingsFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSTextractCustomEmbeddingsFunctionEngine.class);

	private static final String DIR_SEPARATOR = "/";

	private static final String ACCESS_KEY = "ACCESS_KEY";
	private static final String SECRET_KEY = "SECRET_KEY";
	private static final String REGION = "REGION";
	private static final String BUCKETENGINEID = "S3BUCKETENGINEID";
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

	private TextractClient textractClient = null;

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
			AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKey, secretKey);

			this.textractClient = TextractClient.builder().region(Region.of(this.region))
					.credentialsProvider(StaticCredentialsProvider.create(awsCreds)).build();

			this.storagePath = this.storagePath.replace("s3://", "");
			int startIndex = this.storagePath.indexOf(DIR_SEPARATOR);
			int endIndex = this.storagePath.lastIndexOf(DIR_SEPARATOR);
			this.bucketName = this.storagePath.substring(0, startIndex);

			if (startIndex < endIndex && startIndex < this.storagePath.length()) {
				this.objectPath = this.storagePath.substring(startIndex + 1, endIndex);
			} else if (startIndex == endIndex && startIndex < this.storagePath.length()) {
				this.objectPath = this.storagePath.substring(startIndex + 1, this.storagePath.length());
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
				} else if (k.equalsIgnoreCase(Constants.CUSTOM_DOCUMENT_PROCESSOR_USE_STORAGE)) {
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
					storageeng.deleteFromStorage(fileDir);
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
						"Please provide valid input files using \"FILE_PATH\". File types supported include: pdf");
			}

		} catch (Exception e) {
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
		String documentKeyName = fileToProcess.getName();
		parameters.put("FILE_PATH", fileToProcess);
		try {
			extractedTextFromDoc = (List<String>) execute(parameters);

			for (int i = 0; i < extractedTextFromDoc.size(); i++) {
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

	/**
	 * 
	 * @param fileToProcess
	 * @return
	 * @throws Exception
	 */
	private List<String> getSyncTextExtraction(File fileToProcess) throws Exception {
		List<String> extractedTextFromDoc = new ArrayList<String>();
		try {
			byte[] fileBytes = Files.readAllBytes(fileToProcess.toPath());
			SdkBytes imageBytes = SdkBytes.fromByteArray(fileBytes);

			Document document = Document.builder().bytes(imageBytes).build();
			AnalyzeDocumentRequest request = AnalyzeDocumentRequest.builder().document(document)
					.featureTypes(FeatureType.TABLES, FeatureType.FORMS).build();

			AnalyzeDocumentResponse result = this.textractClient.analyzeDocument(request);

			// Extract text page-wise
			List<String> pageTexts = new ArrayList<>();
			StringBuilder currentPage = new StringBuilder();

			for (Block block : result.blocks()) {
				if (BlockType.PAGE.equals(block.blockType())) {
					if (currentPage.length() > 0) {
						pageTexts.add(currentPage.toString());
						currentPage.setLength(0);
					}
				}
				if (BlockType.LINE.equals(block.blockType())) {
					currentPage.append(block.text()).append("\n");
				}
			}
			if (currentPage.length() > 0) {
				pageTexts.add(currentPage.toString());
			}

			extractedTextFromDoc.addAll(pageTexts);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw (e);
		}
		return extractedTextFromDoc;
	}

	/**
	 * 
	 * @param documentPath
	 * @param bucketName
	 * @return
	 */
	private List<String> getAsyncTextExtraction(String documentPath, String bucketName) {
		List<String> extractedTextFromDoc = new ArrayList<>();
		try {
			// Create the StartDocumentTextDetection request
			StartDocumentTextDetectionRequest request = StartDocumentTextDetectionRequest.builder()
					.documentLocation(DocumentLocation.builder()
							.s3Object(S3Object.builder().bucket(bucketName).name(documentPath).build()).build())
					.build();

			// Start text detection
			StartDocumentTextDetectionResponse result = this.textractClient.startDocumentTextDetection(request);

			// Get results
			GetDocumentTextDetectionRequest getRequest = GetDocumentTextDetectionRequest.builder().jobId(result.jobId())
					.build();

			GetDocumentTextDetectionResponse getResult;
			String nextToken = null;

			do {
				// Update request with next token if available
				getRequest = getRequest.toBuilder().nextToken(nextToken).build();

				do {
					getResult = this.textractClient.getDocumentTextDetection(getRequest);
					if (JobStatus.FAILED.equals(getResult.jobStatus())) {
						extractedTextFromDoc.add("Must provide the valid path");
						return extractedTextFromDoc; // Early return on failure
					}

					// Add a small delay to avoid excessive polling
					try {
						Thread.sleep(1000);
					} catch (InterruptedException ie) {
						Thread.currentThread().interrupt();
						throw new RuntimeException("Thread interrupted getting document status", ie);
					}

				} while (!JobStatus.SUCCEEDED.equals(getResult.jobStatus()));

				nextToken = getResult.nextToken();

				// Process blocks for this batch
				for (Block block : getResult.blocks()) {
					if (BlockType.PAGE.equals(block.blockType())) {
						Integer pageNumber = block.page();
						StringBuilder pageText = new StringBuilder();

						for (Block item : getResult.blocks()) {
							if (pageNumber.equals(item.page()) && BlockType.LINE.equals(item.blockType())) {
								pageText.append(item.text()).append("\n");
							}
						}
						extractedTextFromDoc.add(pageText.toString());
					}
				}
			} while (nextToken != null && !nextToken.isEmpty());

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e);
		}
		return extractedTextFromDoc;
	}

	/**
	 * 
	 * @param insightObj
	 * @return
	 */
	private Insight getInsight(Object insightObj) {
		if (insightObj instanceof String) {
			return InsightStore.getInstance().get(insightObj);
		} else {
			return (Insight) insightObj;
		}
	}

	/**
	 * 
	 * @param pdfPath
	 * @return
	 * @throws IOException
	 */
	private boolean hasMoreThanPageLimits(File pdfPath) throws IOException {
		try (PDDocument doc = Loader.loadPDF(pdfPath)) {
			if (doc.isEncrypted()) {
				throw new IOException("PDF is encrypted; cannot read page count without password.");
			}
			return doc.getNumberOfPages() > this.pageLength;
		}
	}

	@Override
	public void close() throws IOException {
		if (this.textractClient != null) {
			this.textractClient.close();
		}
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.AWS_TEXTRACT_CUSTOM_EMBEDDINGS.name();
	}
}
