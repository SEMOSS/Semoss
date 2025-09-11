package prerna.engine.impl.function;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLConnection;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.documentai.v1.BatchDocumentsInputConfig;
import com.google.cloud.documentai.v1.BatchProcessMetadata;
import com.google.cloud.documentai.v1.BatchProcessRequest;
import com.google.cloud.documentai.v1.BatchProcessResponse;
import com.google.cloud.documentai.v1.Document;
import com.google.cloud.documentai.v1.DocumentOutputConfig;
import com.google.cloud.documentai.v1.DocumentOutputConfig.GcsOutputConfig;
import com.google.cloud.documentai.v1.DocumentProcessorServiceClient;
import com.google.cloud.documentai.v1.DocumentProcessorServiceSettings;
import com.google.cloud.documentai.v1.GcsDocument;
import com.google.cloud.documentai.v1.GcsDocuments;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.vision.v1.AnnotateFileRequest;
import com.google.cloud.vision.v1.AnnotateFileResponse;
import com.google.cloud.vision.v1.AnnotateImageResponse;
import com.google.cloud.vision.v1.BatchAnnotateFilesRequest;
import com.google.cloud.vision.v1.BatchAnnotateFilesResponse;
import com.google.cloud.vision.v1.Feature;
import com.google.cloud.vision.v1.ImageAnnotatorClient;
import com.google.cloud.vision.v1.ImageAnnotatorSettings;
import com.google.cloud.vision.v1.InputConfig;
import com.google.cloud.vision.v1.TextAnnotation;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;

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

public class GoogleOCRCustomEmbeddingsFunctionEngine extends AbstractFunctionEngine
		implements ICustomEmbeddingsFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(GoogleOCRCustomEmbeddingsFunctionEngine.class);

	private static final String DIR_SEPARATOR = "/";

	private static final String BUCKETENGINEID = "GOOGLE_BUCKET_ENGINEID";
	private static final String SMSS_KEY_SERVICE_ACCOUNT_CREDENTIALS = "SERVICE_ACCOUNT_CREDENTIALS";
	private static final String REGION = "REGION";
	private static final String PROJECT_ID = "PROJECT_ID";
	private static final String PROCESSOR_ID = "PROCESSOR_ID";
	private static final String STORAGE_PATH = "STORAGE_PATH";
	private static final String PAGE_LENGTH = "PAGE_LENGTH";
	private static final String JSON_EXT = ".json";
	private static final String OUTPUT = "output";

	private String projectId;
	private String processorId;
	private String region;
	private String googleStorageEngineId;
	private String storagePath;
	private String ServiceAccountFile;
	private Storage storage = null;
	private String prefix = "";
	private String bucketName = "";
	private String objectPath = "";
	private int pageLength = 5;
	private DocumentProcessorServiceClient client = null;
	private ImageAnnotatorSettings settings = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		// preset these - don't need user to define
		smssProp.putIfAbsent(IFunctionEngine.NAME_KEY, "Google OCR - For Use With Vector Database Engines");
		smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY, "Execute Google OCR");
		super.open(smssProp);

		this.projectId = smssProp.getProperty(PROJECT_ID);
		this.processorId = smssProp.getProperty(PROCESSOR_ID);
		this.region = smssProp.getProperty(REGION);
		this.googleStorageEngineId = smssProp.getProperty(BUCKETENGINEID);
		this.storagePath = smssProp.getProperty(STORAGE_PATH);
		this.ServiceAccountFile = smssProp.getProperty(SMSS_KEY_SERVICE_ACCOUNT_CREDENTIALS);
		this.pageLength = Integer.parseInt(smssProp.getProperty(PAGE_LENGTH));

		final String SCOPE_KEY = "https://www.googleapis.com/auth/cloud-platform";
		final String SCOPE_VALUE = "https://www.googleapis.com/auth/cloud-platform.read-only";
		final String ENDPOINT_FORMAT = "%s-documentai.googleapis.com:443";

		if (this.projectId == null || this.projectId.isEmpty()) {
			throw new RuntimeException("Must pass in a project Id");
		}
		if (this.processorId == null || this.processorId.isEmpty()) {
			throw new RuntimeException("Must pass in a processor Id");
		}
		if (this.region == null || this.region.isEmpty()) {
			throw new RuntimeException("Must pass in a region");
		}

		if (this.googleStorageEngineId == null || this.googleStorageEngineId.isEmpty()) {
			throw new RuntimeException("Must pass in a google bucket EngineId");
		}

		if (this.ServiceAccountFile == null || this.ServiceAccountFile.isEmpty()) {
			throw new RuntimeException("Must pass in a Service Account File");
		}

		if (this.storagePath == null || this.storagePath.isEmpty()) {
			throw new RuntimeException("Must pass in a storage path");
		}

		try {
			GoogleCredentials credentials = GoogleCredentials
					.fromStream(new ByteArrayInputStream(this.ServiceAccountFile.getBytes()))
					.createScoped(SCOPE_KEY, SCOPE_VALUE);

			FixedCredentialsProvider credentialsProvider = FixedCredentialsProvider.create(credentials);
			String endpoint = String.format(ENDPOINT_FORMAT, this.region);

			this.client = DocumentProcessorServiceClient.create(DocumentProcessorServiceSettings.newBuilder()
					.setCredentialsProvider(credentialsProvider).setEndpoint(endpoint).build());
			this.storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
			this.settings = ImageAnnotatorSettings.newBuilder().setCredentialsProvider(() -> credentials).build();
			
			if (this.storagePath.startsWith("gs://")) {
				this.prefix = "gs://";
				String withoutPrefix = this.storagePath.substring(this.prefix.length());

				int firstSlashIndex = withoutPrefix.indexOf('/');
				if (firstSlashIndex != -1) {
					this.bucketName = withoutPrefix.substring(0, firstSlashIndex);
					this.objectPath = withoutPrefix.substring(firstSlashIndex + 1);
				} else {
					this.bucketName = withoutPrefix;
				}
			}

		} catch (IOException e) {
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
		final String WAITING_INFO = "Waiting for operation to complete...";

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
			IStorageEngine storageEng = Utility.getStorage(this.googleStorageEngineId);
			boolean pdf = filePath.getName().toLowerCase().endsWith(".pdf");
			if (pdf) {
				Insight insight = getInsight(parameterValues.get("INSIGHT"));
				String insightId = insight.getInsightId();
				Insight in = InsightStore.getInstance().get(insightId);
				File instanceDir = new File(Utility.normalizePath(in.getInsightFolder()));

				fileDir = instanceDir + DIR_SEPARATOR + filePath.getName();
				File pdfFilePath = new File(fileDir);

				if (saveFileToStorage) {
					if (!SecurityEngineUtils.userCanEditEngine(insight.getUser(), this.googleStorageEngineId)) {
						throw new IllegalArgumentException("Storage " + this.googleStorageEngineId
							+ " does not exist or user does not have access to this engine");
					}
					Map<String, Object> metadata = new HashMap<>();
					metadata.put("utility", filePath.getName() + "- GoogleOCR_functionality");

					storageEng.copyToStorage(fileDir,
							this.bucketName + DIR_SEPARATOR + this.objectPath + filePath.getName(), metadata);
					classLogger.info(WAITING_INFO);
					extractedTextFromDoc = getAsyncTextExtraction(pdfFilePath);
					
					storageEng.deleteFromStorage(fileDir);
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
		String fileName = fileToProcess.getName();
		parameters.put("FILE_PATH",fileToProcess);
		try {			
			extractedTextFromDoc = (List<String>) execute(parameters);
			for (int i = 0; i < extractedTextFromDoc.size(); i++) {
				writer.writeRow(fileName, String.valueOf(i + 1), extractedTextFromDoc.get(i));
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

		ByteString content = ByteString.readFrom(new FileInputStream(fileToProcess));	
		InputConfig inputConfig = InputConfig.newBuilder().setMimeType("application/pdf").setContent(content).build();
		Feature feature = Feature.newBuilder().setType(Feature.Type.DOCUMENT_TEXT_DETECTION).build();
		AnnotateFileRequest request = AnnotateFileRequest.newBuilder().addFeatures(feature).setInputConfig(inputConfig)
				.build();

		// Wrap in batch request
		BatchAnnotateFilesRequest batchRequest = BatchAnnotateFilesRequest.newBuilder().addRequests(request).build();

		try (ImageAnnotatorClient client = ImageAnnotatorClient.create(settings)) {
			BatchAnnotateFilesResponse response = client.batchAnnotateFiles(batchRequest);

			for (AnnotateFileResponse fileResponse : response.getResponsesList()) {
				for (AnnotateImageResponse imageResponse : fileResponse.getResponsesList()) {
					if (imageResponse.hasFullTextAnnotation()) {
						TextAnnotation annotation = imageResponse.getFullTextAnnotation();

						// Extract text page-wise
						for (com.google.cloud.vision.v1.Page page : annotation.getPagesList()) {
							StringBuilder pageText = new StringBuilder();
							page.getBlocksList()
									.forEach(block -> block.getParagraphsList().forEach(
											paragraph -> paragraph.getWordsList().forEach(word -> word.getSymbolsList()
													.forEach(symbol -> pageText.append(symbol.getText())))));
							extractedTextFromDoc.add(pageText.toString());
						}
					}
				}
			}
		}

		return extractedTextFromDoc;
	}

	private List<String> getAsyncTextExtraction(File fileToProcess) throws Exception {

		String filePathInBucket = null;
		List<String> extractedTextFromDoc = new ArrayList<String>();
		final String PROCESSORNAME_FORMAT = "projects/%s/locations/%s/processors/%s";
		final String END_INFO = "Document processing complete.";
		String fileName = fileToProcess.getName();

		String processorName = String.format(PROCESSORNAME_FORMAT, this.projectId, this.region, this.processorId);

		filePathInBucket = this.prefix + this.bucketName + DIR_SEPARATOR + this.objectPath + fileName;

		// File inputFile = new File(filePathInBucket);
		try {
			String mimeType = URLConnection.guessContentTypeFromName(fileName);

			if (mimeType == null) {
				try (FileInputStream fis = new FileInputStream(fileToProcess)) {
					mimeType = URLConnection.guessContentTypeFromStream(fis);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}

			GcsDocument gcsDocument = GcsDocument.newBuilder().setGcsUri(filePathInBucket).setMimeType(mimeType)
					.build();

			GcsDocuments gcsDocuments = GcsDocuments.newBuilder().addDocuments(gcsDocument).build();

			BatchDocumentsInputConfig inputConfig = BatchDocumentsInputConfig.newBuilder().setGcsDocuments(gcsDocuments)
					.build();

			LocalDateTime now = LocalDateTime.now();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
			String formattedTimestamp = now.format(formatter);

			String outputFileName = fileName.concat("_" + formattedTimestamp + JSON_EXT);

			String fullGcsPath = String.format("gs://%s/%s/%s", this.bucketName, this.objectPath + OUTPUT,
					outputFileName);
			GcsOutputConfig gcsOutputConfig = GcsOutputConfig.newBuilder().setGcsUri(fullGcsPath).build();

			DocumentOutputConfig documentOutputConfig = DocumentOutputConfig.newBuilder()
					.setGcsOutputConfig(gcsOutputConfig).build();

			// Configure the batch process request.
			BatchProcessRequest request = BatchProcessRequest.newBuilder().setName(processorName)
					.setInputDocuments(inputConfig).setDocumentOutputConfig(documentOutputConfig).build();

			OperationFuture<BatchProcessResponse, BatchProcessMetadata> future = client
					.batchProcessDocumentsAsync(request);

			future.get();

			classLogger.info(END_INFO);

			extractedTextFromDoc = getTextFromStorage(outputFileName);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
		return extractedTextFromDoc;

	}

	private List<String> getTextFromStorage(String outputFileName) throws IOException {
		List<String> extractedTextFromDoc = new ArrayList<String>();

		Bucket bucket = this.storage.get(this.bucketName);

		Page<Blob> blobs = bucket
				.list(Storage.BlobListOption.prefix(this.objectPath + OUTPUT + DIR_SEPARATOR + outputFileName));
		for (Blob blob : blobs.iterateAll()) {
			if (!blob.isDirectory()) {
				File tempFile = null;
				try {
					tempFile = File.createTempFile("file", JSON_EXT);
					Blob fileInfo = storage.get(BlobId.of(this.bucketName, blob.getName()));
					fileInfo.downloadTo(tempFile.toPath());
					try (FileReader reader = new FileReader(tempFile)) {
						Document.Builder builder = Document.newBuilder();
						JsonFormat.parser().merge(reader, builder);
						Document document = builder.build();
						for (int pageIndex = 0; pageIndex < document.getPagesCount(); pageIndex++) {
							Document.Page page = document.getPages(pageIndex); // Get the current page
							String pageText = getText(page.getLayout().getTextAnchor(), document.getText());
							extractedTextFromDoc.add(pageText); // Store page-wise text
						}

					}
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw e;
				} finally {
					if (tempFile != null && tempFile.exists()) {
						tempFile.delete();
					}
				}
			}
		}

		return extractedTextFromDoc;
	}

	private static String getText(Document.TextAnchor textAnchor, String text) {
		if (textAnchor.getTextSegmentsList().size() > 0) {
			int startIdx = (int) textAnchor.getTextSegments(0).getStartIndex();
			int endIdx = (int) textAnchor.getTextSegments(0).getEndIndex();
			return text.substring(startIdx, endIdx);
		}
		return " ";
	}

	@Override
	public void close() throws IOException {
		if (this.client != null) {
			this.client.close();
		}
		if (this.storage != null) {
			try {
				this.storage.close();
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
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
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.GOOGLE_OCR_CUSTOM_EMBEDDINGS.name();
	}
}