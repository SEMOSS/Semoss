/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.StorageTypeEnum;
import prerna.engine.impl.storage.GoogleCloudStorageEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.util.Constants;
import prerna.util.Utility;

public class GoogleOCRFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(GoogleOCRFunctionEngine.class);

	protected static final String DIR_SEPARATOR = "/";

	protected static final String BUCKETENGINEID = "GOOGLE_BUCKET_ENGINEID";
	protected static final String SMSS_KEY_SERVICE_ACCOUNT_CREDENTIALS = "SERVICE_ACCOUNT_CREDENTIALS";
	protected static final String REGION = "REGION";
	protected static final String PROJECT_ID = "PROJECT_ID";
	protected static final String PROCESSOR_ID = "PROCESSOR_ID";
	protected static final String PAGE_LENGTH = "PAGE_LENGTH";
	protected static final String OBJECT_PATH = "OBJECT_PATH";
	protected static final String STORAGE_TYPE = "STORAGE_TYPE";
	protected static final String JSON_EXT = ".json";
	protected static final String OUTPUT = "output";

	protected String projectId;
	protected String processorId;
	protected String region;
	protected String googleStorageEngineId;
	protected String ServiceAccountFile;
	protected Storage storage = null;
	protected String prefix = "";
	protected String bucketName = "";
	protected String objectPath;
	protected int pageLength = 5;
	protected DocumentProcessorServiceClient client = null;
	protected ImageAnnotatorSettings settings = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		// preset these - don't need user to define
		smssProp.putIfAbsent(IFunctionEngine.NAME_KEY, "Google OCR");
		smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY, "Execute Google OCR");
		super.open(smssProp);

		this.projectId = smssProp.getProperty(PROJECT_ID);
		this.processorId = smssProp.getProperty(PROCESSOR_ID);
		this.region = smssProp.getProperty(REGION);
		this.googleStorageEngineId = smssProp.getProperty(BUCKETENGINEID);
		this.objectPath = smssProp.getProperty(OBJECT_PATH);
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

		if (this.objectPath == null || this.objectPath.isEmpty()) {
			throw new RuntimeException("Must pass in a object path");
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

			IStorageEngine storageEngine = Utility.getStorage(this.googleStorageEngineId);
			if (storageEngine.getStorageType() == StorageTypeEnum.GOOGLE_CLOUD_STORAGE
					|| storageEngine.getStorageType() == StorageTypeEnum.GOOGLE_CLOUD_STORAGE) {
				this.bucketName = storageEngine.getSmssProp().getProperty(GoogleCloudStorageEngine.GCS_BUCKET_KEY);
			} else {
				throw new IllegalArgumentException("Storage engine is not an Amazon S3 implementation.");
			}

		} catch (IOException e) {
			classLogger.error("Failed to initialize the Google OCR client for projectId={} region={} processorId={}",
					this.projectId, this.region, this.processorId, e);
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
				} else if (k.equalsIgnoreCase(Constants.CUSTOM_DOCUMENT_PROCESSOR_USE_STORAGE)) {
					saveFileToStorage = Boolean.parseBoolean(parameterValues.get(k).toString());
				}
			}
			IStorageEngine storageEng = Utility.getStorage(this.googleStorageEngineId);
			boolean pdf = filePath.getName().toLowerCase().endsWith(".pdf");
			if (pdf) {
				Insight insight = (Insight) parameterValues.get(Constants.INSIGHT);
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
					classLogger.info("{} for: {}", WAITING_INFO, filePath.getName());
					extractedTextFromDoc = getAsyncTextExtraction(pdfFilePath);

					storageEng.deleteFromStorage(DIR_SEPARATOR + this.objectPath + filePath.getName());
				} else {
					if (hasMoreThanPageLimits(pdfFilePath, this.pageLength)) {
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
			classLogger.error("Google OCR failed to extract text from: {}", fileDir, e);
			throw new IllegalArgumentException(e);
		}
		return extractedTextFromDoc;
	}

	/**
	 * 
	 * @param fileToProcess
	 * @return
	 * @throws Exception
	 */
	protected List<String> getSyncTextExtraction(File fileToProcess) throws Exception {
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

	/**
	 * 
	 * @param fileToProcess
	 * @return
	 * @throws Exception
	 */
	protected List<String> getAsyncTextExtraction(File fileToProcess) throws Exception {
		List<String> extractedTextFromDoc = new ArrayList<String>();
		final String PROCESSORNAME_FORMAT = "projects/%s/locations/%s/processors/%s";
		final String END_INFO = "Document processing complete.";
		String fileName = fileToProcess.getName();

		String processorName = String.format(PROCESSORNAME_FORMAT, this.projectId, this.region, this.processorId);

		String filePathInBucket = this.prefix + this.bucketName + DIR_SEPARATOR + this.objectPath + fileName;

		// File inputFile = new File(filePathInBucket);
		try {
			String mimeType = URLConnection.guessContentTypeFromName(fileName);

			if (mimeType == null) {
				try (FileInputStream fis = new FileInputStream(fileToProcess)) {
					mimeType = URLConnection.guessContentTypeFromStream(fis);
				} catch (IOException e) {
					// not fatal, the request is still sent with a null mime type
					classLogger.error("Unable to determine the mime type of: {}", fileName, e);
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

			classLogger.info("{} for: {}", END_INFO, fileName);

			extractedTextFromDoc = getTextFromStorage(outputFileName);

		} catch (Exception e) {
			classLogger.error("Google OCR batch processing failed for: {} using processor: {}", filePathInBucket,
					processorName, e);
			throw e;
		}
		return extractedTextFromDoc;

	}

	/**
	 * 
	 * @param outputFileName
	 * @return
	 * @throws Exception
	 */
	protected List<String> getTextFromStorage(String outputFileName) throws IOException {
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
					classLogger.error("Failed to read the OCR output blob {} from bucket {}", blob.getName(),
							this.bucketName, e);
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

	protected String getText(Document.TextAnchor textAnchor, String text) {
		if (textAnchor.getTextSegmentsList().size() > 0) {
			int startIdx = (int) textAnchor.getTextSegments(0).getStartIndex();
			int endIdx = (int) textAnchor.getTextSegments(0).getEndIndex();
			return text.substring(startIdx, endIdx);
		}
		return " ";
	}

	/**
	 * 
	 * @param pdfPath
	 * @param page_length
	 * @return
	 * @throws IOException
	 */
	protected boolean hasMoreThanPageLimits(File pdfPath, int page_length) throws IOException {
		try (PDDocument doc = Loader.loadPDF(pdfPath)) {
			if (doc.isEncrypted()) {
				throw new IOException("PDF is encrypted; cannot read page count without password.");
			}
			return doc.getNumberOfPages() > page_length;
		}
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
				classLogger.error("Failed to close the Google Cloud Storage client for bucket: {}", this.bucketName, e);
			}
		}
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.GOOGLE_OCR.name();
	}

}
