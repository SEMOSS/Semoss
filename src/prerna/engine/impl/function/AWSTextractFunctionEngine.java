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
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.StorageTypeEnum;
import prerna.engine.impl.storage.S3StorageEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
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

public class AWSTextractFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSTextractCustomEmbeddingsFunctionEngine.class);

	protected static final String DIR_SEPARATOR = "/";

	protected static final String ACCESS_KEY = "ACCESS_KEY";
	protected static final String SECRET_KEY = "SECRET_KEY";
	protected static final String REGION = "REGION";
	protected static final String BUCKETENGINEID = "S3BUCKETENGINEID";
	protected static final String PAGE_LENGTH = "PAGE_LENGTH";
	protected static final String OBJECT_PATH = "OBJECT_PATH";
	protected static final String STORAGE_TYPE = "STORAGE_TYPE";

	protected String accessKey;
	protected String secretKey;
	protected String region;
	protected String storageEngineId;
	protected String bucketName;
	protected String objectPath;
	protected int pageLength = 1;

	protected TextractClient textractClient = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		// preset these - don't need user to define
		smssProp.putIfAbsent(IFunctionEngine.NAME_KEY, "AWS Textract Engines");
		smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY, "Execute AWS Textract");

		super.open(smssProp);

		this.accessKey = smssProp.getProperty(ACCESS_KEY);
		this.secretKey = smssProp.getProperty(SECRET_KEY);
		this.region = smssProp.getProperty(REGION);
		this.storageEngineId = smssProp.getProperty(BUCKETENGINEID);
		this.objectPath = smssProp.getProperty(OBJECT_PATH);
		this.pageLength = Integer.parseInt(smssProp.getProperty(PAGE_LENGTH));

		if (this.accessKey == null || this.accessKey.isEmpty()) {
			throw new RuntimeException("Must pass in an access key");
		}
		if (this.secretKey == null || this.secretKey.isEmpty()) {
			throw new RuntimeException("Must pass in a secret key");
		}
		if (this.region == null || this.region.isEmpty()) {
			throw new RuntimeException("Must define the region");
		}
		if (this.storageEngineId == null || this.storageEngineId.isEmpty()) {
			throw new RuntimeException("Must pass in a Storage Engine Id for an S3 Bucket");
		}
		if (this.requiredParameters == null || (this.requiredParameters.isEmpty())) {
			throw new RuntimeException("Must define the requiredParameters");
		}
		if (this.objectPath == null || this.objectPath.isEmpty()) {
			throw new RuntimeException("Must pass in a object Path");
		}

		try {
			AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKey, secretKey);

			this.textractClient = TextractClient.builder().region(Region.of(this.region))
					.credentialsProvider(StaticCredentialsProvider.create(awsCreds)).build();

			IStorageEngine storageEngine = Utility.getStorage(this.storageEngineId);
			if (storageEngine.getStorageType() == StorageTypeEnum.AMAZON_S3
					|| storageEngine.getStorageType() == StorageTypeEnum.AMAZON_S3_NATIVE) {
				this.bucketName = storageEngine.getSmssProp().getProperty(S3StorageEngine.S3_BUCKET_KEY);
			} else {
				throw new IllegalArgumentException("Storage engine is not an Amazon S3 implementation.");
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
				Insight insight = (Insight) parameterValues.get(Constants.INSIGHT);
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
							this.bucketName + DIR_SEPARATOR + this.objectPath + documentKeyName, metadata);
					extractedTextFromDoc = getAsyncTextExtraction(folderPath, this.bucketName);
					storageeng.deleteFromStorage(this.bucketName + DIR_SEPARATOR + this.objectPath + documentKeyName);
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
						"Please provide valid input files using \"FILE_PATH\". File types supported include: pdf");
			}

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
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
	protected List<String> getAsyncTextExtraction(String documentPath, String bucketName) {
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
		if (this.textractClient != null) {
			this.textractClient.close();
		}
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.AWS_TEXTRACT.name();
	}
}
