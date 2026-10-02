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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.StorageTypeEnum;
import prerna.engine.impl.storage.S3StorageEngine;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.Utility;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.transcribe.TranscribeClient;
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobRequest;
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobResponse;
import software.amazon.awssdk.services.transcribe.model.StartTranscriptionJobRequest;
import software.amazon.awssdk.services.transcribe.model.TranscriptionJobStatus;

public class AWSTranscribeFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSTranscribeFunctionEngine.class);

	protected static final String DIR_SEPARATOR = "/";
	protected static final String JSON_EXT = ".json";

	protected static final String ACCESS_KEY = "ACCESS_KEY";
	protected static final String SECRET_KEY = "SECRET_KEY";
	protected static final String REGION = "REGION";
	protected static final String BUCKETENGINEID = "S3BUCKETENGINEID";
	protected static final String OBJECT_PATH = "OBJECT_PATH";
	protected static final String STORAGE_TYPE = "STORAGE_TYPE";

	protected String accessKey;
	protected String secretKey;
	protected String region;
	protected String storageEngineId;
	protected String bucketName;
	protected String jobName;
	protected String objectPath;

	protected TranscribeClient transcribeClient = null;
	protected S3Client s3Client = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		// preset these - don't need user to define
		smssProp.putIfAbsent(IFunctionEngine.NAME_KEY, "AWS Transcribe Engines");
		smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY, "Execute AWS Transcribe");

		super.open(smssProp);

		this.accessKey = smssProp.getProperty(ACCESS_KEY);
		this.secretKey = smssProp.getProperty(SECRET_KEY);
		this.region = smssProp.getProperty(REGION);
		this.storageEngineId = smssProp.getProperty(BUCKETENGINEID);
		this.objectPath = smssProp.getProperty(OBJECT_PATH);

		if (this.requiredParameters == null || (this.requiredParameters.isEmpty())) {
			throw new RuntimeException("Must define the requiredParameters");
		}
		if (this.accessKey == null || this.accessKey.isEmpty()) {
			throw new RuntimeException("Must pass in an access key");
		}
		if (this.secretKey == null || this.secretKey.isEmpty()) {
			throw new RuntimeException("Must pass in a secret key");
		}
		if (this.region == null || this.region.isEmpty()) {
			throw new RuntimeException("Must pass in a region");
		}
		if (this.storageEngineId == null || this.storageEngineId.isEmpty()) {
			throw new RuntimeException("Must pass in a Storage Engine Id for an S3 Bucket");
		}
		if (this.objectPath == null || this.objectPath.isEmpty()) {
			throw new RuntimeException("Must pass in a object Path");
		}

		try {
			AwsBasicCredentials awsCreds = AwsBasicCredentials.create(this.accessKey, this.secretKey);
			Region awsRegion = Region.of(this.region);

			this.transcribeClient = TranscribeClient.builder()
					.credentialsProvider(StaticCredentialsProvider.create(awsCreds)).region(awsRegion).build();

			this.s3Client = S3Client.builder()
					.credentialsProvider(StaticCredentialsProvider.create(awsCreds)).region(awsRegion).build();

			IStorageEngine storageEngine = Utility.getStorage(this.storageEngineId);
			if (storageEngine.getStorageType() == StorageTypeEnum.AMAZON_S3
					|| storageEngine.getStorageType() == StorageTypeEnum.AMAZON_S3_NATIVE) {
				this.bucketName = storageEngine.getSmssProp().getProperty(S3StorageEngine.S3_BUCKET_KEY);
			} else {
				throw new IllegalArgumentException("Storage engine is not an Amazon S3 implementation.");
			}

		} catch (Exception e) {
			classLogger.error("Failed to initialize AWS Transcribe function engine.", e);
			throw e;
		}
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		String audioFileName = null;
		String output = null;
		File filePathDir = null;
		String folderPath = null;
		String filePath = null;
		String localFilePath = null;
		Insight insight = null;

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
			for (String key : parameterValues.keySet()) {
				if (key.equalsIgnoreCase("S3_FILE_PATH")) {
					filePath = parameterValues.get(key).toString();
				}
			}
			if (filePath == null || filePath.isEmpty()) {
				throw new IllegalArgumentException("Must define required key = S3_FILE_PATH");
			}
			filePathDir = new File(filePath);
			audioFileName = filePathDir.getName();
			folderPath = this.objectPath + DIR_SEPARATOR + audioFileName;
			insight = (Insight) parameterValues.get(Constants.INSIGHT);
			if (insight != null) {
				File localFile = new File(insight.getInsightFolder(), audioFileName);
				localFilePath = localFile.getAbsolutePath();
			}

			output = transcriptionTextFromAudio(folderPath, localFilePath, insight);

		} catch (Exception e) {
			classLogger.error("Failed to execute AWS Transcribe request for file path: " + filePath, e);
			throw new IllegalArgumentException(e);
		}

		return output;
	}

	protected String transcriptionTextFromAudio(String audioFilePath) throws Exception {
		return transcriptionTextFromAudio(audioFilePath, null, null);
	}

	protected String transcriptionTextFromAudio(String audioFilePath, String localFilePath, Insight insight)
			throws Exception {
		JSONObject transcriptionResult = getTranscriptionResultFromAudio(audioFilePath, localFilePath, insight);
		return extractTranscriptionText(transcriptionResult);
	}

	protected JSONObject getTranscriptionResultFromAudio(String audioFilePath, String localFilePath, Insight insight)
			throws Exception {
		JSONObject transcriptionResult = null;
		boolean uploadedAudioFile = false;
		try {
			ZoneId zoneId = Utility.getApplicationZoneIdObj();
			ZonedDateTime now = ZonedDateTime.now(ZoneId.of(zoneId.getId()));
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
			String formattedTimestamp = now.format(formatter);
			jobName = "jobName_" + formattedTimestamp;

			if (!fileExistsInS3(audioFilePath)) {
				validateStorageEditPermission(insight);
				addAudioFileToS3(audioFilePath, localFilePath);
				uploadedAudioFile = true;
			}

			String mediaFileUri = "https://s3-" + this.region + ".amazonaws.com/" + this.bucketName + "/"
					+ audioFilePath;

			StartTranscriptionJobRequest request = StartTranscriptionJobRequest.builder().transcriptionJobName(jobName)
					.languageCode("en-US").media(media -> media.mediaFileUri(mediaFileUri))
					.outputBucketName(this.bucketName).outputKey(this.objectPath + DIR_SEPARATOR).build();

			this.transcribeClient.startTranscriptionJob(request);

			while (true) {
				GetTranscriptionJobRequest getJobRequest = GetTranscriptionJobRequest.builder()
						.transcriptionJobName(jobName).build();

				GetTranscriptionJobResponse response = this.transcribeClient.getTranscriptionJob(getJobRequest);
				TranscriptionJobStatus status = response.transcriptionJob().transcriptionJobStatus();

				if (status == TranscriptionJobStatus.COMPLETED) {
					transcriptionResult = getTranscriptionResultFromS3(jobName);
					break;
				} else if (status == TranscriptionJobStatus.FAILED) {
					classLogger.error("AWS Transcribe job failed for audio file: " + audioFilePath
							+ " with job name: " + jobName);
					throw new IllegalArgumentException("Transcription job failed.");
				}
				Thread.sleep(5000);
			}
		} catch (Exception e) {
			classLogger.error("Failed while processing AWS Transcribe job for audio file: " + audioFilePath, e);
			throw e;
		} finally {
			if (uploadedAudioFile) {
				try {
					removeAudioFileFromS3(audioFilePath);
				} catch (Exception e) {
					classLogger.warn("Failed to clean up audio file from S3: " + audioFilePath, e);
				}
			}
		}
		return transcriptionResult;
	}

	protected String getTranscriptionTextFromS3(String jobName) throws Exception {
		return extractTranscriptionText(getTranscriptionResultFromS3(jobName));
	}

	protected JSONObject getTranscriptionResultFromS3(String jobName) throws Exception {
		JSONObject transcriptionResult = null;
		Path tempFile = null;
		try {
			String filePathInBucket = this.objectPath + DIR_SEPARATOR + jobName + JSON_EXT;
			tempFile = Files.createTempFile("file-temp-", JSON_EXT);
			GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(this.bucketName).key(filePathInBucket)
					.build();

			try (InputStream inputStream = s3Client.getObject(getObjectRequest)) {
				Files.copy(inputStream, tempFile, StandardCopyOption.REPLACE_EXISTING);
			}
			StringBuilder stringBuilder = new StringBuilder();
			try (BufferedReader reader = Files.newBufferedReader(tempFile)) {
				String line;
				while ((line = reader.readLine()) != null) {
					stringBuilder.append(line);
				}
				transcriptionResult = new JSONObject(stringBuilder.toString());
			} catch (Exception e) {
				classLogger.error("Failed to parse transcription result for job: " + jobName, e);
				throw e;
			} finally {
				if (tempFile != null) {
					try {
						Files.deleteIfExists(tempFile);
					} catch (IOException ioe) {
						classLogger.warn("Unable to delete temp file: " + tempFile, ioe);
					}
				}
				try {
					removeTranscriptionOutputFromS3(jobName);
				} catch (Exception e) {
					classLogger.warn("Failed to clean up transcription output from S3: " + jobName, e);
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to retrieve transcription result from S3 for job: " + jobName, e);
			throw e;
		}
		return transcriptionResult;
	}

	protected String extractTranscriptionText(JSONObject transcriptionResult) {
		JSONObject results = transcriptionResult.getJSONObject("results");
		JSONArray transcripts = results.getJSONArray("transcripts");
		return transcripts.getJSONObject(0).getString("transcript");
	}

	@Override
	public void close() throws IOException {
		if (this.transcribeClient != null) {
			this.transcribeClient.close();
		}
		if (this.s3Client != null) {
			this.s3Client.close();
		}
	}

	protected boolean fileExistsInS3(String audioFilePath) throws Exception {
		try {
			this.s3Client.headObject(HeadObjectRequest.builder().bucket(this.bucketName).key(audioFilePath).build());
			return true;
		} catch (NoSuchKeyException e) {
			return false;
		} catch (S3Exception e) {
			if (e.statusCode() == 404) {
				return false;
			}
			classLogger.error("Failed to check whether audio file exists in S3 for key: " + audioFilePath, e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Unexpected error while checking S3 for audio file key: " + audioFilePath, e);
			throw e;
		}
	}

	protected void addAudioFileToS3(String audioFilePath, String localFilePath) throws Exception {
		if (localFilePath == null || localFilePath.isEmpty()) {
			throw new IllegalArgumentException("No local file path available to upload to S3.");
		}
		File localFile = new File(localFilePath);
		if (!localFile.exists() || !localFile.isFile()) {
			throw new IllegalArgumentException("Local file does not exist: " + localFilePath);
		}
		try {
			Path localPath = Paths.get(localFile.getAbsolutePath());
			PutObjectRequest.Builder putRequestBuilder = PutObjectRequest.builder()
					.bucket(this.bucketName)
					.key(audioFilePath);
			String contentType = determineContentType(localPath);
			if (contentType != null) {
				putRequestBuilder.contentType(contentType);
			}
			this.s3Client.putObject(putRequestBuilder.build(), localPath);
		} catch (Exception e) {
			classLogger.error("Failed to upload audio file to S3 for key: " + audioFilePath, e);
			throw e;
		}
	}

	protected void removeAudioFileFromS3(String audioFilePath) {
		try {
			DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
					.bucket(this.bucketName)
					.key(audioFilePath)
					.build();
			this.s3Client.deleteObject(deleteRequest);
		} catch (Exception e) {
			classLogger.error("Failed to delete uploaded audio file from S3 for key: " + audioFilePath, e);
			throw e;
		}
	}

	protected void removeTranscriptionOutputFromS3(String transcriptionJobName) {
		try {
			String transcriptionOutputPath = this.objectPath + DIR_SEPARATOR + transcriptionJobName + JSON_EXT;
			DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
					.bucket(this.bucketName)
					.key(transcriptionOutputPath)
					.build();
			this.s3Client.deleteObject(deleteRequest);
		} catch (Exception e) {
			classLogger.error("Failed to delete transcription output from S3 for job: " + transcriptionJobName, e);
			throw e;
		}
	}

	protected void validateStorageEditPermission(Insight insight) {
		if (insight == null) {
			throw new IllegalArgumentException(
					"Insight is required to upload an audio file to S3 when the file is not already present.");
		}
		if (!SecurityEngineUtils.userCanEditEngine(insight.getUser(), this.storageEngineId)) {
			throw new IllegalArgumentException("Storage " + this.storageEngineId
					+ " does not exist or user does not have access to this engine");
		}
	}

	protected String determineContentType(Path localPath) {
		try {
			return Files.probeContentType(localPath);
		} catch (IOException e) {
			classLogger.warn("Unable to determine content type for local file: " + localPath, e);
			return null;
		}
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.AWS_TRANSCRIBE.name();
	}
}
