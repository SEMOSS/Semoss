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
import java.nio.file.Files;
import java.nio.file.Path;
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

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.StorageTypeEnum;
import prerna.engine.impl.storage.S3StorageEngine;
import prerna.util.Constants;
import prerna.util.Utility;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
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
		String audioFileName = null;
		String output = null;
		File filePathDir = null;
		String folderPath = null;
		String filePath = null;

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
			filePathDir = new File(filePath);
			audioFileName = filePathDir.getName();
			folderPath = this.objectPath + DIR_SEPARATOR + audioFileName;

			output = transcriptionTextFromAudio(folderPath);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e);
		}

		return output;
	}

	protected String transcriptionTextFromAudio(String audioFilePath) throws Exception {
		String transcriptionText = null;
		try {
			ZoneId zoneId = Utility.getApplicationZoneIdObj();
			ZonedDateTime now = ZonedDateTime.now(ZoneId.of(zoneId.getId()));
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
			String formattedTimestamp = now.format(formatter);
			jobName = "jobName_" + formattedTimestamp;

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
					transcriptionText = getTranscriptionTextFromS3(jobName);
					break;
				} else if (status == TranscriptionJobStatus.FAILED) {
					classLogger.error(Constants.STACKTRACE, "Transcription job failed.");
					throw new IllegalArgumentException("Transcription job failed.");
				}
				Thread.sleep(5000);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
		return transcriptionText;
	}

	protected String getTranscriptionTextFromS3(String jobName) throws Exception {
		String transcriptionText = null;
		Path tempFile = null;
		try {
			String filePathInBucket = this.objectPath + DIR_SEPARATOR + jobName + JSON_EXT;
			tempFile = Files.createTempFile("file-temp-", JSON_EXT);
			IStorageEngine storageEng = Utility.getStorage(this.storageEngineId);
			storageEng.copyToLocal(tempFile.toString(), filePathInBucket);
			StringBuilder stringBuilder = new StringBuilder();
			try (BufferedReader reader = Files.newBufferedReader(tempFile)) {
				String line;
				while ((line = reader.readLine()) != null) {
					stringBuilder.append(line);
				}
				JSONObject jsonobj = new JSONObject(stringBuilder.toString());
				JSONObject result = jsonobj.getJSONObject("results");
				JSONArray transcripts = result.getJSONArray("transcripts");
				transcriptionText = transcripts.getJSONObject(0).getString("transcript");
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
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
					storageEng.deleteFromStorage(
							this.bucketName + DIR_SEPARATOR + this.objectPath + DIR_SEPARATOR + jobName);
				} catch (Exception e) {
					classLogger.error("Failed to delete file from the storage ", e);
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
		return transcriptionText;
	}

	@Override
	public void close() throws IOException {
		if (this.transcribeClient != null) {
			this.transcribeClient.close();
		}
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.AWS_TRANSCRIBE.name();
	}
}
