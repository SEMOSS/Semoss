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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.transcribe.TranscribeClient;
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobRequest;
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobResponse;
import software.amazon.awssdk.services.transcribe.model.StartTranscriptionJobRequest;
import software.amazon.awssdk.services.transcribe.model.TranscriptionJobStatus;

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IStorageEngine;
import prerna.util.Constants;
import prerna.util.Utility;

public class AWSTranscribeFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSTranscribeFunctionEngine.class);

	private static final String DIR_SEPARATOR = "/";

	public static final String JSON_EXT = ".json";

	public static final String ACCESS_KEY = "ACCESS_KEY";
	public static final String SECRET_KEY = "SECRET_KEY";
	public static final String REGION = "REGION";
	public static final String BUCKETENGINEID = "S3BUCKETENGINEID";

	private String accessKey;
	private String secretKey;
	private String region;
	private String storageEngineId;
	private String bucketName;
	private String objectPath;

	private TranscribeClient transcribeClient = null;

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

		try {
			AwsBasicCredentials awsCreds = AwsBasicCredentials.create(this.accessKey, this.secretKey);
			Region awsRegion = Region.of(this.region);

			this.transcribeClient = TranscribeClient.builder()
					.credentialsProvider(StaticCredentialsProvider.create(awsCreds)).region(awsRegion).build();

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
			if (filePath.startsWith("s3://")) {
				filePath = filePath.replace("s3://", "");
				int startIndex = filePath.indexOf(DIR_SEPARATOR);
				int endIndex = filePath.lastIndexOf(DIR_SEPARATOR);
				this.bucketName = filePath.substring(0, startIndex);

				if (startIndex < endIndex && startIndex < filePath.length()) {
					this.objectPath = filePath.substring(startIndex + 1, endIndex);
				} else if (startIndex == endIndex && startIndex < filePath.length()) {
					this.objectPath = filePath.substring(startIndex + 1, filePath.length());
				}
			} else {
				throw new IllegalArgumentException("Must provide the valid path.");
			}

			folderPath = this.objectPath + DIR_SEPARATOR + audioFileName;

			output = transcriptionTextFromAudio(folderPath);

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e);
		}

		return output;
	}

	public String transcriptionTextFromAudio(String audioFilePath) throws Exception {
		String transcriptionText = null;
		String jobName = null;
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

	public String getTranscriptionTextFromS3(String jobName) throws Exception {
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
					storageEng.deleteFromStorage(this.bucketName + DIR_SEPARATOR + this.objectPath + DIR_SEPARATOR + jobName);
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
		// TODO Auto-generated method stub
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.AWS_TRANSCRIBE.name();
	}
}
