package prerna.engine.impl.function;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.ICustomEmbeddingsFunctionEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.om.Insight;
import prerna.om.InsightStore;
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

public class AWSTranscribeCustomEmbeddingsFunctionEngine extends AbstractFunctionEngine
		implements ICustomEmbeddingsFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSTextractCustomEmbeddingsFunctionEngine.class);
	private static final String DIR_SEPARATOR = "/";
	public static final String JSON_EXT = ".json";

	public static final String ACCESS_KEY = "ACCESS_KEY";
	public static final String SECRET_KEY = "SECRET_KEY";
	public static final String REGION = "REGION";
	public static final String BUCKETENGINEID = "S3BUCKETENGINEID";
	private static final String STORAGE_PATH = "STORAGE_PATH";

	private String accessKey;
	private String secretKey;
	private String region;
	private String storageEngineId;
	private String bucketName;
	private String objectPath;
	private String jobName;
	private String storagePath;

	private TranscribeClient transcribeClient = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		// preset these - don't need user to define
		smssProp.putIfAbsent(IFunctionEngine.NAME_KEY,
				"AWS Transcribe Custom Embeddings - For Use With Vector Database Engines");
		smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY, "Execute Azure Document Intelligence");

		super.open(smssProp);

		this.accessKey = smssProp.getProperty(ACCESS_KEY);
		this.secretKey = smssProp.getProperty(SECRET_KEY);
		this.region = smssProp.getProperty(REGION);
		this.storageEngineId = smssProp.getProperty(BUCKETENGINEID);
		this.storagePath = smssProp.getProperty(STORAGE_PATH);

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

		if (this.storagePath == null || this.storagePath.isEmpty()) {
			throw new RuntimeException("Must pass in a Storage Path");
		}

		try {
			AwsBasicCredentials awsCreds = AwsBasicCredentials.create(this.accessKey, this.secretKey);
			Region awsRegion = Region.of(this.region);

			this.transcribeClient = TranscribeClient.builder()
					.credentialsProvider(StaticCredentialsProvider.create(awsCreds)).region(awsRegion).build();

			if (this.storagePath.startsWith("s3://")) {
				this.storagePath = this.storagePath.replace("s3://", "");
				int startIndex = this.storagePath.indexOf(DIR_SEPARATOR);
				int endIndex = this.storagePath.lastIndexOf(DIR_SEPARATOR);
				this.bucketName = this.storagePath.substring(0, startIndex);

				if (startIndex < endIndex && startIndex < this.storagePath.length()) {
					this.objectPath = this.storagePath.substring(startIndex + 1, endIndex);
				} else if (startIndex == endIndex && startIndex < this.storagePath.length()) {
					this.objectPath = this.storagePath.substring(startIndex + 1, this.storagePath.length());
				}
			} else {
				throw new IllegalArgumentException("Must provide the valid path.");
			}

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		throw new IllegalArgumentException(
				"This function engine is only intended to be executed for custom vector db embeddings");
	}

	@Override
	public boolean canProcessDocument(File fileToProcess) {
		int lastDotIndex = fileToProcess.getName().lastIndexOf('.');
		if (lastDotIndex > 0 && lastDotIndex < fileToProcess.length() - 1) {
			String extension = fileToProcess.getName().substring(lastDotIndex + 1);
			return extension.equalsIgnoreCase("mp3") || extension.equalsIgnoreCase("wav")
					|| extension.equalsIgnoreCase("flac") || extension.equalsIgnoreCase("ogg")
					|| extension.equalsIgnoreCase("amr") || extension.equalsIgnoreCase("webm")
					|| extension.equalsIgnoreCase("mp4") || extension.equalsIgnoreCase("webm")
					|| extension.equalsIgnoreCase("mov") || extension.equalsIgnoreCase("avi");
		}
		return false;
	}

	@Override
	public int processDocument(String outputCsvFilePath, File fileToProcess, Map<String, Object> parameters) {
		String audioFileName = null;
		Object output = null;
		String folderPath = null;
		String fileDir = null;
		VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(outputCsvFilePath);
		List<String> startAndEndTime = new ArrayList<>();
		List<String> extractedText = new ArrayList<>();
		Path tempFile = null;
		try {
			audioFileName = fileToProcess.getName();

			Insight insight = getInsight(parameters.get("INSIGHT"));
			String insightId = insight.getInsightId();
			Insight in = InsightStore.getInstance().get(insightId);
			File instanceDir = new File(Utility.normalizePath(in.getInsightFolder()));

			fileDir = instanceDir + DIR_SEPARATOR + audioFileName;

			folderPath = this.objectPath + DIR_SEPARATOR + audioFileName;
			IStorageEngine storageEng = Utility.getStorage(this.storageEngineId);
			Map<String, Object> metadata = new HashMap<>();
			metadata.put("utility", audioFileName + "- Transcribe Custom Enbeddings Functionality");
			storageEng.copyToStorage(fileDir, this.bucketName + DIR_SEPARATOR + this.objectPath + fileToProcess.getName(), metadata);
			output = transcriptionTextFromAudio(folderPath);
			storageEng.deleteFromStorage(this.bucketName + DIR_SEPARATOR + this.objectPath + fileToProcess.getName());

			if (output == TranscriptionJobStatus.COMPLETED) {
				String filePathInBucket = this.objectPath + DIR_SEPARATOR + this.jobName + JSON_EXT;
				tempFile = Files.createTempFile("file-temp-", JSON_EXT);
				storageEng.copyToLocal(tempFile.toString(), filePathInBucket);

				StringBuilder stringBuilder = new StringBuilder();
				try (BufferedReader reader = Files.newBufferedReader(tempFile)) {
					String line;
					while ((line = reader.readLine()) != null) {
						stringBuilder.append(line);
					}
					JSONObject jsonobj = new JSONObject(stringBuilder.toString());
					JSONObject result = jsonobj.getJSONObject("results");
					JSONArray audioSegments = result.getJSONArray("audio_segments");
					if (audioSegments != null) {
						for (int i = 0; i < audioSegments.length(); i++) {
							startAndEndTime.add(audioSegments.getJSONObject(i).getString("start_time") + " - "
									+ audioSegments.getJSONObject(i).getString("end_time"));
							extractedText.add(audioSegments.getJSONObject(i).getString("transcript"));
						}
						for (int i = 0; i < extractedText.size(); i++) {
							writer.writeRow(audioFileName, startAndEndTime.get(i), extractedText.get(i));
						}
					}
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
					writer.close();
					try {
						storageEng.deleteFromStorage(this.bucketName + DIR_SEPARATOR + this.objectPath + DIR_SEPARATOR + this.jobName);
					} catch (Exception e) {
						classLogger.error("Failed to delete file from the storage: ", e);
					}
				}
			}

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e);
		}

		return writer.getRowsInCsv();
	}

	public Object transcriptionTextFromAudio(String audioFilePath) throws Exception {
		Object transcriptionText = null;
		try {
			ZoneId zoneId = Utility.getApplicationZoneIdObj();
			ZonedDateTime now = ZonedDateTime.now(ZoneId.of(zoneId.getId()));
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
			String formattedTimestamp = now.format(formatter);
			this.jobName = "jobName_" + formattedTimestamp;

			String mediaFileUri = "https://s3-" + this.region + ".amazonaws.com/" + this.bucketName + "/"
					+ audioFilePath;

			StartTranscriptionJobRequest request = StartTranscriptionJobRequest.builder()
					.transcriptionJobName(this.jobName).languageCode("en-US")
					.media(media -> media.mediaFileUri(mediaFileUri)).outputBucketName(this.bucketName)
					.outputKey(this.objectPath + DIR_SEPARATOR).build();

			this.transcribeClient.startTranscriptionJob(request);

			// Poll for the job status
			while (true) {
				GetTranscriptionJobRequest getJobRequest = GetTranscriptionJobRequest.builder()
						.transcriptionJobName(this.jobName).build();

				GetTranscriptionJobResponse response = this.transcribeClient.getTranscriptionJob(getJobRequest);
				TranscriptionJobStatus status = response.transcriptionJob().transcriptionJobStatus();

				if (status == TranscriptionJobStatus.COMPLETED) {
					transcriptionText = status;
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

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.AWS_TRANSCRIBE_CUSTOM_EMBEDDINGS.name();
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

}
