package prerna.engine.impl.function;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.transcribe.AmazonTranscribe;
import com.amazonaws.services.transcribe.AmazonTranscribeClientBuilder;
import com.amazonaws.services.transcribe.model.GetTranscriptionJobRequest;
import com.amazonaws.services.transcribe.model.GetTranscriptionJobResult;
import com.amazonaws.services.transcribe.model.Media;
import com.amazonaws.services.transcribe.model.StartTranscriptionJobRequest;

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.IStorageEngine;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.util.Constants;
import prerna.util.Utility;

public class AWSTranscribeFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSTranscribeFunctionEngine.class);

	private static final String DIR_SEPARATOR = "/";

	public static final String OUTPUTBUCKET = "aws-service-repos";
	public static final String SUB_FOLDER = "transcribe/";
	public static final String JSON_EXT = ".json";

	public static final String ACCESS_KEY = "ACCESS_KEY";
	public static final String SECRET_KEY = "SECRET_KEY";
	public static final String REGION = "REGION";
	public static final String BUCKETENGINEID = "S3BUCKETENGINEID";
	public static final String VECTOR_DB_PROCESSOR = "VECTOR_DB_PROCESSOR";
	public static final String VECTOR_DB_PROCESSOR_ID = "VECTOR_DB_PROCESSOR_ID";
	public static final String OUTPUTFOLDER = "aws-service-repos";

	public static final String ACCESS_KEY_ERRMSG = "Must pass in an access key";
	public static final String SECRET_KEY_ERRMSG = "Must pass in a secret key";
	public static final String BUCKETENGINEID_ERRMSG = "Must pass in a S3BucketEngineId";
	public static final String REQUIREDPARM_ERRMSG = "Must define the requiredParameters";
	public static final String REGION_ERRMSG = "Must pass in a region";
	public static final String VECTOR_DB_ID_ERRMSG = "Must pass in a vector Db Id";
	public static final String VAILD_PATH_ERRMSG = "Must provide the valid path";
	public static final String INSIGHT_FILE_ERRMSG = "File is not in the Insight";

	private String accessKey;
	private String secretKey;
	private String region;
	private String bucketEngineId;
	private boolean vectorDbProcessor = false;
	private String vectorDbProcessorId;
	AmazonTranscribe transcribeClient = null;
	AmazonS3 s3Client = null;
	IVectorDatabaseEngine vectorDatabase = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.accessKey = smssProp.getProperty(ACCESS_KEY);
		this.secretKey = smssProp.getProperty(SECRET_KEY);
		this.region = smssProp.getProperty(REGION);
		this.bucketEngineId = smssProp.getProperty(BUCKETENGINEID);
		if (this.vectorDbProcessor) {
			this.vectorDbProcessorId = this.smssProp.getProperty(VECTOR_DB_PROCESSOR_ID);
			this.vectorDbProcessor = true;
			if (this.vectorDbProcessorId == null || (this.vectorDbProcessorId.isEmpty())) {
				throw new RuntimeException(VECTOR_DB_ID_ERRMSG);
			}
		}
		if (this.requiredParameters == null || (this.requiredParameters.isEmpty())) {
			throw new RuntimeException(REQUIREDPARM_ERRMSG);
		}
		if (this.accessKey == null || this.accessKey.isEmpty()) {
			throw new RuntimeException(ACCESS_KEY_ERRMSG);
		}
		if (this.secretKey == null || this.secretKey.isEmpty()) {
			throw new RuntimeException(SECRET_KEY_ERRMSG);
		}
		if (this.region == null || this.region.isEmpty()) {
			throw new RuntimeException("Must pass in a region");
		}
		if (this.bucketEngineId == null || this.bucketEngineId.isEmpty()) {
			throw new RuntimeException(BUCKETENGINEID_ERRMSG);
		}
		try {
			BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
			this.transcribeClient = AmazonTranscribeClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(region).build();
			this.s3Client = AmazonS3ClientBuilder.standard().withRegion(this.region)
					.withCredentials(
							new AWSStaticCredentialsProvider(new BasicAWSCredentials(this.accessKey, this.secretKey)))
					.build();
			this.vectorDatabase = Utility.getVectorDatabase(this.vectorDbProcessorId);
			if (this.vectorDatabase == null) {
				throw new SemossPixelException("Unable to find engine");
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		String audioFileName = null;
		Object output = null;
		Object convertedText = null;
		File filedir = null;
		File filePathDir = null;
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
			for (String key : parameterValues.keySet()) {
				filePathDir = new File(parameterValues.get(key).toString());
				audioFileName = filePathDir.getName();
			}

			Insight insight = getInsight(parameterValues.get("INSIGHT"));
			String insightId = insight.getInsightId();
			Insight in = InsightStore.getInstance().get(insightId);
			File instanceDir = new File(Utility.normalizePath(in.getInsightFolder()));

			File[] files = instanceDir.listFiles();
			if (files != null && files.length != 0) {
				for (File file : files) {
					if (filePathDir.getName().equalsIgnoreCase(file.getName())) {
						filedir = new File(instanceDir + DIR_SEPARATOR + filePathDir.getName());
					}
				}
			} else {
				throw new IllegalArgumentException(INSIGHT_FILE_ERRMSG);
			}			
			
			folderPath = SUB_FOLDER + audioFileName;
			IStorageEngine storage = Utility.getStorage(this.bucketEngineId);
			Map<String, Object> metadata = new HashMap<>();
			metadata.put("functionalityUsed", audioFileName + "-Transcribe_functionality");

			storage.copyToStorage(filedir.toString(), OUTPUTBUCKET + DIR_SEPARATOR + SUB_FOLDER, metadata);

			if (this.vectorDbProcessor) {

				convertedText = transcriptionTextFromAudio(folderPath, OUTPUTBUCKET);

				if (!instanceDir.exists()) {
					throw new IllegalArgumentException(INSIGHT_FILE_ERRMSG + instanceDir);
				}

				String textFileName = audioFileName.substring(0, audioFileName.lastIndexOf("."));
				textFileName = textFileName + ".txt";

				File txtFile = new File(instanceDir, textFileName);

				try (FileWriter writer = new FileWriter(txtFile)) {
					writer.write(convertedText.toString());
					writer.flush();
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw e;
				}

				if (!this.vectorDatabase.userCanAccessEmbeddingModels(insight.getUser())) {
					throw new IllegalArgumentException(
							"User does not have access to all the vector database dependent models");
				}

				File file1 = new File(instanceDir, textFileName);
				if (!file1.exists() && !file1.isFile()) {
					throw new IllegalArgumentException(
							"The file- " + instanceDir + "doesnot exists in the insight folder");
				}
				Map<String, Object> paramMap = new HashMap<String, Object>();
				paramMap.put(Constants.INSIGHT, insight);

				List<String> validFiles = new ArrayList<>();
				validFiles.add(file1.toString());

				output = this.vectorDatabase.addDocument(validFiles, paramMap);
			} else {
				output = transcriptionTextFromAudio(folderPath, OUTPUTBUCKET);
			}

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return output;
	}

	public Object transcriptionTextFromAudio(String audioFilePath, String bucketName) throws Exception {

		Object transcriptionText = null;
		String jobName = null;
		try {

			LocalDateTime now = LocalDateTime.now();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
			String formattedTimestamp = now.format(formatter);
			jobName = "jobName_" + formattedTimestamp;

			String mediaFileUri = "https://s3-" + this.region + ".amazonaws.com/" + bucketName + "/" + audioFilePath;

			StartTranscriptionJobRequest request = new StartTranscriptionJobRequest().withTranscriptionJobName(jobName)
					.withLanguageCode("en-US").withMedia(new Media().withMediaFileUri(mediaFileUri))
					.withOutputBucketName(OUTPUTBUCKET).withOutputKey(SUB_FOLDER);

			this.transcribeClient.startTranscriptionJob(request);

			// Poll for the job status
			while (true) {
				GetTranscriptionJobRequest getJobRequest = new GetTranscriptionJobRequest()
						.withTranscriptionJobName(jobName);

				GetTranscriptionJobResult response = this.transcribeClient.getTranscriptionJob(getJobRequest);
				String status = response.getTranscriptionJob().getTranscriptionJobStatus();

				if ("COMPLETED".equals(status)) {
					transcriptionText = getTranscriptionTextFromS3(jobName, OUTPUTBUCKET);
					break;
				} else if ("FAILED".equals(status)) {
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

	public String getTranscriptionTextFromS3(String jobName, String bucketName) throws Exception {

		String transcriptionText = null;
		try {
			S3Object s3Object = this.s3Client.getObject(bucketName, SUB_FOLDER + jobName + JSON_EXT);
			S3ObjectInputStream inputStream = s3Object.getObjectContent();

			StringBuilder stringBuilder = new StringBuilder();
			try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
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
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}
		return transcriptionText;
	}

	public static Insight getInsight(Object insightObj) {
		if (insightObj instanceof String) {
			return InsightStore.getInstance().get((String) insightObj);
		} else {
			return (Insight) insightObj;
		}
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.AWS_TEXTRACT.name();
	}

}