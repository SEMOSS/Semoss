package prerna.engine.impl.function;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.polly.AmazonPolly;
import com.amazonaws.services.polly.AmazonPollyClientBuilder;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskRequest;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskResult;
import com.amazonaws.services.polly.model.OutputFormat;
import com.amazonaws.services.polly.model.StartSpeechSynthesisTaskRequest;
import com.amazonaws.services.polly.model.StartSpeechSynthesisTaskResult;
import com.amazonaws.services.polly.model.VoiceId;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import prerna.util.Constants;


public class AWSPollyFunctionEngine extends AbstractFunctionEngine {
	
	private static final Logger classLogger = LogManager.getLogger(AWSPollyFunctionEngine.class);

	public static final String ACCESS_KEY = "ACCESS_KEY";
	public static final String SECRET_KEY = "SECRET_KEY";
	public static final String REGION = "REGION";	
	public static final String OUTPUTFOLDER = "aws-service-repos";

	private String accessKey;
	private String secretKey;	
	private String region;	
	private  AmazonPolly pollyClient = null;
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.accessKey = smssProp.getProperty(ACCESS_KEY);
		this.secretKey = smssProp.getProperty(SECRET_KEY);
		this.region = smssProp.getProperty(REGION);		

		if(this.requiredParameters == null || (this.requiredParameters.isEmpty())) {
			throw new RuntimeException("Must define the requiredParameters");
		}
		if(this.accessKey == null || this.accessKey.isEmpty()){
			throw new RuntimeException("Must pass in an access key");
		}		
		if(this.secretKey == null || this.secretKey.isEmpty()){
			throw new RuntimeException("Must pass in a secret key");
		}	
		if(this.region == null || this.region.isEmpty()){
			throw new RuntimeException("Must pass in a region");
		}
			
		try {
			BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
			this.pollyClient = AmazonPollyClientBuilder.standard()
	                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
	                .withRegion(region) // Replace with your preferred region
	                .build();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);	
		} 
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {		
		String extractedText = null;
		String audioFilePath = null;
		String nameOfTheAudioFile =null;
		Object Output = null;
		if(this.requiredParameters != null && !this.requiredParameters.isEmpty()) {
			Set<String> missingPs = new HashSet<>();
			for(String requiredP : this.requiredParameters) {
				if(!parameterValues.containsKey(requiredP)) {
					missingPs.add(requiredP);
				}
			}
			if(!missingPs.isEmpty()) {
				throw new IllegalArgumentException("Must define required keys = " + missingPs);
			}
		}
		try {
			for(String k : parameterValues.keySet()) {
				if(k.equalsIgnoreCase("nameOfTheAudioFile")) {
					nameOfTheAudioFile = parameterValues.get(k).toString();					   
				} 
				if(k.equalsIgnoreCase("extractedText")){
					extractedText = parameterValues.get(k).toString();
				}				
			}
			
			audioFilePath = "polly/" + nameOfTheAudioFile;
				
			boolean identifyBucket = listObjects(OUTPUTFOLDER, audioFilePath);	
			
			if(!identifyBucket) {				
				createFolderinS3(OUTPUTFOLDER);			
			} 	
			
			StartSpeechSynthesisTaskRequest request = new StartSpeechSynthesisTaskRequest()
	                .withOutputFormat(OutputFormat.Mp3)
	                .withVoiceId(VoiceId.Joanna) 
	                .withText(extractedText)
	                .withOutputS3BucketName(OUTPUTFOLDER)
	                .withOutputS3KeyPrefix(audioFilePath+"_");
			
			// Start speech synthesis task
            StartSpeechSynthesisTaskResult result = this.pollyClient.startSpeechSynthesisTask(request);           
            String taskId = result.getSynthesisTask().getTaskId();

            boolean isTaskCompleted = false;
            while (!isTaskCompleted) {
                // Get the speech synthesis task status
                GetSpeechSynthesisTaskRequest getRequest = new GetSpeechSynthesisTaskRequest().withTaskId(taskId);
                GetSpeechSynthesisTaskResult getResult = this.pollyClient.getSpeechSynthesisTask(getRequest);
 
                // Check the task status
                String taskStatus = getResult.getSynthesisTask().getTaskStatus();
                if ("completed".equals(taskStatus)) {
                	Output = "Task completed successfully.";
                    isTaskCompleted = true;
                } else if ("failed".equals(taskStatus)) {
                	Output = "Task failed.";
                    isTaskCompleted = true;
                } else {                    
                    isTaskCompleted = false;
                }
            }
			
		}catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return Output;
	}
	
	public Boolean listObjects(String bucketName, String folderPath){
		BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
				.withRegion(this.region)
				.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
				.build();
		boolean result = false;
		boolean bucketExists = doesBucketExist(s3Client, bucketName);
		if (bucketExists) {
			try {
				// Check if the object exists
				s3Client.getObjectMetadata(new GetObjectMetadataRequest(bucketName, folderPath));				
				result = true;
			} catch (com.amazonaws.services.s3.model.AmazonS3Exception e) {
				if (e.getStatusCode() == 404) {
					result = false;
				} else {
					e.printStackTrace();
				}
			}
		} else {
			result = false;
		}

		return result;
	}

	private static boolean doesBucketExist(AmazonS3 s3Client, String bucketName) {
		try {
			s3Client.headBucket(new HeadBucketRequest(bucketName));
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private void createFolderinS3(String bucketName) {       
		BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
				.withRegion(this.region)
				.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
				.build();		
		boolean bucketExists = doesBucketExist(s3Client, bucketName);
		if(!bucketExists) {		
			s3Client.createBucket(bucketName);			
		}
	}
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
