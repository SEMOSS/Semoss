package prerna.engine.impl.function;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.HashSet;
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
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import prerna.engine.api.IStorageEngine;
import prerna.util.Constants;
import prerna.util.Utility;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.transcribe.TranscribeClient;
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobRequest;
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobResponse;
import software.amazon.awssdk.services.transcribe.model.StartTranscriptionJobRequest;
import software.amazon.awssdk.services.transcribe.model.TranscriptionJobStatus;

public class AWSTranscribeFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSTranscribeFunctionEngine.class);
	
	public static final String ACCESS_KEY = "ACCESS_KEY";
	public static final String SECRET_KEY = "SECRET_KEY";
	public static final String REGION = "REGION";
	//public static final String BUCKETNAME = "BUCKETNAME";
	public static final String BUCKETENGINEID = "S3BUCKETENGINEID";		
	public static final String OUTPUTFOLDER = "aws-service-repos";

	private String accessKey;
	private String secretKey;	
	private String region;
	private String bucketEngineId;		
	TranscribeClient transcribeClient = null;
	
	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.accessKey = smssProp.getProperty(ACCESS_KEY);
		this.secretKey = smssProp.getProperty(SECRET_KEY);
		this.region = smssProp.getProperty(REGION);
		this.bucketEngineId = smssProp.getProperty(BUCKETENGINEID);		

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
		if(this.bucketEngineId == null || this.bucketEngineId.isEmpty()) {
			throw new RuntimeException("Must pass in a S3BucketPath");		
		}
		try {
			AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKey, secretKey);
			this.transcribeClient = TranscribeClient.builder()
	                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
	                .region(software.amazon.awssdk.regions.Region.of(region))
	                .build();
			
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);	
		} 		
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {		
		String audioFileName = null;
		Object output = null;		
		String filePath = null;
		File file = null;
		String folderPath = null;
		String isFilePresentInS3 = null;		
		
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
			for(String key : parameterValues.keySet()) {
				if(key.equalsIgnoreCase("isFilePresentInS3")) {
					isFilePresentInS3 = parameterValues.get(key).toString();
				}else {
					filePath = parameterValues.get(key).toString();
					file = new File(parameterValues.get(key).toString());
					audioFileName = file.getName();
				}
			}	
			if(isFilePresentInS3.equalsIgnoreCase("true")){	
				int startIndex = filePath.indexOf('/');
				int endIndex = filePath.lastIndexOf('/');				
				String bucketName = filePath.substring(0, startIndex);
				
				if (startIndex < endIndex && startIndex < filePath.length()) {
					folderPath = filePath.substring(startIndex+1, endIndex);					
				}else if(startIndex==endIndex && startIndex < filePath.length()) {
					folderPath = filePath.substring(startIndex+1, filePath.length());
				}
				
				if(folderPath == null || folderPath.isEmpty() || folderPath.equals(audioFileName)) {					
		        	folderPath = audioFileName;
		        }else {		        	
		        	folderPath = folderPath + "/" + audioFileName;
		        }
				
				boolean identifyBucket = listObjects(bucketName, folderPath);
				
				if(identifyBucket) {
					output = transcriptionTextFromAudio(folderPath,bucketName);		
				}else {			        	
					output = "Must provide the valid path";
					throw new RuntimeException("Must provide the valid path");				
				}
			
			}else {
				/*BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
		        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
	                    .withRegion(this.region)
	                    .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
	                    .build();*/

	            // Upload the file to the bucket
		        //s3Client.putObject(new PutObjectRequest(this.S3BucketName, documentKeyName, file));		        
		        
		        folderPath = "transcribe/" + audioFileName;
		        				
				boolean identifyBucket = listObjects(OUTPUTFOLDER, folderPath);		        

				IStorageEngine storage = Utility.getStorage(this.bucketEngineId);
				Map<String, Object> map = new HashMap<>();
				map.put("functionalityUsed",audioFileName+"-Transcribe_functionality");	
				
				if(identifyBucket) {																	
					output = transcriptionTextFromAudio(folderPath,OUTPUTFOLDER);		
				} else {
					createFolderinS3(OUTPUTFOLDER);
					storage.syncLocalToStorage(folderPath,OUTPUTFOLDER, map);	
					//s3Client.putObject(new PutObjectRequest(OUTPUTFOLDER,folderPath ,file));
					output = transcriptionTextFromAudio(folderPath,OUTPUTFOLDER);
				}
			}							
		}catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}		
		return output;
	}
	
	public Object transcriptionTextFromAudio(String audioFilePath,String bucketName) {
		
		Object transcriptionText = null;
		String jobName = null;		
		try {
		
			LocalDateTime now = LocalDateTime.now();
		    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
		    String formattedTimestamp = now.format(formatter);
		    jobName= "jobName_" + formattedTimestamp;
			
			StartTranscriptionJobRequest request = StartTranscriptionJobRequest.builder()
		            .transcriptionJobName(jobName)
		            .languageCode("en-US") 
		            .media(media -> media.mediaFileUri("https://s3-"+this.region+".amazonaws.com/"+bucketName+"/"+ audioFilePath))
		            .outputBucketName(OUTPUTFOLDER) 
		            .outputKey("transcribe/")
		            .build();
		        
		        this.transcribeClient.startTranscriptionJob(request);
		        
		        // Poll for the job status
		            GetTranscriptionJobResponse response; 
		            while (true) {
		                response = this.transcribeClient.getTranscriptionJob(GetTranscriptionJobRequest.builder()
		                        .transcriptionJobName(jobName)
		                        .build());
		                
		                TranscriptionJobStatus status = response.transcriptionJob().transcriptionJobStatus();                
		 
		                if (status == TranscriptionJobStatus.COMPLETED) {
		                	transcriptionText = getTranscriptionTextFromS3(jobName, OUTPUTFOLDER);                  	
		                    break;
		                } else if (status == TranscriptionJobStatus.FAILED) {                    
		                    return "Transcription job failed.";
		            } 
		        }             
		}catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return transcriptionText;
	}
	
	public String getTranscriptionTextFromS3(String jobName, String bucketName) {
		
		 String transcriptionText =  null;
		 try {		 
			 BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
			 AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
					.withRegion(this.region)
					.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
					.build();
			 S3Object s3Object = s3Client.getObject(bucketName, "transcribe/"+jobName+".json");
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
			        e.printStackTrace();
			 }
		}catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		 return transcriptionText;
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
