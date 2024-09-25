package prerna.engine.impl.function;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.DetectTextRequest;
import com.amazonaws.services.rekognition.model.DetectTextResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;

import prerna.engine.api.IStorageEngine;
import prerna.util.Constants;
import prerna.util.Utility;


public class AWSRekognitionFunctionEngine extends AbstractFunctionEngine{

	private static final Logger classLogger = LogManager.getLogger(AWSRekognitionFunctionEngine.class);

	public static final String ACCESS_KEY = "ACCESS_KEY";
	public static final String SECRET_KEY = "SECRET_KEY";
	public static final String REGION = "REGION";
	//public static final String BUCKETNAME = "BUCKETNAME";
	public static final String BUCKETENGINEID = "S3BUCKETENGINEID";	
	public static final String OUTPUTFOLDER = "aws-service-repos";
	public static final String LINE = "LINE";

	private String accessKey;
	private String secretKey;	
	private String region;
	private String bucketEngineId;
	private AmazonRekognition rekognitionClient = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.accessKey = smssProp.getProperty(ACCESS_KEY);
		this.secretKey = smssProp.getProperty(SECRET_KEY);
		this.region = smssProp.getProperty(REGION);
		this.bucketEngineId = smssProp.getProperty(BUCKETENGINEID);

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
			BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
			this.rekognitionClient = AmazonRekognitionClientBuilder.standard()
	        		.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
	        		.withRegion(region) 
	                .build();       

		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);	
		} 
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		Object output = null;
		String imageKeyName = null;		
		File file = null;
		String filePath = null;
		String isFilePresentInS3 = null;
		String folderPath = null;	

		// validate all the required keys are set
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
				if(k.equalsIgnoreCase("isFilePresentInS3")) {
					isFilePresentInS3 = parameterValues.get(k).toString();
				}else {
					filePath = parameterValues.get(k).toString();
					file = new File(parameterValues.get(k).toString());
					imageKeyName = file.getName();
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
				
				if(folderPath == null || folderPath.isEmpty() || folderPath.equals(imageKeyName)) {					
		        	folderPath = imageKeyName;
		        }else {		        	
		        	folderPath = folderPath + "/" + imageKeyName;
		        }
				
				boolean identifyBucket = listObjects(bucketName, folderPath);
				if(identifyBucket) {
					output = rekognitionFromImage(folderPath,bucketName);		
				}else {			        	
					output = "Must provide the valid path";
					throw new RuntimeException("Must provide the valid path");
				}
			}else {						
		       /* BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
		        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
	                    .withRegion(this.region)
	                    .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
	                    .build();*/

	            // Upload the file to the bucket
		        //s3Client.putObject(new PutObjectRequest(this.S3BucketName, documentKeyName, file));		        
		        
		        folderPath = "rekognition/" + imageKeyName;
				
				boolean identifyBucket = listObjects(OUTPUTFOLDER, folderPath);		        

				IStorageEngine storage = Utility.getStorage(this.bucketEngineId);
				Map<String, Object> map = new HashMap<>();
				map.put("functionalityUsed",imageKeyName+"-Rekognition_functionality");


				if(identifyBucket) {																	
					output = rekognitionFromImage(folderPath,OUTPUTFOLDER);		
				} else {
					createFolderinS3(OUTPUTFOLDER);
					storage.syncLocalToStorage(folderPath,OUTPUTFOLDER, map);	
					//s3Client.putObject(new PutObjectRequest(OUTPUTFOLDER,folderPath ,file));
					output = rekognitionFromImage(folderPath,OUTPUTFOLDER);
				}
			}
						
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);	
		}
		return output;
	}
	
	public Object rekognitionFromImage(String imageName, String S3BucketPath){
		Map<String, Object> map = new HashMap<>();
		StringBuffer detectedText = new StringBuffer();
		List<String> detectedLabels = new ArrayList<String>();
		try {
			DetectTextRequest detectTextRequest = new DetectTextRequest()
	                .withImage(new Image()
	                        .withS3Object(new S3Object()
	                                .withBucket(S3BucketPath)
	                                .withName(imageName)
	                        )
	                );
	
	        DetectTextResult response = this.rekognitionClient.detectText(detectTextRequest);
	
	        // Process the result
	        response.getTextDetections().forEach(detection -> {
	        	if(detection.getType().equalsIgnoreCase(LINE)) {        		
	        		detectedText.append(detection.getDetectedText());        		  
	        	}        
	        });			
	
	        DetectLabelsRequest detectLabesrequest = new DetectLabelsRequest()
		            .withImage(new Image()
		                    .withS3Object(new S3Object()
		                            .withBucket(S3BucketPath)
		                            .withName(imageName)))
		            .withMaxLabels(10)
		            .withMinConfidence(75F);
	
		    DetectLabelsResult labels = this.rekognitionClient.detectLabels(detectLabesrequest);
	
		    for (Label label : labels.getLabels()) {
		    	detectedLabels.add(label.getName());	        
		    }
	
		    map.put("detected_text", detectedText);
			map.put("detected_labels", detectedLabels);
		}catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return map;
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