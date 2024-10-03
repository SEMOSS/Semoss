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
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.HeadBucketRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.textract.AmazonTextract;
import com.amazonaws.services.textract.AmazonTextractClientBuilder;
import com.amazonaws.services.textract.model.Block;
import com.amazonaws.services.textract.model.DocumentLocation;
import com.amazonaws.services.textract.model.GetDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.GetDocumentTextDetectionResult;
import com.amazonaws.services.textract.model.S3Object;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionResult;

import prerna.engine.api.IStorageEngine;
import prerna.util.Constants;
import prerna.util.Utility;

public class AWSTextractFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSTextractFunctionEngine.class);

	public static final String ACCESS_KEY = "ACCESS_KEY";
	public static final String SECRET_KEY = "SECRET_KEY";
	public static final String REGION = "REGION";
	//public static final String BUCKETNAME = "BUCKETNAME";
	public static final String BUCKETENGINEID = "S3BUCKETENGINEID";	
	public static final String OUTPUTFOLDER = "aws-service-repos";
	public static final String SUCCEEDED = "SUCCEEDED";
	public static final String PAGE = "PAGE";
	public static final String LINE = "LINE";
	
	private String accessKey;
	private String secretKey;	
	private String region;	
	private String bucketEngineId;		
	private AmazonTextract textractClient = null;

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
			throw new RuntimeException("Must pass in a S3BucketEngineId");		
		}
		
		try {
			BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
			this.textractClient = AmazonTextractClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
					.withRegion(this.region) 
					.build(); 
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);	
		} 
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		Object output = null;
		String documentKeyName = null;	
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
			for(String key : parameterValues.keySet()) {
				if(key.equalsIgnoreCase("isFilePresentInS3")) {
					isFilePresentInS3 = parameterValues.get(key).toString();
				}else {
					filePath = parameterValues.get(key).toString();
					file = new File(parameterValues.get(key).toString());
					documentKeyName = file.getName();
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
				
				if(folderPath == null || folderPath.isEmpty() || folderPath.equals(documentKeyName)) {					
		        	folderPath = documentKeyName;
		        }else {		        	
		        	folderPath = folderPath + "/" + documentKeyName;
		        }
				
				boolean identifyBucket = listObjects(bucketName, folderPath);
				if(identifyBucket) {
					output = textractFromDocument(folderPath,bucketName);		
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
		        
		        folderPath = "textract/" + documentKeyName;		       
				boolean identifyBucket = listObjects(OUTPUTFOLDER, folderPath);		        

				IStorageEngine storage = Utility.getStorage(this.bucketEngineId);
				Map<String, Object> map = new HashMap<>();
				map.put("functionalityUsed",documentKeyName+"-Textract_functionality");


				if(identifyBucket) {																	
					output = textractFromDocument(folderPath,OUTPUTFOLDER);		
				} else {
					createFolderinS3(OUTPUTFOLDER);
					storage.syncLocalToStorage(folderPath,OUTPUTFOLDER, map);	
					//s3Client.putObject(new PutObjectRequest(OUTPUTFOLDER,folderPath ,file));
					output = textractFromDocument(folderPath,OUTPUTFOLDER);
				}
			}
						
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);	
		}
		return output;
	}

	public List<String> textractFromDocument(String documentName, String S3BucketPath){
		List<String> extractedTextFromDoc = new ArrayList<String>();    	
		try { 
			// Create the StartDocumentTextDetection request
			StartDocumentTextDetectionRequest request = new StartDocumentTextDetectionRequest()
					.withDocumentLocation(new DocumentLocation()
							.withS3Object(new S3Object()
									.withBucket(S3BucketPath)
									.withName(documentName)));

			// Start text detection
			StartDocumentTextDetectionResult result = this.textractClient.startDocumentTextDetection(request);

			//results
			GetDocumentTextDetectionRequest getRequest = new GetDocumentTextDetectionRequest().withJobId(result.getJobId());
			GetDocumentTextDetectionResult getResult;
			String nextToken = null;            

			do {
				getRequest.setNextToken(nextToken);
				do {
					getResult = this.textractClient.getDocumentTextDetection(getRequest); 
					if(getResult.getJobStatus().equalsIgnoreCase("FAILED")) {
						extractedTextFromDoc.add("Must provide the valid file");
						break;
					}
				} while (!getResult.getJobStatus().equalsIgnoreCase(SUCCEEDED));
				nextToken = getResult.getNextToken();

				for (Block block : getResult.getBlocks()) {
					if (PAGE.equalsIgnoreCase(block.getBlockType())) {
						int pageNumber = block.getPage();
						StringBuilder pageText = new StringBuilder();
						for (Block item : getResult.getBlocks()) {
							if (item.getPage() == pageNumber && LINE.equalsIgnoreCase(item.getBlockType())) {
								pageText.append(item.getText());
							}
						}
						extractedTextFromDoc.add(pageText.toString());
					}
				}
			} while (nextToken != null);
				        
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);	
			e.printStackTrace();
		}	
		return extractedTextFromDoc;
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
