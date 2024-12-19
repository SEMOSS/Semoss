package prerna.engine.impl.function;

import com.google.cloud.speech.v1.LongRunningRecognizeMetadata;
import com.google.cloud.speech.v1.LongRunningRecognizeResponse;
import com.google.cloud.speech.v1.RecognitionAudio;
import com.google.cloud.speech.v1.RecognitionConfig;
import com.google.cloud.speech.v1.SpeechClient;
import com.google.cloud.speech.v1.SpeechRecognitionResult;
import com.google.cloud.speech.v1.SpeechSettings;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IStorageEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.util.Constants;
import prerna.util.Utility;

public class GoogleSpeechtoTextFunctionEngine extends AbstractFunctionEngine  {

	private static final Logger classLogger = LogManager.getLogger(GoogleSpeechtoTextFunctionEngine.class);
	
	private static final String FILE_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	public static final String BUCKETENGINEID = "GOOGLE_BUCKET_ENGINEID";	
	public static final String OUTPUTBUCKET = "gcp-service-repos";
	public static final String SMSS_KEY_SERVICE_ACCOUNT_CREDENTIALS = "SERVICE_ACCOUNT_CREDENTIALS";
	public static final String SUB_FOLDER =  "speech-to-text/";
	public static final String BUCKET_PREFIX =  "gs://";
	
	private String googleBucketEngineId;
	private String ServiceAccountFile;
	private Storage storage = null;
	private SpeechClient speechClient = null;
	
	@Override
	public void open(Properties smssProp) throws Exception {
				
		this.googleBucketEngineId = smssProp.getProperty(BUCKETENGINEID);
		
		this.ServiceAccountFile = smssProp.getProperty(SMSS_KEY_SERVICE_ACCOUNT_CREDENTIALS);
		
		if(this.googleBucketEngineId == null || this.googleBucketEngineId.isEmpty()) {
			throw new RuntimeException("Must pass in a google bucket EngineId");		
		}
		
		if(this.ServiceAccountFile == null || this.ServiceAccountFile.isEmpty()) {
			throw new RuntimeException("Must pass in a Service Account File");		
		}
		
		try {
			GoogleCredentials credentials = GoogleCredentials.fromStream( new ByteArrayInputStream(this.ServiceAccountFile.getBytes()));
			
			CredentialsProvider credentialsProvider = () -> credentials;
			this.storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
			this.speechClient = SpeechClient.create(SpeechSettings.newBuilder().
					  setCredentialsProvider(credentialsProvider).build());
		}catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		Object output = null;
		String audioKeyName = null;			
		String filePath = null;
		String isFilePresentInBucket = null;
		String folderPath = null;
		File filedir = null;
		
		Insight insight = getInsight(parameterValues.get("insight"));
		
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
				if(k.equalsIgnoreCase("isFilePresentInBucket")) {
					isFilePresentInBucket = parameterValues.get(k).toString();
				}else {
					filePath = parameterValues.get(k).toString();									
				}
			}
			
			if(isFilePresentInBucket.equalsIgnoreCase("true")){
				String bucketName = null;
	            
	            if (filePath.startsWith(BUCKET_PREFIX)) {
	            	bucketName = filePath.substring(5);  
	            }
	            int endIndex = bucketName.indexOf('/');
	            bucketName = bucketName.substring(0, endIndex);
	            URI uri = new URI(filePath);
	            folderPath = uri.getPath().substring(1);
	            
	            boolean identifyBucket = listObjects(bucketName, folderPath,null);
	            
	            if(identifyBucket) {
					output = extractFromAudio(filePath);		
				}else {			        	
					output = "Must provide the valid path";
					throw new RuntimeException(output.toString());
				}
			}else {
				
				String insightId = insight.getInsightId();
				Insight in = InsightStore.getInstance().get(insightId);
				File instanceDir = new File( Utility.normalizePath(in.getInsightFolder()));
				
				File[] files = instanceDir.listFiles();
				
				if (files != null && files.length != 0) {
					for (File file : files) {
						if(filePath.equalsIgnoreCase(file.getName())) {
							filedir = new File(instanceDir +FILE_SEPARATOR+filePath);
						}                   	                         
                    }                   
	            } else {
	            	classLogger.error("File is not in the Insight");
	            }
				
				folderPath = SUB_FOLDER + filedir.getName();
				
				boolean identifyBucket = listObjects(OUTPUTBUCKET, folderPath,filedir.toString());		        

				IStorageEngine storageeng = Utility.getStorage(this.googleBucketEngineId);
				Map<String, Object> map = new HashMap<>();
				map.put("functionalityUsed",audioKeyName+"- GoogleSpeechToText_functionality");


				if(identifyBucket) {																	
					output = "File alread present in the bucket";
					throw new RuntimeException(output.toString());
				} else {
					createBucket(OUTPUTBUCKET);
					storageeng.copyToStorage(filedir.toString(),OUTPUTBUCKET+folderPath, map);						
					output = extractFromAudio(BUCKET_PREFIX+OUTPUTBUCKET+FILE_SEPARATOR+folderPath);	
				}
			}
			
		}catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to transcribe -  " +e);
		}
		return output;
	}
	
	private StringBuilder extractFromAudio(String audioUri) {
		StringBuilder extractedText = new StringBuilder();
		
		try {			
			
	        RecognitionConfig config = RecognitionConfig.newBuilder() 
	               .setSampleRateHertz(16000) 
	               .setLanguageCode("en-US") 
                    .build();	       
	 	      
	           RecognitionAudio audio = RecognitionAudio.newBuilder()
	           		.setUri(audioUri).build();	 
	           
	           OperationFuture<LongRunningRecognizeResponse, LongRunningRecognizeMetadata> future =
	                   this.speechClient.longRunningRecognizeAsync(config, audio);
	           
	           classLogger.info("Waiting for operation to complete...");
	      
	           LongRunningRecognizeResponse response = future.get();
	 
	           // Process the results
	       for (SpeechRecognitionResult result : response.getResultsList()) {
	    	   extractedText.append(result.getAlternatives(0).getTranscript());	           
	       }			  
	      
		}catch (Exception e) {
		  classLogger.error(Constants.STACKTRACE, e);
		  throw new IllegalArgumentException(e);
		}
		return extractedText; 
	}
	
	private Boolean listObjects(String bucketName, String folderPath, String localFilePath){
		  boolean result = false;
		  try {			  
			  Bucket bucket = this.storage.get(bucketName);
			  if(bucket != null) {
				  Iterable<Blob> blobs = bucket.list(Storage.BlobListOption.prefix(folderPath)).iterateAll();
	              for (Blob blob : blobs) {
	            	  if(localFilePath == ""|| localFilePath == null) {
	            		  if (blob.getName().startsWith(folderPath)&& blob.getSize()>0) {
		                    	result = true;
		                    	break;
		                    } 
	            	  }else {
	            		  if(Files.exists(Paths.get(localFilePath))) {
	            			  long localFileSize = Files.size(Paths.get(localFilePath));
	            			  if(blob.getName().startsWith(folderPath)&& blob.getSize().equals(localFileSize)) {
	            				  result = true;
	            				  break;
	            			  }
	            		  }
	            	  }
	                   
	              }
			  }		  
		  }catch (Exception e) {
			  classLogger.error(Constants.STACKTRACE, e);
			  throw new IllegalArgumentException(e);
	      }		 
		  return result;
	  }
	  
	  private void createBucket(String bucketName) { 
		  try {			  
			  Bucket bucket = this.storage.get(bucketName);
			  if(bucket == null) {
				  bucket = this.storage.create(Bucket.newBuilder(bucketName).build());
			  }	
		  }catch (Exception e) {
			  classLogger.error(Constants.STACKTRACE, e);
			  throw new IllegalArgumentException(e);
	      }			  	  
	  }
	  
	  private Insight getInsight(Object insightObj) {
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
}
