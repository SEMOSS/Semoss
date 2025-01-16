package prerna.engine.impl.function;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.documentai.v1.BatchDocumentsInputConfig;
import com.google.cloud.documentai.v1.BatchProcessMetadata;
import com.google.cloud.documentai.v1.BatchProcessRequest;
import com.google.cloud.documentai.v1.BatchProcessResponse;
import com.google.cloud.documentai.v1.Document;
import com.google.cloud.documentai.v1.DocumentOutputConfig;
import com.google.cloud.documentai.v1.DocumentProcessorServiceClient;
import com.google.cloud.documentai.v1.DocumentProcessorServiceSettings;
import com.google.cloud.documentai.v1.GcsDocument;
import com.google.cloud.documentai.v1.GcsDocuments;
import com.google.cloud.documentai.v1.DocumentOutputConfig.GcsOutputConfig;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.protobuf.util.JsonFormat;

import prerna.engine.api.IStorageEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.util.Constants;
import prerna.util.Utility;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GoogleOCRFunctionEngine extends AbstractFunctionEngine  {

	private static final Logger classLogger = LogManager.getLogger(GoogleOCRFunctionEngine.class);

	private static final String DIR_SEPARATOR = "/";

	public static final String BUCKETENGINEID = "GOOGLE_BUCKET_ENGINEID";	
	public static final String OUTPUTBUCKET = "gcp-service-repos";
	public static final String SMSS_KEY_SERVICE_ACCOUNT_CREDENTIALS = "SERVICE_ACCOUNT_CREDENTIALS";
	public static final String REGION = "REGION";
	public static final String PROJECT_ID = "PROJECT_ID";
	public static final String PROCESSOR_ID = "PROCESSOR_ID";
	public static final String SUB_FOLDER =  "ocr/";
	public static final String BUCKET_PREFIX =  "gs://";

	private String projectId;
	private String processorId;	
	private String region;	
	private String googleBucketEngineId;
	private String ServiceAccountFile;
	private Storage storage = null;
	private DocumentProcessorServiceClient client = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		
		this.projectId = smssProp.getProperty(PROJECT_ID);
		this.processorId = smssProp.getProperty(PROCESSOR_ID);
		this.region = smssProp.getProperty(REGION);
		this.googleBucketEngineId = smssProp.getProperty(BUCKETENGINEID);
		this.ServiceAccountFile = smssProp.getProperty(SMSS_KEY_SERVICE_ACCOUNT_CREDENTIALS);
		
		final String SCOPE_KEY = "https://www.googleapis.com/auth/cloud-platform";
		final String SCOPE_VALUE = "https://www.googleapis.com/auth/cloud-platform.read-only";
		final String ENDPOINT_FORMAT = "%s-documentai.googleapis.com:443";
		
		final String PROJECTID_ERRMSG = "Must pass in a project Id";
		final String PROCESSORID_ERRMSG = "Must pass in a processor Id";
		final String REGION_ERRMSG = "Must pass in a region";
		final String GOOGLEBUCKETENGINEID_ERRMSG = "Must pass in a google bucket EngineId";
		final String SERVICEACCFILE_ERRMSG = "Must pass in a Service Account File";
			
		if(this.projectId == null || this.projectId.isEmpty()){
			throw new RuntimeException(PROJECTID_ERRMSG); 
		}		
		if(this.processorId == null || this.processorId.isEmpty()){
			throw new RuntimeException(PROCESSORID_ERRMSG);
		}	
		if(this.region == null || this.region.isEmpty()){
			throw new RuntimeException(REGION_ERRMSG);
		}
		
		if(this.googleBucketEngineId == null || this.googleBucketEngineId.isEmpty()) {
			throw new RuntimeException(GOOGLEBUCKETENGINEID_ERRMSG);		
		}

		if(this.ServiceAccountFile == null || this.ServiceAccountFile.isEmpty()) {
			throw new RuntimeException(SERVICEACCFILE_ERRMSG);		
		}

		try {			 
			 GoogleCredentials credentials = GoogleCredentials.fromStream( new ByteArrayInputStream(this.ServiceAccountFile.getBytes()))
					  .createScoped(SCOPE_KEY, SCOPE_VALUE); 
			    
			    FixedCredentialsProvider credentialsProvider = FixedCredentialsProvider.create(credentials);
			    String endpoint = String.format(ENDPOINT_FORMAT, this.region); 
			    
			    this.client = DocumentProcessorServiceClient.create(DocumentProcessorServiceSettings.newBuilder()
		        		.setCredentialsProvider(credentialsProvider)
		        		.setEndpoint(endpoint)
		        		.build());
			    this.storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
			    
		}catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new RuntimeException(e);
		}
		
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		
		Object output = null;			
		String filePath = null;
		Boolean isFilePresentInBucket = false; 
		String folderPath = null;
		File filedir = null;
		File filePathDir = null;
		boolean identifyBucket = false;
		String bucketName = null;
		final String INSIGHT = "insight";
		
		final String VAILD_PATH_ERRMSG = "Must provide the valid path";
		final String MISSINGPS_ERRMSG = "Must define required keys = ";
		final String INSIGHT_FILE_ERRMSG = "File is not in the Insight";
		final String BUCKET_FILE_ERRMSG = "File already present in the bucket";

		Insight insight = getInsight(parameterValues.get(INSIGHT));
		
		if(this.requiredParameters != null && !this.requiredParameters.isEmpty()) {
			Set<String> missingPs = new HashSet<>();
			for(String requiredP : this.requiredParameters) {
				if(!parameterValues.containsKey(requiredP)) {
					missingPs.add(requiredP);
				}
			}
			if(!missingPs.isEmpty()) {
				throw new IllegalArgumentException(MISSINGPS_ERRMSG + missingPs); 
			}
		}
		try {
			for(String k : parameterValues.keySet()) {
				if(k.equalsIgnoreCase("isFilePresentInBucket")) {
					isFilePresentInBucket = Boolean.parseBoolean(parameterValues.get(k).toString());
				}else {
					filePath = parameterValues.get(k).toString();									
				}
			}
			if(isFilePresentInBucket){				
				
				Map<String,String> extractedPath = gcsPathExtractor(filePath);
	            bucketName = extractedPath.get("bucketName");
	            folderPath = extractedPath.get("folderPath");

	            identifyBucket = listObjects(bucketName, folderPath,null); 
	            
	            if(identifyBucket) {
					output = ocrFiles(filePath);		
				}else {		
					classLogger.error(Constants.STACKTRACE, VAILD_PATH_ERRMSG);
					throw new RuntimeException(VAILD_PATH_ERRMSG); 
				}
				
			}else {
				String insightId = insight.getInsightId(); 
				Insight in = InsightStore.getInstance().get(insightId);
				File instanceDir = new File( Utility.normalizePath(in.getInsightFolder()));
				try {
					File[] files = instanceDir.listFiles();
					filePathDir = new File(filePath);
					if (files != null && files.length != 0) {
						for (File file : files) {
							if(filePathDir.getName().equalsIgnoreCase(file.getName())) {
								filedir = new File(instanceDir +DIR_SEPARATOR+filePathDir.getName());
							}                   	                         
	                    }                   
		            } else {
		            	classLogger.error(Constants.STACKTRACE, INSIGHT_FILE_ERRMSG);
		            	throw new IllegalArgumentException(INSIGHT_FILE_ERRMSG); 
		            	
		            }
				}catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new IllegalArgumentException(e);
				}				

				folderPath = SUB_FOLDER + filedir.getName();
				identifyBucket = listObjects(OUTPUTBUCKET, folderPath,filedir.toString());		        

				IStorageEngine storageeng = Utility.getStorage(this.googleBucketEngineId);
				Map<String, Object> metadata = new HashMap<>(); 
				metadata.put("utility",filePathDir.getName()+"- GoogleOCR_functionality"); 

				if(identifyBucket) {																	
					classLogger.error(Constants.STACKTRACE, BUCKET_FILE_ERRMSG);
					throw new RuntimeException(BUCKET_FILE_ERRMSG);
				} else {
					createBucket(OUTPUTBUCKET);
					storageeng.copyToStorage(filedir.toString(),OUTPUTBUCKET+folderPath, metadata);					
					output = ocrFiles(BUCKET_PREFIX+OUTPUTBUCKET+DIR_SEPARATOR+folderPath);	
				}

			}
		}catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e);
		}	
		
		return output;
	}
	
	public List<String> ocrFiles(String filePath) throws InterruptedException, ExecutionException{
		List<String> extractedTextFromDoc = new ArrayList<String>();
		final String OUTPUT = "output";
		final String PROCESSORNAME_FORMAT = "projects/%s/locations/%s/processors/%s";
		final String JSON_EXT = ".json";
		final String WAITING_INFO = "Waiting for operation to complete...";
		final String END_INFO = "Document processing complete.";
		try {
			classLogger.info(WAITING_INFO);
			String processorName =
			          String.format(PROCESSORNAME_FORMAT, this.projectId, this.region, this.processorId);
			
			File inputFile = new File(filePath);
			
			String mimeType = URLConnection.guessContentTypeFromName(inputFile.getName());
			
			if (mimeType == null) {
			    try (FileInputStream fis = new FileInputStream(inputFile)) {
			        mimeType = URLConnection.guessContentTypeFromStream(fis);
			    } catch (IOException e) {
			        e.printStackTrace();			      
			    }
			}
			
			GcsDocument gcsDocument =
	            GcsDocument.newBuilder().setGcsUri(filePath).setMimeType(mimeType).build();

	        GcsDocuments gcsDocuments = GcsDocuments.newBuilder().addDocuments(gcsDocument).build();
	        
	        BatchDocumentsInputConfig inputConfig =
	                BatchDocumentsInputConfig.newBuilder().setGcsDocuments(gcsDocuments).build();
	        
	        LocalDateTime now = LocalDateTime.now();
		    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
		    String formattedTimestamp = now.format(formatter);
		    
	        String outputFileName = inputFile.getName().concat("_" + formattedTimestamp + JSON_EXT); 

	        String fullGcsPath = String.format("gs://%s/%s/%s", OUTPUTBUCKET, SUB_FOLDER+OUTPUT,outputFileName);
	        GcsOutputConfig gcsOutputConfig = GcsOutputConfig.newBuilder().setGcsUri(fullGcsPath).build();

	        DocumentOutputConfig documentOutputConfig =
	            DocumentOutputConfig.newBuilder().setGcsOutputConfig(gcsOutputConfig).build();

	        // Configure the batch process request.
	        BatchProcessRequest request =
	            BatchProcessRequest.newBuilder()
	                .setName(processorName)
	                .setInputDocuments(inputConfig)
	                .setDocumentOutputConfig(documentOutputConfig)
	                .build();
	        
	        OperationFuture<BatchProcessResponse, BatchProcessMetadata> future =
	                client.batchProcessDocumentsAsync(request);            
	        
	        future.get();

	        classLogger.info(END_INFO);
	        
	        Bucket bucket = this.storage.get(OUTPUTBUCKET);

	        Page<Blob> blobs = bucket.list(Storage.BlobListOption.prefix(SUB_FOLDER+OUTPUT + DIR_SEPARATOR + outputFileName));
	        for (Blob blob : blobs.iterateAll()) {
	            if (!blob.isDirectory()) {
	              File tempFile = File.createTempFile("file", JSON_EXT);
	              try {
	            	  Blob fileInfo = storage.get(BlobId.of(OUTPUTBUCKET, blob.getName()));
	                  fileInfo.downloadTo(tempFile.toPath());
	                  try(FileReader reader = new FileReader(tempFile)) {
	                	  Document.Builder builder = Document.newBuilder();
	                      JsonFormat.parser().merge(reader, builder);
	                      Document document = builder.build();
	                      for (int pageIndex = 0; pageIndex < document.getPagesCount(); pageIndex++) {
	                    	  Document.Page page = document.getPages(pageIndex); // Get the current page 
	                    	  String pageText = getText(page.getLayout().getTextAnchor(), document.getText());        	             
	        	              extractedTextFromDoc.add(pageText);  // Store page-wise text
	        	          }
	                  }finally {
	                	  if (tempFile.exists()) {
	                          tempFile.delete();	                          
	                      }
	                  }
	              }catch (IOException e) {
					e.printStackTrace();
				}
	          } 
	        }   
			
		}catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);	
			e.printStackTrace();
		}		
		return extractedTextFromDoc;
	}
	
	private static String getText(Document.TextAnchor textAnchor, String text) {
	    if (textAnchor.getTextSegmentsList().size() > 0) {
	      int startIdx = (int) textAnchor.getTextSegments(0).getStartIndex();
	      int endIdx = (int) textAnchor.getTextSegments(0).getEndIndex();
	      return text.substring(startIdx, endIdx);
	    }
	    return " ";
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
	
	private Map<String,String> gcsPathExtractor(String filePath){
		Map<String,String> extractedPath = new HashMap<>(); 
		try {
			Pattern bucketPattern = Pattern.compile("^gs://([^/]+)");
	        Matcher bucketMatcher = bucketPattern.matcher(filePath);
	        String bucketName = "";
	        if (bucketMatcher.find()) {
	        	bucketName = bucketMatcher.group(1);
	        }
	        
	        Pattern folderPattern = Pattern.compile("gs://[^/]+/(.*)");
	        Matcher folderMatcher = folderPattern.matcher(filePath);
	        String folderPath = "";
	        if (folderMatcher.find()) {
	        	folderPath = folderMatcher.group(1);
	        }
	        extractedPath.put("bucketName",bucketName);
	        extractedPath.put("folderPath",folderPath);
	        		
		}catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e);
		}
        
		return extractedPath;		
	}
	
	@Override
	public void close() throws IOException {
		//not used..
	}

}
