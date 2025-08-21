package prerna.engine.impl.function;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.textract.AmazonTextract;
import com.amazonaws.services.textract.AmazonTextractClientBuilder;
import com.amazonaws.services.textract.model.Block;
import com.amazonaws.services.textract.model.DocumentLocation;
import com.amazonaws.services.textract.model.GetDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.GetDocumentTextDetectionResult;
import com.amazonaws.services.textract.model.S3Object;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionRequest;
import com.amazonaws.services.textract.model.StartDocumentTextDetectionResult;

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.api.ICustomEmbeddingsFunctionEngine;
import prerna.engine.api.IFunctionEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.reactor.export.pdf.PDFUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class AWSTextractFunctionEngine extends AbstractFunctionEngine implements ICustomEmbeddingsFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSTextractFunctionEngine.class);
	
	private static final String DIR_SEPARATOR = "/";

	public static final String OUTPUTBUCKET = "aws-service-repos";
	public static final String SUB_FOLDER = "textract/";
	
	public static final String ACCESS_KEY = "ACCESS_KEY";
	public static final String SECRET_KEY = "SECRET_KEY";
	public static final String REGION = "REGION";	
	public static final String BUCKETENGINEID = "S3BUCKETENGINEID";
	public static final String SUCCEEDED = "SUCCEEDED";
	public static final String PAGE = "PAGE";
	public static final String LINE = "LINE";
	
	public static final String ACCESS_KEY_ERRMSG = "Must pass in an access key";
	public static final String SECRET_KEY_ERRMSG = "Must pass in a secret key";
	public static final String BUCKETENGINEID_ERRMSG = "Must pass in a S3BucketEngineId";
	public static final String REQUIREDPARM_ERRMSG = "Must define the requiredParameters";	
	public static final String REGION_ERRMSG = "Must pass in a region";	
	public static final String VAILD_PATH_ERRMSG = "Must provide the valid path";

	private String accessKey;
	private String secretKey;
	private String region;
	private String bucketEngineId;

	private AmazonTextract textractClient = null;
	AmazonS3 s3Client = null;

	@Override
	public void open(Properties smssProp) throws Exception {		
		
		// preset these - don't need user to define
		smssProp.putIfAbsent(IFunctionEngine.NAME_KEY, "AWS Textract - For Use With Vector Database Engines");
		smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY, "Execute AWS Textract");
		
		super.open(smssProp);
		
		this.accessKey = smssProp.getProperty(ACCESS_KEY);
		this.secretKey = smssProp.getProperty(SECRET_KEY);
		this.region = smssProp.getProperty(REGION);
		this.bucketEngineId = smssProp.getProperty(BUCKETENGINEID);

		if (this.accessKey == null || this.accessKey.isEmpty()) {
			throw new RuntimeException(ACCESS_KEY_ERRMSG);
		}
		if (this.secretKey == null || this.secretKey.isEmpty()) {
			throw new RuntimeException(SECRET_KEY_ERRMSG);
		}
		if (this.region == null || this.region.isEmpty()) {
			throw new RuntimeException(REGION_ERRMSG);
		}
		if (this.bucketEngineId == null || this.bucketEngineId.isEmpty()) {
			throw new RuntimeException(BUCKETENGINEID_ERRMSG);
		}
		try {
			BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
			this.textractClient = AmazonTextractClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(this.region).build();
			this.s3Client = AmazonS3ClientBuilder.standard().withRegion(this.region)
					.withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();
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
		boolean pdf = fileToProcess.getName().toLowerCase().endsWith(".pdf");
		if (pdf) {
			try {
				return PDFUtility.pdfContainsImages(fileToProcess.getAbsolutePath());
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return false;
	}
	
	@Override
	public int processDocument(String outputCsvFilePath, File fileToProcess, Map<String, Object> parameters) {
		VectorDatabaseCSVWriter writer = new VectorDatabaseCSVWriter(outputCsvFilePath);
		List<String> extractedTextFromDoc = new ArrayList<String>();
		String documentKeyName = null;	
		String folderPath = null;

		try {
			documentKeyName = fileToProcess.getName();
			folderPath = SUB_FOLDER + documentKeyName;
			
			IStorageEngine storageeng = Utility.getStorage(this.bucketEngineId);
			Map<String, Object> metadata = new HashMap<>();
			metadata.put("utility", fileToProcess.getName() + "- Textract_functionality");
			
			storageeng.copyToStorage(fileToProcess.toString(),
					OUTPUTBUCKET + DIR_SEPARATOR + SUB_FOLDER + fileToProcess.getName(), metadata);			
			
			extractedTextFromDoc = textractFromDocument(folderPath, OUTPUTBUCKET);
			
			for (int i = 0; i < extractedTextFromDoc.size(); i++) {
				int k = i + 1;
				writer.writeRow(fileToProcess.getName(), String.valueOf(k), extractedTextFromDoc.get(i));
			}
		}catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}finally {
			writer.close();
		}

		return writer.getRowsInCsv();
	}

	public List<String> textractFromDocument(String documentName, String S3BucketPath) {
		List<String> extractedTextFromDoc = new ArrayList<String>();
		try {
			// Create the StartDocumentTextDetection request
			StartDocumentTextDetectionRequest request = new StartDocumentTextDetectionRequest()
					.withDocumentLocation(new DocumentLocation()
							.withS3Object(new S3Object().withBucket(S3BucketPath).withName(documentName)));

			// Start text detection
			StartDocumentTextDetectionResult result = this.textractClient.startDocumentTextDetection(request);

			// results
			GetDocumentTextDetectionRequest getRequest = new GetDocumentTextDetectionRequest()
					.withJobId(result.getJobId());
			GetDocumentTextDetectionResult getResult;
			String nextToken = null;

			do {
				getRequest.setNextToken(nextToken);
				do {
					getResult = this.textractClient.getDocumentTextDetection(getRequest);
					if (getResult.getJobStatus().equalsIgnoreCase("FAILED")) {
						extractedTextFromDoc.add(VAILD_PATH_ERRMSG);
						break;
					}
				} while (!getResult.getJobStatus().equalsIgnoreCase(SUCCEEDED));
				nextToken = getResult.getNextToken();

				for (Block block : getResult.getBlocks()) {
					if (PAGE.equalsIgnoreCase(block.getBlockType())) {
						int pageNumber = block.getPage();
						StringBuilder pageText = new StringBuilder();
						for (Block item : getResult.getBlocks()) {
							if (item.getPage() == pageNumber
									&& LINE.equalsIgnoreCase(item.getBlockType())) {
								pageText.append(item.getText());
							}
						}
						extractedTextFromDoc.add(pageText.toString());
					}
				}
			} while (nextToken != null);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e);
		}
		return extractedTextFromDoc;
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