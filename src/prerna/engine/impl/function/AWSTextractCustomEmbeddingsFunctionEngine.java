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

public class AWSTextractCustomEmbeddingsFunctionEngine extends AbstractFunctionEngine
		implements ICustomEmbeddingsFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSTextractCustomEmbeddingsFunctionEngine.class);

	private static final String DIR_SEPARATOR = "/";

	private static final String OUTPUTBUCKET = "aws-service-repos";
	private static final String SUB_FOLDER = "textract/";

	private static final String ACCESS_KEY = "ACCESS_KEY";
	private static final String SECRET_KEY = "SECRET_KEY";
	private static final String REGION = "REGION";
	private static final String BUCKETENGINEID = "S3BUCKETENGINEID";
	private static final String SUCCEEDED = "SUCCEEDED";
	private static final String PAGE = "PAGE";
	private static final String LINE = "LINE";

	private String accessKey;
	private String secretKey;
	private String region;
	private String storageEngineId;

	private AmazonTextract textractClient = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		// preset these - don't need user to define
		smssProp.putIfAbsent(IFunctionEngine.NAME_KEY, "AWS Textract - For Use With Vector Database Engines");
		smssProp.putIfAbsent(IFunctionEngine.DESCRIPTION_KEY, "Execute AWS Textract");

		super.open(smssProp);

		this.accessKey = smssProp.getProperty(ACCESS_KEY);
		this.secretKey = smssProp.getProperty(SECRET_KEY);
		this.region = smssProp.getProperty(REGION);
		this.storageEngineId = smssProp.getProperty(BUCKETENGINEID);

		if (this.accessKey == null || this.accessKey.isEmpty()) {
			throw new RuntimeException("Must pass in an access key");
		}
		if (this.secretKey == null || this.secretKey.isEmpty()) {
			throw new RuntimeException("Must pass in a secret key");
		}
		if (this.region == null || this.region.isEmpty()) {
			throw new RuntimeException("Must define the requiredParameters");
		}
		if (this.storageEngineId == null || this.storageEngineId.isEmpty()) {
			throw new RuntimeException("Must pass in a Storage Engine Id for an S3 Bucket");
		}
		try {
			BasicAWSCredentials awsCreds = new BasicAWSCredentials(this.accessKey, this.secretKey);
			this.textractClient = AmazonTextractClientBuilder.standard()
					.withCredentials(new AWSStaticCredentialsProvider(awsCreds)).withRegion(this.region).build();
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

			IStorageEngine storageeng = Utility.getStorage(this.storageEngineId);
			Map<String, Object> metadata = new HashMap<>();
			metadata.put("utility", fileToProcess.getName() + "- Textract_functionality");

			storageeng.copyToStorage(fileToProcess.toString(),
					OUTPUTBUCKET + DIR_SEPARATOR + SUB_FOLDER + fileToProcess.getName(), metadata);

			extractedTextFromDoc = textractFromDocument(folderPath, OUTPUTBUCKET);

			String fileName = fileToProcess.getName();
			for (int i = 0; i < extractedTextFromDoc.size(); i++) {
				// source, divider, content
				writer.writeRow(fileName, String.valueOf(i + 1), extractedTextFromDoc.get(i));
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
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
						extractedTextFromDoc.add("Must provide the valid path");
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
		return FunctionTypeEnum.AWS_TEXTRACT_CUSTOM_EMBEDDINGS.name();
	}
}
