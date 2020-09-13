package prerna.sablecc2.reactor.qs.source;


import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.sablecc2.reactor.qs.AbstractQueryStructReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class S3FileRetrieverReactor extends AbstractQueryStructReactor{

	private static final String CLASS_NAME = S3FileRetrieverReactor.class.getName();


	public S3FileRetrieverReactor() {
		this.keysToGet = new String[] { "bucket","path", "region" };
	}
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		//get keys
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String bucketName = this.keyValue.get(this.keysToGet[0]);
		String path = this.keyValue.get(this.keysToGet[1]);
		String clientRegion = this.keyValue.get(this.keysToGet[2]);

		if (bucketName == null || bucketName.length() <= 0) {
			throw new IllegalArgumentException("Need to specify bucket name");
		}	
		if (path == null || path.length() <= 0) {
			throw new IllegalArgumentException("Need to file path on s3");
		}
		if (clientRegion == null || clientRegion.length() <= 0) {
			throw new IllegalArgumentException("Need to specify region");
		}


		String filePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + DIR_SEPARATOR 
				+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		
		filePath += DIR_SEPARATOR + Utility.getRandomString(10) + ".csv";



		try {
			
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
					.withRegion(clientRegion)
					.withCredentials(new AWSStaticCredentialsProvider(S3Utils.getInstance().getS3Creds()))
					.build();

			// Get an object and print its contents.
			System.out.println("Downloading an object");
			File localFile = new File(filePath);

			ObjectMetadata object = s3Client.getObject(new GetObjectRequest(bucketName, path), localFile);



		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process 
			// it, so it returned an error response.
			e.printStackTrace();
		} catch (SdkClientException e) {
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			e.printStackTrace();
		}
		
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(',');
		helper.parse(filePath);
		Map[] predictionMaps = FileHelperUtil.generateDataTypeMapsFromPrediction(helper.getHeaders(), helper.predictTypes());
		Map<String, String> dataTypes = predictionMaps[0];
		Map<String, String> additionalDataTypes = predictionMaps[1];
		CsvQueryStruct qs = new CsvQueryStruct();
		for (String keys : dataTypes.keySet()) {
			qs.addSelector("DND", keys);
		}
		helper.clear();
		qs.merge(this.qs);
		qs.setFilePath(filePath);
		qs.setDelimiter(',');
		qs.setColumnTypes(dataTypes);
		qs.setAdditionalTypes(additionalDataTypes);
		
		return qs;
	}


	public static void main(String[] args) throws IOException {
		Regions clientRegion = ***REMOVED***;
		String bucketName = "sample";
		String key = "frame_export2.csv";

		S3Object fullObject = null;

		BasicAWSCredentials awsCreds = new BasicAWSCredentials("test", "test");


		try {
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
					.withRegion(clientRegion)
					.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
					.build();

			// Get an object and print its contents.
			System.out.println("Downloading an object");
			File localFile = new File("/Users/semoss/Documents/Daily_Notes/s3_push_pull/testfile");

			ObjectMetadata object = s3Client.getObject(new GetObjectRequest(bucketName, key), localFile);

			System.out.println("Content-Type: " + object.getContentType());



		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process 
			// it, so it returned an error response.
			e.printStackTrace();
		} catch (SdkClientException e) {
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			e.printStackTrace();
		} finally {
			// To ensure that the network connection doesn't remain open, close any open input streams.
			if (fullObject != null) {
				fullObject.close();
			}
		}
	}


}
