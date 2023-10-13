package prerna.aws.s3;

import java.io.File;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;

import prerna.poi.main.helper.CSVFileHelper;
import prerna.poi.main.helper.FileHelperUtil;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class S3FileRetrieverReactor extends AbstractQueryStructReactor {
	
	private static final Logger logger = LogManager.getLogger(S3FileRetrieverReactor.class);

	private static final String BUCKET = "bucket";
	private static final String PATH = "path";
	
	public S3FileRetrieverReactor() {
		this.keysToGet = S3Utils.addCommonS3Keys(new String[] { BUCKET, PATH });
	}
	
	@Override
	public String getDescriptionForKey(String key) {
		if(key.equals(BUCKET)) {
			return "S3 bucket name";
		} else if(key.equals(PATH)) {
			return "S3 path to download from";
		} else {
			String commonDescription = S3Utils.getDescriptionForCommonS3Key(key);
			if(commonDescription != null) {
				return commonDescription;
			}
		}
		return super.getDescriptionForKey(key);
	}
	
	@Override
	public String getReactorDescription() {
		return "Download and load a csv file from an S3 bucket. Credentials can be optionally set via a profile path/name, or with an explicit access key and secret. Otherwise, credentials from environment variables or social properties are used.";
	}
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		organizeKeys();
		String bucketName = this.keyValue.get(this.keysToGet[0]);
		String path = this.keyValue.get(this.keysToGet[1]);

		if (bucketName == null || bucketName.length() <= 0) {
			throw new IllegalArgumentException("Need to specify bucket name");
		}	
		if (path == null || path.length() <= 0) {
			throw new IllegalArgumentException("Need to give file path on s3");
		}

		String filePath = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + DIR_SEPARATOR 
				+ DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		
		filePath += DIR_SEPARATOR + Utility.getRandomString(10) + ".csv";

		try {
			AmazonS3 s3Client = S3Utils.getInstance().getS3Client(this.keyValue);

			File localFile = new File(filePath);
			s3Client.getObject(new GetObjectRequest(bucketName, path), localFile);
		} catch (SdkClientException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process 
			// it, so it returned an error response.
			logger.error(Constants.STACKTRACE, e);
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


}
