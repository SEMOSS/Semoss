package prerna.aws.s3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class S3ListBucketsReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(S3ListBucketsReactor.class);
	
	public S3ListBucketsReactor() {
		this.keysToGet = S3Utils.addCommonS3Keys(null);
	}
	
	@Override
	public String getDescriptionForKey(String key) {
		String commonDescription = S3Utils.getDescriptionForCommonS3Key(key);
		if(commonDescription != null) {
			return commonDescription;
		}
		return super.getDescriptionForKey(key);
	}
	
	@Override
	public String getReactorDescription() {
		return "List the bucket names accessible in S3. Credentials can be optionally set via a profile path/name, or with an explicit access key and secret. Otherwise, credentials from environment variables or social properties are used.";
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
				
		List<HashMap<String, Object>> bucketList = new ArrayList<HashMap<String, Object>>();
		try{
			AmazonS3 s3Client = S3Utils.getInstance().getS3Client(this.keyValue);
			
			List<Bucket> buckets = s3Client.listBuckets();
			for (Bucket b : buckets) {
				HashMap<String, Object> tempMap = new HashMap<String, Object>();
				tempMap.put("name", b.getName());
				bucketList.add(tempMap);
				logger.debug("* " + b.getName());
			}
		} catch (SdkClientException e) {
			logger.error(Constants.STACKTRACE, e);
			return getError("Error occurred listing buckets: " + e.getMessage());
		}
		
		NounMetadata noun = new NounMetadata(bucketList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.S3);
		return noun;
	}

}
