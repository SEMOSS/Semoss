package prerna.sablecc2.reactor.qs.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.Bucket;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class S3ListBucketsReactor extends AbstractS3Reactor {

	public S3ListBucketsReactor() {
		this.keysToGet = getS3KeysToGet(null);
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
			AmazonS3 s3Client = getS3Client();
			
			List<Bucket> buckets = s3Client.listBuckets();
			for (Bucket b : buckets) {
				HashMap<String, Object> tempMap = new HashMap<String, Object>();
				tempMap.put("name", b.getName());
				bucketList.add(tempMap);
				System.out.println("* " + b.getName());
			}
		} catch (SdkClientException e) {
			e.printStackTrace();
			return getError("Error occurred listing buckets: " + e.getMessage());
		}
		
		NounMetadata noun = new NounMetadata(bucketList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.S3);
		return noun;
	}

}
