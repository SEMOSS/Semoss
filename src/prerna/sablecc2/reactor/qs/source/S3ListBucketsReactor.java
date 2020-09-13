package prerna.sablecc2.reactor.qs.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class S3ListBucketsReactor  extends AbstractReactor{

	public S3ListBucketsReactor() {
		this.keysToGet = new String[] { "region" };
	}
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String clientRegion = this.keyValue.get(this.keysToGet[0]);
		List<HashMap<String, Object>> bucketList = new ArrayList<HashMap<String, Object>>();


		AWSCredentials creds = S3Utils.getInstance().getS3Creds();
		if (creds != null );
		try{
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
					.withRegion(clientRegion)
					.withCredentials(new AWSStaticCredentialsProvider(creds))
					.build();
			List<Bucket> buckets = s3Client.listBuckets();
			for (Bucket b : buckets) {
				HashMap<String, Object> tempMap = new HashMap<String, Object>();
				tempMap.put("name", b.getName());
				bucketList.add(tempMap);
			    System.out.println("* " + b.getName());
			}
		} catch (AmazonServiceException e) {
			// The call was transmitted successfully, but Amazon S3 couldn't process 
			// it, so it returned an error response.
			e.printStackTrace();
		} catch (SdkClientException e) {
			// Amazon S3 couldn't be contacted for a response, or the client
			// couldn't parse the response from Amazon S3.
			e.printStackTrace();
		}catch (Exception e) {
			e.printStackTrace();
		}
		
	
		// TODO Auto-generated method stub
		NounMetadata noun = new NounMetadata(bucketList, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.S3);
		return noun;
	}


}
