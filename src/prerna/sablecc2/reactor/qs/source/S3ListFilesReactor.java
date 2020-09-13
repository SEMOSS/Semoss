package prerna.sablecc2.reactor.qs.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class S3ListFilesReactor extends AbstractReactor{


	AmazonS3 s3Client = null; 

	public S3ListFilesReactor() {
		this.keysToGet = new String[] { "region" , "bucket", "path" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String clientRegion = this.keyValue.get(this.keysToGet[0]);
		String bucketName = this.keyValue.get(this.keysToGet[1]);
		String path = this.keyValue.get(this.keysToGet[2]);
		if(clientRegion == null || clientRegion.isEmpty() || bucketName == null || bucketName.isEmpty()  || path == null){
			throw new IllegalArgumentException("Region and Path cannot be empty");
		}


		Map<String, Object> returnMap = new HashMap<String, Object>();
		List <Map<String, Object>> output = new ArrayList <Map<String, Object>>();

		AWSCredentials creds = S3Utils.getInstance().getS3Creds();
		if (creds != null );
		try{
			s3Client = AmazonS3ClientBuilder.standard()
					.withRegion(clientRegion)
					.withCredentials(new AWSStaticCredentialsProvider(creds))
					.build();

			Boolean isTopLevel = false;
			String delimiter = "/";
			if(path.equals("") || path.equals("/")) {
				isTopLevel = true;
			}
			if (!path.endsWith(delimiter)) {
				path += delimiter;
			}

			ListObjectsRequest listObjectsRequest = null;
			if (isTopLevel) {
				listObjectsRequest =
						new ListObjectsRequest().withBucketName(bucketName).withDelimiter(delimiter);
			} else {
				listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName).withPrefix(path)
						.withDelimiter(delimiter);
			}
			com.amazonaws.services.s3.model.ObjectListing objects = s3Client.listObjects(listObjectsRequest);

			List<String> folders = objects.getCommonPrefixes();
			List<S3ObjectSummary> files = objects.getObjectSummaries();

			for(String s : folders){
				Map<String, Object> folderMap = new HashMap<>();
				folderMap.put("name", s);
				folderMap.put("type", "folder");
				output.add(folderMap);
				System.out.println(s);
			}
			//the base path is being added as a file, manually remove on the ret map
			for (S3ObjectSummary o : files) {
				String fileName = o.getKey();
				if (fileName.equals(path)){
					continue;
				}
				Map<String, Object> fileMap = new HashMap<>();
				System.out.println(o.getKey());
				fileMap.put("name", o.getKey());
				fileMap.put("type", "file");
				output.add(fileMap);
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
		NounMetadata noun = new NounMetadata(output, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.S3);
		return noun;

	}

	public static void main(String[] args){

		BasicAWSCredentials awsCreds = new BasicAWSCredentials("test", "test");
		List <Map<String, Object>> output = new ArrayList <Map<String, Object>>();

		try{
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
					.withRegion("us-east-1")
					.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
					.build();
			String path = "test/testinner/";
			ListObjectsRequest req = new ListObjectsRequest().withBucketName("samplekunal2").withPrefix(path).withDelimiter("/");
			com.amazonaws.services.s3.model.ObjectListing objects = s3Client.listObjects(req);

			List<S3ObjectSummary> S3Objects = objects.getObjectSummaries();

			//			String prefix = "test/testinner/";
			//			String bucketName = "samplekunal2";

			System.out.println("FOLDERS");

			for(String s : objects.getCommonPrefixes()){
				Map<String, Object> folderMap = new HashMap<>();
				folderMap.put("name", s);
				folderMap.put("type", "folder");
				output.add(folderMap);
				System.out.println(s);
			}
			System.out.println("FILES");

			//List<S3ObjectSummary> S3Objects = s3Client.listObjectsV2("samplekunal2", "").getObjectSummaries();
			for (S3ObjectSummary o : S3Objects) {
				String fileName = o.getKey();
				if (fileName.equals(path)){
					continue;
				}
				Map<String, Object> fileMap = new HashMap<>();
				System.out.println(o.getKey());
				fileMap.put("name", o.getKey());
				fileMap.put("type", "file");
				output.add(fileMap);
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
		for(Map<String, Object> m : output){
			System.out.println("item");
			for (Map.Entry<String, Object> entry : m.entrySet()) {
				System.out.println(entry.getKey() + ":" + entry.getValue().toString());
			}
		}
	}

}
