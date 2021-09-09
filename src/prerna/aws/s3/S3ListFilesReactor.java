package prerna.aws.s3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
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

public class S3ListFilesReactor extends AbstractReactor {
	
	private static final String BUCKET = "bucket";
	private static final String PATH = "path";
	private static final String RECURSIVE = "recursive";
	private static final String LIMIT = "limit";
	
	public S3ListFilesReactor() {
		this.keysToGet = S3Utils.addCommonS3Keys(new String[] { BUCKET, PATH, RECURSIVE, LIMIT });
	}
	
	@Override
	public String getDescriptionForKey(String key) {
		if(key.equals(BUCKET)) {
			return "S3 bucket name";
		} else if(key.equals(PATH)) {
			return "S3 path to list files from";
		} else if(key.equals(RECURSIVE)) {
			return "Boolean flag to indicate if objects should be listed recursively (default false)";
		} else if(key.equals(LIMIT)) {
			return "Max number of results to list (default 100). Use -1 for unlimited";
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
		return "List the files in an S3 bucket. Credentials can be optionally set via a profile path/name, or with an explicit access key and secret. Otherwise, credentials from environment variables or social properties are used.";
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String bucketName = this.keyValue.get(BUCKET);
		String path = this.keyValue.get(PATH);
		if(bucketName == null || bucketName.isEmpty() || path == null){
			throw new IllegalArgumentException("Bucket and Path cannot be empty");
		}
		
		int limit = 100;
		String limitString = this.keyValue.get(LIMIT);
		if(limitString != null && !limitString.isEmpty()) {
			try {
				limit = Integer.parseInt(limitString);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Limit must be an integer");
			}
		}
		
		List <Map<String, Object>> output = new ArrayList <Map<String, Object>>();
		try{
			AmazonS3 s3Client = S3Utils.getInstance().getS3Client(this.keyValue);

			Boolean isTopLevel = false;
			String delimiter = "/";
			if(path.equals("") || path.equals("/")) {
				isTopLevel = true;
			}
			if (!path.endsWith(delimiter)) {
				path += delimiter;
			}

			ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);
			if(!Boolean.parseBoolean(this.keyValue.get(RECURSIVE))) {
				listObjectsRequest.setDelimiter(delimiter);
			}
			
			if (!isTopLevel) {
				listObjectsRequest.setPrefix(path);
			}
			
			if(limit > 0) {
				listObjectsRequest.setMaxKeys(limit);
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
		} catch (SdkClientException e) {
			e.printStackTrace();
			return getError("Error occurred listing files: " + e.getMessage());
		}
		
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
