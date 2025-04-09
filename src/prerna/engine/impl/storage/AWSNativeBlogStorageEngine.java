package prerna.engine.impl.storage;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.StorageTypeEnum;
import prerna.util.Constants;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

public class AWSNativeBlogStorageEngine extends AbstractStorageEngine {

	private static final Logger classLogger = LogManager.getLogger(AWSNativeBlogStorageEngine.class);

	public static final String S3_REGION_KEY = "S3_REGION";
	public static final String S3_BUCKET_KEY = "S3_BUCKET";
	public static final String S3_ACCESS_KEY = "S3_ACCESS";
	public static final String S3_SECRET_KEY = "S3_SECRET";
	public static final String BUCKETNAME = "BUCKETNAME";

	private String accessKey;
	private String secretKey;
	private String region;
	private String bucketPath;

	private S3Client client = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.accessKey = smssProp.getProperty(S3_ACCESS_KEY);
		this.secretKey = smssProp.getProperty(S3_SECRET_KEY);
		this.region = smssProp.getProperty(S3_REGION_KEY);
		this.bucketPath = smssProp.getProperty(BUCKETNAME);

		if (this.accessKey == null || this.accessKey.isEmpty()) {
			throw new RuntimeException("Must pass in an access key");
		}
		if (this.secretKey == null || this.secretKey.isEmpty()) {
			throw new RuntimeException("Must pass in a secret key");
		}
		if (this.region == null || this.region.isEmpty()) {
			throw new RuntimeException("Must pass in a region");
		}
		if (this.bucketPath == null || this.bucketPath.isEmpty()) {
			throw new RuntimeException("Must pass in a S3BucketPath");
		}
		createServiceClient();
	}

	public void createServiceClient() {
		this.client = S3Client.builder().region(Region.AP_SOUTH_1)
				.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
				.build();
		classLogger.info("S3 Blob Service client created successfully.");
	}

	@Override
	public StorageTypeEnum getStorageType() {
		return StorageTypeEnum.AMAZON_S3_NATIVE;
	}

	@Override
	public List<String> list(String path) throws Exception {
		List<String> fileList = new ArrayList<String>();

		try {
			ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketPath).prefix(path)
					.build();
			ListObjectsV2Response listObjectsV2Response = this.client.listObjectsV2(listObjectsV2Request);
			for (S3Object object : listObjectsV2Response.contents()) {
				fileList.add(object.key());
			}

		} catch (S3Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return fileList;
	}

	@Override
	public List<Map<String, Object>> listDetails(String path) throws Exception {
		List<Map<String, Object>> objectDetails = new ArrayList<>();

		try {

			ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketPath).prefix(path)
					.build();
			ListObjectsV2Response listObjectsV2Response = this.client.listObjectsV2(listObjectsV2Request);

			for (S3Object object : listObjectsV2Response.contents()) {
				Map<String, Object> objectInfo = new HashMap<>();
				objectInfo.put("key", object.key());
				objectInfo.put("size", object.size());
				objectInfo.put("lastModified", object.lastModified());
				objectInfo.put("etag", object.eTag());
				objectDetails.add(objectInfo);
			}

		} catch (S3Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		return objectDetails;
	}

	@Override
	public void syncLocalToStorage(String localPath, String storagePath, Map<String, Object> metadata)
			throws Exception {

		Path localFilePath = Paths.get(localPath);
		List<String> uploadedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		boolean found = false;

		try {
			if (!Files.exists(localFilePath)) {
				throw new Exception("Invalid path:" + localPath);
			}
			Path localBasePath = Files.isDirectory(localFilePath) ? localFilePath : localFilePath.getParent();

			// Remove empty directories locally
			deleteEmptyDirectories1(localFilePath);

			Files.walk(localBasePath).filter(Files::isRegularFile).forEach(file -> {
				try {
					uploadedFiles.add(uploadFileToS3(storagePath, file, metadata));
				} catch (Exception e) {
					failedFiles.add(file.toString());
					classLogger.error("Failed to upload file:" + file, e);
				}
			});
			found = true;
		} catch (Exception e) {
			classLogger.error("Sync operation failed.Rolling back failed uploads.", e);
			throw e;

		}

		classLogger.info(uploadedFiles.isEmpty() ? "No files were uploaded." : "Sucessfully uploaded:" + uploadedFiles);
		classLogger.info(
				found ? "Sync complted successfully for:" + storagePath : "No files found to sync for:" + storagePath);
	}

	@Override
	public void syncStorageToLocal(String storagePath, String localPath) throws Exception {

		Path localDirectory = Paths.get(localPath);

		if (!Files.exists(localDirectory) || !Files.isDirectory(localDirectory)) {
			Files.createDirectories(localDirectory);
		}

		ListObjectsV2Response listObjectsResponse = s3ListObjectResponse(bucketPath, storagePath);
		Set<String> cloudFiles = new HashSet<>();

		// Iterate through each object found in the S3 bucket.
		for (S3Object s3Object : listObjectsResponse.contents()) {
			String key = s3Object.key();
			Path localFilePath = localDirectory.resolve(key.toString());
			cloudFiles.add(localFilePath.toString());

			// Ensure the parent directories exist for the file before downloading.
			Files.createDirectories(localFilePath.getParent());

			// Check if the file needs to be downloaded based on its existence and
			// modification time.
			boolean shouldDownload = !Files.exists(localFilePath);
			if (!shouldDownload) {
				FileTime localModifiedTime = Files.getLastModifiedTime(localFilePath);
				long localFileSize = Files.size(localFilePath);

				shouldDownload = s3Object.size() != localFileSize
						|| s3Object.lastModified().toEpochMilli() > localModifiedTime.toMillis();
			}

			if (shouldDownload) {
				downloadedFile(bucketPath, key, localFilePath);
				classLogger.info("Downloaded: " + localFilePath);
			}
		}
	}

	@Override
	public void copyToStorage(String storageFolderPath, String localFilePath, Map<String, Object> metadata)
			throws Exception {
		Path filePath = Paths.get(localFilePath);
		if (!Files.exists(filePath)) {
			throw new IOException("File not found: " + localFilePath);
		}

		List<String> uploadedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		boolean found = false;

		List<Path> paths = parseLocalPaths(localFilePath);

		for (Path filePaths : paths) {
			if (!Files.exists(filePaths)) {
				classLogger.error("File not found: " + filePaths);
				failedFiles.add(filePaths.toString());
				continue;
			}

		}

		deleteEmptyDirectories1(filePath);

		if (Files.isDirectory(filePath)) {
			try (Stream<Path> fileStream = Files.walk(filePath).filter(Files::isRegularFile)) {
				fileStream.forEach(file -> {
					try {
						uploadedFiles.add(uploadFile(storageFolderPath, file, metadata));
					} catch (Exception e) {
						failedFiles.add(file.toString());
					}
				});
			}
			found = true;
		} else {
			try {
				uploadedFiles.add(uploadFile(storageFolderPath, filePath, metadata));
				found = true;
			} catch (Exception e) {
				failedFiles.add(filePath.toString());
			}
		}

		classLogger.info(
				uploadedFiles.isEmpty() ? "No files were uploaded." : "Successfully uploaded files: " + uploadedFiles);
		classLogger.info(found ? "Copy completed successfully for: " + storageFolderPath
				: "No files found to copy for: " + storageFolderPath);
	}

	@Override
	public void copyToLocal(String storagePath, String localFolderPath) throws Exception {
		// Extract bucket name and folder path
		String[] parts = storagePath.split("/", 2);
		String s3FolderPath = parts.length > 1 ? parts[1] : "";

		localFolderPath = "D://dev//punith";
		
		// Ensure the local directory exists
		Path localDirectory = Paths.get(localFolderPath);
		Files.createDirectories(localDirectory);

		List<String> downloadedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();
		boolean found = false;
		ListObjectsV2Response listObjectsResponse = s3ListObjectResponse(bucketPath, s3FolderPath);

		for (S3Object s3Object : listObjectsResponse.contents()) {
			String key = s3Object.key();
			/*
			 * Path localFilePath =
			 * localDirectory.resolve(key.substring(s3FolderPath.length()));
			 */

			try {
				/* Files.createDirectories(localDirectory.getParent()); */
				downloadFile(key, localDirectory);
				downloadedFiles.add(key);
				classLogger.info("Downloaded file: " + localDirectory);
				found = true;
			} catch (Exception e) {
				failedFiles.add(key);
				classLogger.info("Failed to download: " + key + " - " + e.getMessage());
			}
		}

		if (downloadedFiles.isEmpty()) {
			classLogger.info("No files were downloaded.");
		} else {
			classLogger.info("Successfully downloaded files: " + downloadedFiles);
		}

		if (!failedFiles.isEmpty()) {
			classLogger.info("Some files failed to download: " + failedFiles);
		}

		classLogger.info(found ? "Copy completed successfully for: " + storagePath
				: "No files found to copy for: " + storagePath);
	}

	@Override
	public void deleteFromStorage(String storagePath) throws Exception {

		storagePath = storagePath.replace("\\", "/").replaceFirst("^/", "");

		List<String> deletedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();

		ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketPath)
				.prefix(storagePath.isEmpty() ? null : storagePath).build();

		ListObjectsV2Response result = client.listObjectsV2(listObjectsV2Request);

		if (result == null || result.contents() == null || result.contents().isEmpty()) {

			classLogger.info(storagePath.isEmpty() ? "No files found in bucket:testbucket-punith"
					: "No files found in directory:" + storagePath);
			return;
		}

		boolean hasFilesToDelete = false;

		for (S3Object objectSummary : result.contents()) {
			String objectKey = objectSummary.key();
			if (!objectKey.equals(storagePath) || (objectKey.length() > storagePath.length()
					&& !objectKey.substring(storagePath.length()).contains("/"))) {
				hasFilesToDelete = true;

				if (deleteObject(objectKey)) {
					deletedFiles.add(objectKey);
				} else {
					failedFiles.add(objectKey);
				}
			}
		}

		if (!hasFilesToDelete) {
			classLogger.warn(storagePath.isEmpty() ? "No files found in bucket: " + "testbucket-punith"
					: "No files found in directory: " + storagePath);
			return;
		}

		classLogger.info(
				deletedFiles.isEmpty() ? "No files were deleted." : "Successfully deleted files: " + deletedFiles);

		if (!failedFiles.isEmpty()) {
			classLogger.error("Some files failed to delete. Retrying...");
			retryDelete(failedFiles);
		}
		classLogger.info(
				deletedFiles.isEmpty() ? "No files were deleted." : "Successfully deleted files: " + deletedFiles);
	}

	private void retryDelete(List<String> failedFiles) {
		if (failedFiles == null || failedFiles.isEmpty()) {
			return;
		}

		List<String> stillFailedFiles = new ArrayList<>();

		for (String file : failedFiles) {
			if (!deleteObject(file)) {
				stillFailedFiles.add(file);
			}
		}

		if (!stillFailedFiles.isEmpty()) {
			classLogger.error("Retry failed for some files: " + stillFailedFiles);
		} else {
			classLogger.info("All previously failed files were successfully deleted.");
		}
	}

	@Override
	public void deleteFromStorage(String storagePath, boolean leaveFolderStructure) throws Exception {
		storagePath = storagePath.replace("\\", "/").replaceFirst("^/", "");
		List<String> deletedFiles = new ArrayList<>();
		List<String> failedFiles = new ArrayList<>();

		ListObjectsV2Request listObjects = ListObjectsV2Request.builder().bucket(bucketPath).prefix(storagePath)
				.build();

		ListObjectsV2Response responseListObjects = this.client.listObjectsV2(listObjects);
		List<S3Object> s3Objects = responseListObjects.contents();
		boolean hasFilesToDelete = false;

		for (S3Object s3Object : s3Objects) {
			hasFilesToDelete = true;
			String objectName = s3Object.key();

			// Attempt to delete the object
			if (deleteObject(objectName)) {
				deletedFiles.add(objectName);
			} else {
				failedFiles.add(objectName);
			}
		}
		// Log deletion process results
		classLogger.info(hasFilesToDelete ? "Deletion process completed for: " + storagePath
				: "No files found to delete in path: " + storagePath);

		classLogger.info(
				deletedFiles.isEmpty() ? "No files were deleted." : "Successfully deleted files: " + deletedFiles);

		// Preserve folder structure if required
		if (leaveFolderStructure && !deletedFiles.isEmpty()) {
			preserveFolderStructure(deletedFiles);
		}
	}

	@Override
	public void deleteFolderFromStorage(String storageFolderPath) throws Exception {
		createServiceClient();

		ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucketPath)
				.prefix(storageFolderPath + "/").build();

		ListObjectsV2Response listObjectsV2Response = this.client.listObjectsV2(listObjectsV2Request);

		for (S3Object object : listObjectsV2Response.contents()) {
			DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucketPath).key(object.key())
					.build();
			this.client.deleteObject(deleteObjectRequest);
		}

		DeleteObjectRequest deleteFolderRequest = DeleteObjectRequest.builder().bucket(bucketPath)
				.key(storageFolderPath + "/").build();

		this.client.deleteObject(deleteFolderRequest);

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	private String uploadFileToS3(String storageFolderPath, Path filePath, Map<String, Object> metadata)
			throws Exception {
		String fileKey = storageFolderPath + "/" + filePath.getFileName();

		PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder().bucket(bucketPath).key(fileKey);
		if (metadata != null && !metadata.isEmpty()) {
			Map<String, String> metaDataMap = metadata.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().toString()));
			putObjectRequestBuilder.metadata(metaDataMap);
		}

		this.client.putObject(putObjectRequestBuilder.build(), filePath);
		return storageFolderPath;

	}

	private void deleteEmptyDirectories1(Path path) {
		try {

			List<Path> directories = Files.walk(path).sorted(Comparator.reverseOrder()) // Delete children first
					.filter(Files::isDirectory).collect(Collectors.toList());

			for (Path dir : directories) {
				try (DirectoryStream<Path> entries = Files.newDirectoryStream(dir)) {
					if (!entries.iterator().hasNext()) { // Directory is empty
						Files.delete(dir);
						classLogger.info("Deleted empty local folder: " + dir);
					}
				} catch (IOException e) {
					classLogger.error("Failed to delete empty folder: " + dir, e);
				}
			}
		} catch (IOException e) {
			classLogger.error("Error while deleting empty directories", e);
		}
	}

	private String uploadFile(String storageFolderPath, Path filePath, Map<String, Object> metadata)
			throws S3Exception, IOException {

		String fileKey = storageFolderPath + "/" + filePath.getFileName();
		PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder().bucket(bucketPath).key(fileKey);

		if (metadata != null && !metadata.isEmpty()) {
			Map<String, String> metaDataMap = new HashMap<>();

			for (Map.Entry<String, Object> entry : metadata.entrySet()) {
				metaDataMap.put(entry.getKey(), entry.getValue().toString());

			}
			putObjectRequestBuilder.metadata(metaDataMap);
		}

		this.client.putObject(putObjectRequestBuilder.build(), filePath);
		return fileKey;
	}

	private void downloadFile(String key, Path localFilePath) throws Exception {
		GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketPath).key(key).build();

		// Stream the file from S3 to the local filesystem
		try (InputStream s3Object = this.client.getObject(getObjectRequest);
				FileOutputStream fileOutputStream = new FileOutputStream(localFilePath.toFile())) {
			byte[] buffer = new byte[1024];
			int length;
			while ((length = s3Object.read(buffer)) != -1) {
				fileOutputStream.write(buffer, 0, length);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
		}
	}

	// This method downloads a file from S3 to the specified local path.
	private void downloadedFile(String bucket, String key, Path destinationPath) throws IOException {
		GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();

		try (ResponseInputStream<?> s3Object = this.client.getObject(getObjectRequest);
				FileOutputStream fileOutputStream = new FileOutputStream(destinationPath.toFile())) {
			byte[] buffer = new byte[1024];
			int length;
			while ((length = s3Object.read(buffer)) != -1) {
				fileOutputStream.write(buffer, 0, length);
			}
		}
	}

	private ListObjectsV2Response s3ListObjectResponse(String bucket, String prefix) {
		ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build();

		// Retrieve the list of objects from S3 based on the request.
		ListObjectsV2Response listObjectsResponse = this.client.listObjectsV2(listObjectsRequest);
		return listObjectsResponse;
	}

	private boolean deleteObject(String objectKey) {
		try {
			this.client.deleteObject(DeleteObjectRequest.builder().bucket(bucketPath).key(objectKey).build());
			return true;
		} catch (SdkException e) {
			classLogger.info("Failed to delete object: " + objectKey);
			return false;
		}

	}

	private void preserveFolderStructure(List<String> deletedFiles) {
		for (String deletedFile : deletedFiles) {

			String[] parts = deletedFile.split("/");
			if (parts.length > 1) {
				String folderPath = parts[0];
				String folderPlaceHolder = folderPath + "/";
				this.client.putObject(PutObjectRequest.builder().bucket(bucketPath).key(folderPlaceHolder).build(),
						RequestBody.fromString(""));
			}

			classLogger.info("Folder structure preserved for deleted files.");
		}

	}

}
