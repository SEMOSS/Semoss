package prerna.aws.s3;

import static prerna.aws.s3.S3Utils.BUCKET;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.SSECustomerKey;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;

public class S3FileDownloadToAssetsReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(S3FileDownloadToAssetsReactor.class);

	public static final String PATH = "path";
	public static final String TARGET_SPACE = "targetSpace";
	public static final String SSE_KEY_PATH = "sseKeyPath";
	public static final String SSE_KEY_64 = "sseKey64";

	public S3FileDownloadToAssetsReactor() {
		this.keysToGet = S3Utils.addCommonS3Keys(new String[] { BUCKET, PATH, TARGET_SPACE, SSE_KEY_PATH, SSE_KEY_64 });
	}

	@Override
	public String getDescriptionForKey(String key) {
		if(key.equals(BUCKET)) {
			return "S3 bucket name";
		} else if(key.equals(PATH)) {
			return "Path to S3 object";
		} else if(key.equals(TARGET_SPACE)) {
			return "Destination target indicator: \"USER\", a project id, or \"INSIGHT\" (default)";
		} else if(key.equals(SSE_KEY_PATH)) {
			return "File path to a custom encryption key";
		} else if(key.equals(SSE_KEY_64)) {
			return "Base-64 encoding of a custom encryption key";
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
		return "Download a file in an S3 bucket to an asset folder. If a custom server-side encryption was used, it can be provided via a base-64 encoded string or with a file path to the key on the local filesystem. Credentials can be set via a profile path/name or with an explicit access key and secret";
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		String bucketName = this.keyValue.get(BUCKET);
		String path = this.keyValue.get(PATH);
		if (bucketName == null || bucketName.length() <= 0) {
			throw new IllegalArgumentException("Need to specify bucket name");
		}
		if (path == null || path.length() <= 0) {
			throw new IllegalArgumentException("Need to specify file path on s3");
		}

		String space = this.keyValue.get(TARGET_SPACE);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, true);
		if(assetFolder == null || assetFolder.isEmpty()) {
			return getError("Unable to retrieve asset directory");
		}

		Path targetPath = Paths.get(assetFolder, path).normalize();
		String bucketKey = Paths.get(assetFolder).relativize(targetPath).toString().replace('\\', '/');

		String filePath = targetPath.toString();
		File localFile = new File(filePath);
		try {
			AmazonS3 s3Client = S3Utils.getInstance().getS3Client(this.keyValue);
			GetObjectRequest request = new GetObjectRequest(bucketName, bucketKey);

			// add custom decryption, if needed
			String sseKeyPath = this.keyValue.get(SSE_KEY_PATH);
			String sseKey64 = this.keyValue.get(SSE_KEY_64);
			if(sseKeyPath != null && !sseKeyPath.isEmpty()) {
				try {
					byte[] keyBytes = Files.readAllBytes(new File(sseKeyPath).toPath());
					request.setSSECustomerKey(new SSECustomerKey(keyBytes));
				} catch(IOException e) {
					return getError("Unable to read key file from sseKeyPath. Detailed message: " + e.getMessage());
				}
			} else if(sseKey64 != null && !sseKey64.isEmpty()) {
				request.setSSECustomerKey(new SSECustomerKey(sseKey64));
			}

			s3Client.getObject(request, localFile);
		} catch (SdkClientException e) {
			logger.error(Constants.STACKTRACE, e);
			return getError("Error occurred downloading from S3: " + e.getMessage());
		}

		NounMetadata noun = new NounMetadata("File downloaded successfully to " + filePath, PixelDataType.CONST_STRING, PixelOperationType.S3);
		return noun;
	}

}
