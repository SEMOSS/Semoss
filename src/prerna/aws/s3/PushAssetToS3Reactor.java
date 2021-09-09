package prerna.aws.s3;

import java.io.File;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;

public class PushAssetToS3Reactor extends AbstractReactor {
	
	private static final String BUCKET = "bucket";
	
	public PushAssetToS3Reactor() {
		this.keysToGet = S3Utils.addCommonS3Keys(new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(), BUCKET});
	}
	
	@Override
	public String getDescriptionForKey(String key) {
		if(key.equals(BUCKET)) {
			return "S3 bucket name";
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
		return "Upload an asset file to an S3 bucket. Credentials can be set via a profile path/name or with an explicit access key and secret";
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get base asset folder
		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, false);
		String pushPath = assetFolder;
		// if a specific file is specified for download
		String relativeAssetPath = keyValue.get(keysToGet[0]);
		String bucketName = this.keyValue.get(BUCKET);
		
		AmazonS3 s3Client = S3Utils.getInstance().getS3Client(this.keyValue);
		
		if (relativeAssetPath != null && relativeAssetPath.length() > 0) {
			pushPath = assetFolder + DIR_SEPARATOR + relativeAssetPath;
			File assetToPush = new File(pushPath);
			if (!assetToPush.exists()) {
				NounMetadata error = NounMetadata.getErrorNounMessage("File does not exist");
				SemossPixelException exception = new SemossPixelException(error);
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
			
			TransferManager xfer_mgr = TransferManagerBuilder.standard().withS3Client(s3Client).build();
			boolean transferFailure = false;
			try {
				if (assetToPush.isDirectory()) {
					MultipleFileUpload xfer = xfer_mgr.uploadDirectory(bucketName,
							assetToPush.getName(), new File("/"), true);
					xfer.waitForCompletion();
				} else {
					Upload xfer = xfer_mgr.upload(bucketName, assetToPush.getName(), assetToPush);
					xfer.waitForCompletion();
				}
			} catch (AmazonClientException | InterruptedException e) {
				System.err.println("Amazon upload failure: " + e.getMessage());
				transferFailure = true;
			}
			xfer_mgr.shutdownNow();
			
			if(transferFailure) {
				return getError("Error occurred during upload");
			}
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.SUCCESS);
	}
}
