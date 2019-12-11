package prerna.sablecc2.reactor.export;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.*;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class PushAssetToS3Reactor  extends AbstractReactor {


	private static Properties socialData = null;
	private static final String AWS_ACCOUUNT = "aws_account";
	private static final String AWS_KEY = "aws_key";


	public PushAssetToS3Reactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(), "bucket", "region"};
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

		String bucketName = this.keyValue.get(this.keysToGet[2]);
		String clientRegion = this.keyValue.get(this.keysToGet[3]);


		File f = new File(DIHelper.getInstance().getProperty("SOCIAL"));
		FileInputStream fis = null;

		try {
			if(f.exists()) {
				socialData = new Properties();
				fis = new FileInputStream(f);
				socialData.load(fis);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}


		S3Object fullObject = null;

		String account = socialData.getProperty(AWS_ACCOUUNT);
		String key = socialData.getProperty(AWS_KEY);

		if (account != null && account.length() > 0 && key != null && key.length() > 0) {
			BasicAWSCredentials awsCreds = new BasicAWSCredentials(account, key);
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
					.withRegion(clientRegion)
					.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
					.build();
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

				// if the file to download is a dir it will be zipped
				if (assetToPush.isDirectory()) {
					try {
						MultipleFileUpload xfer = xfer_mgr.uploadDirectory(bucketName,
								assetToPush.getName(), new File("/"), true);
						try {
							xfer.waitForCompletion();
						} catch (AmazonServiceException e) {
							System.err.println("Amazon service error: " + e.getMessage());
						} catch (AmazonClientException e) {
							System.err.println("Amazon client error: " + e.getMessage());
						} catch (InterruptedException e) {
							System.err.println("Transfer interrupted: " + e.getMessage());
						}
					} catch (AmazonServiceException e) {
						System.err.println(e.getErrorMessage());
					}
					xfer_mgr.shutdownNow();
					//do something
				} else{
					try {
						Upload xfer = xfer_mgr.upload(bucketName, assetToPush.getName(), assetToPush);
						try {
							xfer.waitForCompletion();
						} catch (AmazonServiceException e) {
							System.err.println("Amazon service error: " + e.getMessage());
						} catch (AmazonClientException e) {
							System.err.println("Amazon client error: " + e.getMessage());
						} catch (InterruptedException e) {
							System.err.println("Transfer interrupted: " + e.getMessage());
						}
					} catch (AmazonServiceException e) {
						System.err.println(e.getErrorMessage());
						System.exit(1);
					}
					xfer_mgr.shutdownNow();
				}
			}
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.SUCCESS);

	}
}