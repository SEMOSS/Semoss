package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class DownloadAssetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DownloadAssetReactor.class);
	
	public DownloadAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get base asset folder
		String filePath = this.keyValue.get(this.keysToGet[0]);
		if(filePath != null && filePath.startsWith("/")) {
			filePath = filePath.substring(1);
		}
		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, false).replace("\\", "/");

		File downloadF = null;
		if(filePath != null) {
			downloadF = new File(assetFolder + "/" + filePath);
		} else {
			downloadF = new File(assetFolder);
		}
		
		if(!downloadF.exists()) {
			throw new IllegalArgumentException("Could not find file or directory with name " + filePath);
		}

		String downloadFileLocation = null;
		if(downloadF.isDirectory()) {
			// we need to make a zip
			// and make sure its unique
			// zip goes at same level as the directory
			String zipFileLocation = Utility.getUniqueFilePath(downloadF.getParent(), downloadF.getName() + ".zip");
			zipFolder(downloadF.getAbsolutePath(), zipFileLocation);
			// the new download file location is now zipFileLocation
			downloadFileLocation = zipFileLocation;
			
			
			//TODO: add logic to add this to cloud
			//TODO: add logic to add this to cloud
			//TODO: add logic to add this to cloud
			//TODO: add logic to add this to cloud
			//TODO: add logic to add this to cloud
			//TODO: add logic to add this to cloud

			
		} else {
			downloadFileLocation = downloadF.getAbsolutePath();
		}
		
		// store the insight file 
		// in the insight so the FE can download it
		// only from the given insight
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setFilePath(downloadFileLocation);
		insightFile.setDeleteOnInsightClose(false);
		this.insight.addExportFile(downloadKey, insightFile);
		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
		return retNoun;
	}

	/**
	 * Zip the directory
	 * @param folder
	 * @param downloadPath
	 */
	private void zipFolder(String folder, String downloadPath) {
		ZipOutputStream zos = null;
		try {
			zos = ZipUtils.zipFolder(folder, downloadPath);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to zip and download directory", e);
		} finally {
			try {
				if (zos != null) {
					zos.flush();
					zos.close();
				}
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Unable to zip and download directory", e);
			}
		}
	}

}