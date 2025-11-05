package prerna.reactor.engine;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class DownloadEngineAssetReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(DownloadEngineAssetReactor.class);
	
	public DownloadEngineAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.FILE_PATH.getKey() };
		this.keyRequired = new int[] {1,0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException("Engine " + engineId + " does not exist or user does not have access to edit assets.");
		}

		String relativeFilePath = this.keyValue.get(this.keysToGet[1]);
		if(relativeFilePath != null) {
			relativeFilePath = Utility.normalizePath(relativeFilePath.trim());
			if(!relativeFilePath.isEmpty()) {
				relativeFilePath = relativeFilePath.replace('\\', '/');
				if(!relativeFilePath.startsWith("/")) {
					relativeFilePath = "/" + relativeFilePath;
				}
			}
		}
		
		String filePath = EngineUtility.getSpecificEngineBaseFolder(engineId);
		if(relativeFilePath != null && !relativeFilePath.isEmpty()) {
			filePath += relativeFilePath;
		}
		
		File toDownloadF = new File(filePath);
		if(!toDownloadF.exists()) {
			throw new IllegalArgumentException("The file/directory " + relativeFilePath + " does not exist within the engine folder");
		}

		String downloadFileLocation = null;
		if(toDownloadF.isDirectory()) {
			// we need to make a zip
			// and make sure its unique
			// zip goes at same level as the directory
			String zipFileLocation = Utility.getUniqueFilePath(this.insight.getInsightFolder(), toDownloadF.getName() + ".zip");
			zipFolder(toDownloadF.getAbsolutePath(), zipFileLocation);
			// the new download file location is now zipFileLocation
			downloadFileLocation = zipFileLocation;
		} else {
			downloadFileLocation = toDownloadF.getAbsolutePath();
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
			classLogger.error("Error zipping folder <{}> with download path <{}>.", folder, downloadPath);
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Unable to zip and download directory");
		} finally {
			try {
				if (zos != null) {
					zos.flush();
					zos.close();
				}
			} catch (IOException e) {
				classLogger.error(e.getMessage());
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Could not flush or close Zip Output Stream.");
			}
		}
	}
	
	@Override
	public String getReactorDescription() {
		return "Download a file or directory from within the engine folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the engine";
		} else if(key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The relative file path to a file or directory within the engine folder";
		}
		return super.getDescriptionForKey(key);
	}

}