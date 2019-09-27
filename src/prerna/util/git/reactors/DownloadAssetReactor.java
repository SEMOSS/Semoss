package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.zip.ZipOutputStream;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.ZipUtils;

public class DownloadAssetReactor extends AbstractReactor {

	public DownloadAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get base asset folder
		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, false);
		String downloadPath = assetFolder;
		// create path for a zip file
		String randomKey = UUID.randomUUID().toString();
		String OUTPUT_PATH = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "export"
				+ DIR_SEPARATOR + "ZIPs";
		String zipPath = OUTPUT_PATH + DIR_SEPARATOR + this.insight.getInsightName() + ".zip";
		// if a specific file is specified for download
		String relativeAssetPath = keyValue.get(keysToGet[0]);
		if (relativeAssetPath != null && relativeAssetPath.length() > 0) {
			downloadPath = assetFolder + DIR_SEPARATOR + relativeAssetPath;
			File assetToDownload = new File(downloadPath);
			if (!assetToDownload.exists()) {
				NounMetadata error = NounMetadata.getErrorNounMessage("File does not exist");
				SemossPixelException exception = new SemossPixelException(error);
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
			// if the file to download is a dir it will be zipped
			if (assetToDownload.isDirectory()) {
				// zip asset folder to the zip path
				zipFolder(downloadPath, zipPath);
				// set download path to the zipPath
				downloadPath = zipPath;
			}

		} else {
			// zip asset folder to the zip path
			zipFolder(assetFolder, zipPath);
			downloadPath = zipPath;
		}
		this.insight.addExportFile(randomKey, downloadPath);
		return new NounMetadata(randomKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);

	}

	private void zipFolder(String folder, String downloadPath) {
		ZipOutputStream zos = null;
		try {
			zos = ZipUtils.zipFolder(folder, downloadPath);
		} catch (IOException e) {
			e.printStackTrace();
			NounMetadata error = NounMetadata.getErrorNounMessage("Unable to zip and download");
			SemossPixelException exception = new SemossPixelException(error);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		} finally {
			try {
				if (zos != null) {
					zos.flush();
					zos.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
				NounMetadata error = NounMetadata.getErrorNounMessage("Unable to zip and download");
				SemossPixelException exception = new SemossPixelException(error);
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
		}
	}

}