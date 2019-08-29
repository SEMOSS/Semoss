package prerna.util.git.reactors;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;
import java.util.zip.ZipOutputStream;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.ZipUtils;

public class DownloadAssetReactor extends AbstractReactor {

	public DownloadAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get base asset folder
		String assetFolder = this.insight.getInsightFolder();
		assetFolder = assetFolder.replaceAll("\\\\", "/");
		String randomKey = UUID.randomUUID().toString();
		String downloadPath = assetFolder;
		// if a specific file is specified for download
		if (keyValue.containsKey(keysToGet[0])) {
			downloadPath = assetFolder + "/" + keyValue.get(keysToGet[0]);
		} else {
			// zip and download asset dir
			String OUTPUT_PATH = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/export/ZIPs";
			downloadPath = OUTPUT_PATH + this.insight.getInsightId() + "__" + randomKey + ".zip";
			ZipOutputStream zos = null;
			try {
				zos = ZipUtils.zipFolder(assetFolder, downloadPath);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (zos != null) {
						zos.flush();
						zos.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		this.insight.addExportFile(randomKey, downloadPath);
		return new NounMetadata(randomKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);

	}

}