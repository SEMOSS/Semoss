package prerna.reactor.vector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.vector.AbstractVectorDatabaseEngine;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class FileDownloadReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(FileDownloadReactor.class);

	private String engineId;
	private String downloadKey;

	public FileDownloadReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), "filenames",
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		this.engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Vector db " + engineId + " does not exist or user does not have access to this engine");
		}

		List<String> fileNames = getFiles();

		try {
			downloadKey = getDownload(fileNames);
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(
					"Error occurred attempting to delete the files. Detailed message = " + e.getMessage());
		}

		return new NounMetadata(downloadKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}

	public String getDownload(List<String> fileNameList) {

		String engineId = this.keyValue.get(this.keysToGet[0]);

		IEngine engine = Utility.getEngine(engineId);
		String engineName = engine.getEngineName();
		String thisEngineDir = EngineUtility.getSpecificEngineBaseFolder(IEngine.CATALOG_TYPE.VECTOR, engineId,
				engineName) + DIR_SEPARATOR + "schema" + DIR_SEPARATOR + "default" + DIR_SEPARATOR
				+ AbstractVectorDatabaseEngine.DOCUMENTS_FOLDER_NAME;
		String engineNameAndId = SmssUtilities.getUniqueName(engineName, engineId);
		String outputDir = this.insight.getInsightFolder();
		String outFilePath = null;

		FileOutputStream fileoutStream = null;
		ZipOutputStream zos = null;
		try {

			if (fileNameList.size() == 1) {
				// Logic for single file download
				outFilePath = thisEngineDir + DIR_SEPARATOR + fileNameList.get(0);
				fileoutStream = new FileOutputStream(outFilePath);
				File fileToDownload = new File(outFilePath);
				FileInputStream fis = new FileInputStream(fileToDownload);
				try {
					byte[] buffer = new byte[1024];
					int length;
					while ((length = fis.read(buffer)) > 0) {
						fileoutStream.write(buffer, 0, length);
					}
				} finally {
					try {
						if (fis != null) {
							fis.close();
						}
					} catch (IOException e) {
						logger.error(Constants.STACKTRACE, e);
					}
				}
			} else if (fileNameList.size() > 1) {

				// Logic for multifile download as Zip
				outFilePath = outputDir + DIR_SEPARATOR + engineNameAndId + "_engine.zip";
				fileoutStream = new FileOutputStream(outFilePath);
				zos = new ZipOutputStream(fileoutStream);
				for (String filename : fileNameList) {
					File filetozip = new File(thisEngineDir + DIR_SEPARATOR + filename);
					ZipUtils.addToZipFile(filetozip, zos);
				}
			} else {
				logger.error(Constants.STACKTRACE, "Kindly provide a valid filename to download");
				throw new SemossPixelException("Kindly provide a valid filename to download ");
			}
		} catch (Exception e) {
			logger.info("Error occurred on download engine");
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException(
					"Error occurred while downloding file. Detailed message = " + e.getMessage());
		} finally {
			try {
				if (zos != null) {
					zos.flush();
					zos.close();
				}
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
			try {
				if (fileoutStream != null) {
					fileoutStream.close();
				}
			} catch (IOException e) {
				logger.error(Constants.STACKTRACE, e);
			}
		}

		this.downloadKey = UUID.randomUUID().toString();

		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setDeleteOnInsightClose(true);
		insightFile.setFilePath(outFilePath);
		this.insight.addExportFile(downloadKey, insightFile);

		return downloadKey;

	}

	/**
	 * 
	 * @return list of files to download
	 */
	public List<String> getFiles() {
		List<String> filePaths = new ArrayList<>();

		// see if added as key
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				filePaths.add(grs.get(i).toString());
			}
			return filePaths;
		}

		// no key is added, grab all inputs
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			filePaths.add(this.curRow.get(i).toString());
		}
		return filePaths;
	}

}
