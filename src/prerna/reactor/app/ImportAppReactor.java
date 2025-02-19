package prerna.reactor.app;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.reactor.project.UploadProjectAppReactor;
import prerna.reactor.project.UploadProjectReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;
import prerna.util.ZipUtils;

public class ImportAppReactor extends AbstractReactor {
	
	private static final Logger classLogger = LogManager.getLogger(ImportAppReactor.class);
	private static final String CLASS_NAME = ImportAppReactor.class.getName();

	public ImportAppReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey(), ReactorKeysEnum.GLOBAL.getKey()};
	}
	
	
	public NounMetadata execute() {
		
		organizeKeys();
		Logger logger = this.getLogger(CLASS_NAME);
		int step = 1;
		String zipFilePath = this.keyValue.get(this.keysToGet[0]);
		String parentDirectory = zipFilePath.substring(0, zipFilePath.lastIndexOf("\\") + 1);
		
		// do we want this project to be accessible to everyone
		//boolean global = Boolean.parseBoolean(this.keyValue.get(ReactorKeysEnum.GLOBAL.getKey())+"");
		
		//User auth
		
		// creating a temp folder to unzip assets folder
		String randomIdAsDir = UUID.randomUUID().toString();
		String randomTempUnzipFolderPath = parentDirectory + randomIdAsDir;
		File randomTempUnzipF = new File(randomTempUnzipFolderPath);
		
		Map<String, List<String>> filesAdded = new HashMap<>();
		File[] fileList = null;
		boolean error = false;
		boolean unzipped = false;
		
		try {
			logger.info(step + ") Unzipping project");
			filesAdded = ZipUtils.unzip(zipFilePath, randomTempUnzipFolderPath);
			fileList = randomTempUnzipF.listFiles();
			logger.info(step + ") Done");
			step++;
			unzipped = true;
		} catch (Exception e) {
			error = true;
			logger.error(Constants.STACKTRACE, e);
			throw new SemossPixelException("Error occurred while unzipping the files", false);
		}  finally {
			if (unzipped) {
				cleanUpFolders(fileList[0]);
				logger.info(step + ")assets deleted");
			}
		}
		
		boolean ImportResult = false;
		return new NounMetadata(ImportResult, PixelDataType.BOOLEAN); 
	}
	private void cleanUpFolders(File... fileToDelete) {
		for(File f : fileToDelete) {
			if(f != null && f.exists()) {
				try {
					FileUtils.forceDelete(f);
				} catch (IOException e) {
					classLogger.warn("Error on clean up attempting to delete " + f.getAbsolutePath());
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
}
