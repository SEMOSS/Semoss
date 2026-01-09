package prerna.cache;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Constants;

public interface ICache {

	static final Logger classLogger = LogManager.getLogger(ICache.class);

	/**
	 * Deletes a folder and all its sub-directories
	 * 
	 * @param folderLocation The location of the folder
	 */
	static void deleteFolder(String folderLocation) {
		File folder = new File(folderLocation);
		deleteFolder(folder);
	}

	/**
	 * Deletes a folder and all its sub-directories
	 * 
	 * @param folderLocation The location of the folder
	 */
	static void deleteFolder(File folder) {
		if (folder.exists() && folder.isDirectory()) {
			try {
				FileUtils.forceDelete(folder);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
	}

	/**
	 * Deletes a file
	 * 
	 * @param file The File object to delete
	 */
	static void deleteFile(File file) {
		if (file.exists() && file.isFile()) {
			try {
				FileUtils.forceDelete(file);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
	}

}
