package prerna.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.io.Files;

import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.ReactorKeysEnum;

public class AddFileUtils {

	private static final Logger classLogger = LogManager.getLogger(AddFileUtils.class);
	private static final String FILE_PATHS_KEY = "filePaths";
	private static final String RESPONSE_MAP_FILE_PATH_KEY = "filePath";
	private static final String FILE_TYPE_JSON = "json";
	private static final String DOT_SEPARATOR = ".";
	private static final String DIR_SEPARATOR = "/";

	public static List<Map<String, String>> addFileToModel(NounStore store, String engineId, String engineName,
			Insight insight, IEngine.CATALOG_TYPE catalogType, String fileSuffix) {

		String ERROR_VALID_FILES = "Supported file type is %s";
		String ERROR_FILE_PATH = "File path for %s does not exist within the insight or project space.";
		List<String> validFiles = new ArrayList<>();
		List<String> invalidFiles = new ArrayList<>();
		String rootFolder = getRootFolder(store, insight);

		try {
			getFiles(store, rootFolder, validFiles, invalidFiles);
			if (validFiles.isEmpty()) {
				throw new IllegalArgumentException(String.format(ERROR_VALID_FILES, FILE_TYPE_JSON.toUpperCase()));
			}
			for (String filePath : validFiles) {
				File file = new File(Utility.normalizePath(filePath));
				// Check if the file exists
				if (!file.exists()) {
					throw new IllegalArgumentException(String.format(ERROR_FILE_PATH, file.getName()));
				}
			}

			List<Map<String, String>> addDocument = addDocument(catalogType, validFiles, insight, engineId, engineName,
					fileSuffix);
			return addDocument;
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException(e.getMessage());
		}
	}

	private static String getRootFolder(NounStore store, Insight insight) {
		String space = null;
		GenRowStruct spaceGrs = store.getNoun(ReactorKeysEnum.SPACE.getKey());
		if (spaceGrs != null && !spaceGrs.isEmpty()) {
			space = spaceGrs.get(0).toString();
		}

		return AssetUtility.getRootFolderPath(insight, space, false);
	}

	private static void getFiles(NounStore store, String rootFolder, List<String> validFiles, List<String> invalidFiles)
			throws IOException {
		GenRowStruct grs = store.getNoun(FILE_PATHS_KEY);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				String filePath = rootFolder + DIR_SEPARATOR + grs.get(i).toString();
				if (isSupportedFileType(filePath)) {
					validFiles.add(filePath);
				} else {
					invalidFiles.add(filePath);
				}
			}
		}
	}

	private static boolean isSupportedFileType(String filePath) {
		// Find the last index of '.'
		int dotIndex = filePath.lastIndexOf(DOT_SEPARATOR);

		if (dotIndex > 0 && dotIndex < filePath.length() - 1) {
			// Extract the extension and convert it to lower case
			String extension = filePath.substring(dotIndex + 1).toLowerCase();
			return extension.equals(FILE_TYPE_JSON);
		}
		return false;
	}

	private static List<Map<String, String>> addDocument(IEngine.CATALOG_TYPE catalogType, List<String> filePaths,
			Insight insight, String engineId, String engineName, String fileSuffix) throws Exception {
		String ERROR_UNABLE_TO_REMOVE = "Unable to remove previously created file for %s or move it to the document directory";
		Map<String, String> responseMap = null;
		List<Map<String, String>> responseList = new ArrayList<Map<String, String>>();
		String engineDir = EngineUtility.getSpecificEngineBaseFolder(catalogType, engineId, engineName);

		for (String fileName : filePaths) {
			responseMap = new HashMap<String, String>();
			File fileInInsightFolder = new File(Utility.normalizePath(fileName));

			// Double check that they are files and not directories
			if (!fileInInsightFolder.isFile()) {
				continue;
			}

			File destinationFile = new File(engineDir,
					fileInInsightFolder.getName().replace(DOT_SEPARATOR, fileSuffix + DOT_SEPARATOR));

			// Check if the destination file exists, and if so, delete it
			try {
				if (destinationFile.exists()) {
					FileUtils.forceDelete(destinationFile);
				}
				Files.copy(fileInInsightFolder, destinationFile);
				responseMap.put(RESPONSE_MAP_FILE_PATH_KEY, destinationFile.getPath());
				responseList.add(responseMap);
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException(String.format(ERROR_UNABLE_TO_REMOVE, destinationFile.getName()));
			}
		}
		return responseList;
	}
}
