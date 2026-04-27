/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.reactor.vector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tika.Tika;
import org.apache.tika.metadata.Metadata;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.vector.FileEmbeddingStatus;
import prerna.reactor.AbstractReactor;
import prerna.reactor.vector.VectorDatabaseParamOptionsEnum.CreateEmbeddingsParamOptions;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class CreateEmbeddingsFromVectorCSVFileReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(CreateEmbeddingsFromVectorCSVFileReactor.class);

	private final String PATH_TO_UNZIP_FILES = "zipFileExtractFolder";
	private final String FILE_PATHS_KEY = "filePaths";

	public CreateEmbeddingsFromVectorCSVFileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), FILE_PATHS_KEY, ReactorKeysEnum.SPACE.getKey(),
				ReactorKeysEnum.PARAM_VALUES_MAP.getKey() };
		this.keyRequired = new int[] { 1, 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Vector db " + engineId + " does not exist or user does not have access to this engine");
		}

		IVectorDatabaseEngine vectorDatabase = Utility.getVectorDatabase(engineId);
		if (vectorDatabase == null) {
			throw new SemossPixelException("Unable to find engine");
		}

		Map<String, Object> paramMap = getMap();
		if (paramMap == null) {
			paramMap = new HashMap<String, Object>();
		}

		// check user has access to any embedding models as well
		// this actually throws an error
		// but will wrap in if statement just in case
		if (!vectorDatabase.userCanAccessEmbeddingModels(this.insight.getUser())) {
			throw new IllegalArgumentException("User does not have access to all the vector database dependent models");
		}

		// send the insight so it can be used with IModelEngine call
		paramMap.put(Constants.INSIGHT, this.insight);

		String rootFolder = getRootFolder();
		// this is coming from an insight so i assume its just the file names
		List<String> validFiles = new ArrayList<>();
		List<String> invalidFiles = new ArrayList<>();
		List<FileEmbeddingStatus> fileStatusList;
		try {
			getFiles(rootFolder, validFiles, invalidFiles);
			if (validFiles.isEmpty()) {
				throw new IllegalArgumentException(
						"Please provide valid input files using \"filePaths\". This method assumes a valid csv file format");
			}

			for (String filePath : validFiles) {
				File file = new File(Utility.normalizePath(filePath));
				// Check if the file exists
				if (!file.exists()) {
					throw new IllegalArgumentException("File path for " + file.getName() + " does not exist.");
				}
			}

			fileStatusList = vectorDatabase.addEmbeddings(validFiles, insight, paramMap);
		} catch (Exception e) {
			classLogger.error("Error creating embeddings from vector CSV files for engine {}", engineId, e);
			throw new IllegalArgumentException("The following exception occured: " + e.getMessage());
		}

		NounMetadata noun = new NounMetadata(fileStatusList, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.OPERATION);
		if (!invalidFiles.isEmpty()) {
			List<String> invalidFileNamesRelative = new ArrayList<>(invalidFiles.size());
			for (String invalidF : invalidFiles) {
				invalidFileNamesRelative.add(invalidF.replace(rootFolder, ""));
			}
			noun.addAdditionalReturn(NounMetadata
					.getWarningNounMessage("Unable to upload " + String.join(", ", invalidFileNamesRelative)));
		}
		return noun;
	}

	/**
	 * Get the map from the paramValues noun store
	 * 
	 * @return list of engines to delete
	 */
	private Map<String, Object> getMap() {
		GenRowStruct mapGrs = this.store.getGenRowStruct(ReactorKeysEnum.PARAM_VALUES_MAP.getKey());
		if (mapGrs != null && !mapGrs.isEmpty()) {
			List<NounMetadata> mapInputs = mapGrs.getNounsOfType(PixelDataType.MAP);
			if (mapInputs != null && !mapInputs.isEmpty()) {
				return (Map<String, Object>) mapInputs.get(0).getValue();
			}
		}
		List<NounMetadata> mapInputs = this.curRow.getNounsOfType(PixelDataType.MAP);
		if (mapInputs != null && !mapInputs.isEmpty()) {
			return (Map<String, Object>) mapInputs.get(0).getValue();
		}

		return null;
	}

	/**
	 * @param insightFolder
	 * @param validFiles
	 * @param invalidFiles
	 * @return
	 * @throws IOException
	 */
	private void getFiles(String insightFolder, List<String> validFiles, List<String> invalidFiles) throws IOException {
		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[1]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				String filePath = insightFolder + "/" + grs.get(i).toString();
				if (isZipFile(filePath)) {
					String zipFileLocation = filePath.replace('\\', '/');
					File zipFileExtractFolder = new File(insightFolder, PATH_TO_UNZIP_FILES);
					unzipAndFilter(zipFileLocation, zipFileExtractFolder.getAbsolutePath(), validFiles, invalidFiles);
				} else {
					// String filePath = destDirectory + File.separator + entry.getName();
					if (isSupportedFileType(filePath)) {
						validFiles.add(filePath);
					} else {
						invalidFiles.add(filePath);
					}
				}
			}
		}
	}

	/**
	 * Recursively go through all the zips, directories and files in a zip file and
	 * save the paths of valid file types
	 * 
	 * @param zipFilePath
	 * @param destDirectory
	 * @param validFiles
	 * @param invalidFiles
	 * @throws IOException
	 */
	private void unzipAndFilter(String zipFilePath, String destDirectory, List<String> validFiles,
			List<String> invalidFiles) throws IOException {
		File destDir = new File(Utility.normalizePath(destDirectory));
		if (!destDir.exists()) {
			destDir.mkdir();
		}

		try (ZipInputStream zipIn = new ZipInputStream(new FileInputStream(Utility.normalizePath(zipFilePath)))) {
			ZipEntry entry = zipIn.getNextEntry();

			while (entry != null) {
				String filePath = destDirectory + "/" + entry.getName();
				if (!entry.isDirectory() && isSupportedFileType(filePath)) {
					if (isSupportedFileType(filePath)) {
						extractFile(zipIn, filePath);
						validFiles.add(filePath);
					} else {
						invalidFiles.add(filePath);
					}
				} else if (entry.isDirectory()) {
					File dir = new File(Utility.normalizePath(filePath));
					dir.mkdirs();
				} else if (isZipFile(filePath)) {
					// Handle nested zip file
					this.extractFile(zipIn, filePath);

					// Check if the entry is not in the root directory
					String parentPath = null;
					if (filePath.contains("/")) { // ZIP entries use "/" as a separator
						parentPath = filePath.substring(0, filePath.lastIndexOf('/'));
					}

					// Extract the last part of the path (file name + extension)
					String fileNameWithExtension = filePath.contains("/")
							? filePath.substring(filePath.lastIndexOf('/') + 1)
							: filePath;

					// Remove the extension
					String baseName = fileNameWithExtension.contains(".")
							? fileNameWithExtension.substring(0, fileNameWithExtension.lastIndexOf('.'))
							: fileNameWithExtension;

					unzipAndFilter(filePath, parentPath + "/" + baseName, validFiles, invalidFiles);
				}

				zipIn.closeEntry();
				entry = zipIn.getNextEntry();
			}
		}
	}

	/**
	 * 
	 * @return
	 */
	private String getRootFolder() {
		String space = null;
		GenRowStruct spaceGrs = store.getGenRowStruct(ReactorKeysEnum.SPACE.getKey());
		if (spaceGrs != null && !spaceGrs.isEmpty()) {
			space = spaceGrs.get(0).toString();
		}

		return AssetUtility.getRootFolderPath(this.insight, space, false);
	}

	/**
	 * 
	 * @param zipIn
	 * @param filePath
	 * @throws IOException
	 */
	private void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
		try (FileOutputStream fos = new FileOutputStream(Utility.normalizePath(filePath))) {
			byte[] buffer = new byte[1024];
			int bytesRead;
			while ((bytesRead = zipIn.read(buffer)) != -1) {
				fos.write(buffer, 0, bytesRead);
			}
		}
	}

	/**
	 * 
	 * @param filePath
	 * @return
	 */
	private boolean isSupportedFileType(String filePath) {
		// Find the last index of '.'
		int dotIndex = filePath.lastIndexOf('.');
		if (dotIndex > 0 && dotIndex < filePath.length() - 1) {
			String extension = filePath.substring(dotIndex + 1).toLowerCase();
			return extension.equals("csv");
		}

		return false;
	}

	/**
	 * 
	 * @param filePath
	 * @return
	 */
	private boolean isZipFile(String filePath) {
		// Find the last index of '.'
		int dotIndex = filePath.lastIndexOf('.');

		if (dotIndex > 0 && dotIndex < filePath.length() - 1) {
			// Extract the extension and convert it to lower case
			String extension = filePath.substring(dotIndex + 1).toLowerCase();

			return extension.equals("zip");
		} else {
			// do a mime type check
			Tika tika = new Tika();
			File file = new File(Utility.normalizePath(filePath));
			try (FileInputStream inputstream = new FileInputStream(file)) {
				String mimeType = tika.detect(inputstream, new Metadata());

				if (mimeType != null) {
					if (mimeType.equalsIgnoreCase("application/zip")) {
						return true;
					}
				}

				return false;
			} catch (IOException e) {
				classLogger.error(Constants.ERROR_MESSAGE, e);
				return false;
			}
		}
	}

	@Override
	public String getReactorDescription() {
		return """
				Creates embeddings from VectorCSVFile-formatted csv files and adds them to a vector database. \
				Accepts csv files directly or zip archives containing csv files. \
				Expected csv headers are: Source, Modality, Divider, Part, Tokens, Content. \
				Header names should match exactly as shown (case-sensitive). \
				Standard CSV quoting is supported, but quotes are optional unless a value contains commas, newlines, or quotes. \
				`Tokens` should be numeric, and `Modality` is typically `text` but can be `image`, `audio`, `video`, etc. \
				Relative file paths are resolved from the current insight/room space by default, \
				or from another space when the optional `space` parameter is provided.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The vector database engine ID to add embeddings into.";
		} else if (key.equals(FILE_PATHS_KEY)) {
			return """
					The list of csv file paths (or zip files containing csv files) to process. \
					Each csv should include headers: Source, Modality, Divider, Part, Tokens, Content. \
					Use these header names exactly (case-sensitive). \
					CSV field quotes are optional unless needed for commas/newlines/quotes. \
					Paths are resolved relative to the selected `space` \
					(or current insight when `space` is omitted).\
					""";
		} else if (key.equals(ReactorKeysEnum.SPACE.getKey())) {
			return """
					Optional space used to resolve relative file paths. \
					When omitted, files are resolved from the current insight/room space. \
					Pass a project/app UUID to resolve from that app/project folder, or pass `user` for user space.\
					""";
		} else if (key.equals(ReactorKeysEnum.PARAM_VALUES_MAP.getKey())) {
			StringBuilder finalDescription = new StringBuilder("Param Options depend on the engine implementation");

			for (CreateEmbeddingsParamOptions entry : CreateEmbeddingsParamOptions.values()) {
				finalDescription.append("\n").append("\t\t\t\t\t")
						.append(entry.getVectorDbType().getVectorDatabaseName()).append(":");

				for (String paramKey : entry.getParamOptionsKeys()) {
					finalDescription.append("\n").append("\t\t\t\t\t\t").append(paramKey).append("\t").append("-")
							.append("\t").append("(").append(entry.getRequirementStatus(paramKey)).append(")")
							.append(" ").append(VectorDatabaseParamOptionsEnum.getDescriptionFromKey(paramKey));
				}
			}
			return finalDescription.toString();
		}

		return super.getDescriptionForKey(key);
	}
}
