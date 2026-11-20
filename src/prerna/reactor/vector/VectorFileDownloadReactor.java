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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.io.Files;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.ZipUtils;

public class VectorFileDownloadReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(VectorFileDownloadReactor.class);

	private final String FILE_NAMES = "fileNames";

	public VectorFileDownloadReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), FILE_NAMES };
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityEngineUtils.userCanViewEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException(
					"Vector db " + engineId + " does not exist or user does not have access to this engine");
		}

		List<String> fileNames = getFiles();
		if (fileNames == null || fileNames.isEmpty()) {
			throw new IllegalArgumentException("Must provide the key '" + FILE_NAMES + "' for the files to download");
		}
		try {
			return getDownload(engineId, fileNames);
		} catch (SemossPixelException e) {
			classLogger.error("Failed to process vector file download request: {}", e.getMessage(), e);
			throw e;
		} catch (Exception e) {
			classLogger.error("Failed to process vector file download request: {}", e.getMessage(), e);
			throw new IllegalArgumentException(
					"Error occurred attempting to download the files. Detailed message = " + e.getMessage());
		}
	}

	/**
	 * 
	 * @param fileNames
	 * @return
	 * @throws IOException
	 */
	private NounMetadata getDownload(String engineId, List<String> fileNames) throws IOException {
		String downloadKey = UUID.randomUUID().toString();

		IVectorDatabaseEngine vectorDb = Utility.getVectorDatabase(engineId);
		String engineName = vectorDb.getEngineName();
		String engineNameAndId = SmssUtilities.getUniqueName(engineName, engineId);

		String vectorDbDocumentFilePath = vectorDb.getDocumentsFilesPath(null);
		String outputDir = this.insight.getInsightFolder();
		String outFilePath = null;

		List<String> warnings = new ArrayList<>();

		try {
			if (fileNames.size() == 1) {
				String filepath = vectorDbDocumentFilePath + DIR_SEPARATOR + fileNames.get(0);
				File fileToCheck = new File(filepath);
				if (!fileToCheck.exists()) {
					throw new SemossPixelException(
							"File " + fileNames.get(0) + " does not exist in the vector db to download");
				}
				outFilePath = outputDir + DIR_SEPARATOR + fileNames.get(0);
				Files.copy(fileToCheck, new File(outFilePath));
			} else {
				outFilePath = outputDir + DIR_SEPARATOR + engineNameAndId + "_files.zip";
				try (FileOutputStream fos = new FileOutputStream(outFilePath);
						ZipOutputStream zos = new ZipOutputStream(fos);) {
					int fileExistsCount = 0;
					for (String fileName : fileNames) {
						File filetozip = new File(vectorDbDocumentFilePath + DIR_SEPARATOR + fileName);
						if (filetozip.exists()) {
							ZipUtils.addToZipFile(filetozip, zos);
							fileExistsCount++;
						} else {
							warnings.add(fileName);
						}
					}
					if (fileExistsCount == 0) {
						throw new SemossPixelException(
								"None of the files selected to download exist in the vector db to download");
					}
				}
			}
		} catch (IOException e) {
			classLogger.error("Failed to build vector file download package: {}", e.getMessage(), e);
			throw e;
		}

		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setDeleteOnInsightClose(true);
		insightFile.setFilePath(outFilePath);
		this.insight.addExportFile(downloadKey, insightFile);

		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING,
				PixelOperationType.FILE_DOWNLOAD);
		if (!warnings.isEmpty()) {
			retNoun.addAdditionalReturn(
					NounMetadata.getWarningNounMessage("Could not find some of the files to download: " + warnings));
		}
		return retNoun;
	}

	/**
	 * 
	 * @return list of files to download
	 */
	public List<String> getFiles() {
		List<String> filePaths = new ArrayList<>();

		// see if added as key
		GenRowStruct grs = this.store.getGenRowStruct(FILE_NAMES);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				filePaths.add(grs.get(i).toString());
			}
			return filePaths;
		}

		throw new IllegalArgumentException("Must pass in the file names to download");
	}

	@Override
	protected MCP_KEY_TYPE getKeyTypeForMCP(String key) {
		if (key.equals(FILE_NAMES)) {
			return MCP_KEY_TYPE.ARRAY;
		}
		return super.getKeyTypeForMCP(key);
	}

	@Override
	public String getReactorDescription() {
		return """
				Downloads original document files from a vector database. \
				Retrieves the actual source files that were uploaded to the vector database. \
				Downloads a single file if one file is specified, or a zip archive containing multiple files. \
				Returns a download key. If this is being called as an MCP, ignore the download key and alert the user it's been added to the room.\
				""";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(FILE_NAMES)) {
			return "The list of file names to download from the vector database";
		}
		return super.getDescriptionForKey(key);
	}

}
