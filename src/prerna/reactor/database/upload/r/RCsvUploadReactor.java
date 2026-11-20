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
package prerna.reactor.database.upload.r;

import java.io.File;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import prerna.algorithm.api.SemossDataType;
import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.engine.impl.r.RNativeEngine;
import prerna.poi.main.helper.CSVFileHelper;
import prerna.reactor.database.upload.AbstractDatabaseUploadFileReactor;
import prerna.reactor.database.upload.rdbms.RDBMSEngineCreationHelper;
import prerna.util.UploadInputUtility;
import prerna.util.UploadUtilities;
import prerna.util.Utility;

public class RCsvUploadReactor extends AbstractDatabaseUploadFileReactor {

	private CSVFileHelper helper;

	public RCsvUploadReactor() {
		this.keysToGet = new String[] { UploadInputUtility.DATABASE, UploadInputUtility.FILE_PATH,
				UploadInputUtility.DELIMITER, UploadInputUtility.DATA_TYPE_MAP, UploadInputUtility.NEW_HEADERS,
				UploadInputUtility.ADDITIONAL_DATA_TYPES };
	}

	@Override
	public String getReactorDescription() {
		return "Uploads a delimited file (CSV, TSV, etc.) to create a new database backed by an R native engine. "
				+ "The data is loaded as a single flat table (one concept and its properties).";
	}

	@Override
	public void generateNewDatabase(User user, String newDatabaseName, String filePath) throws Exception {
		// grab inputs passed in
		final String delimiter = UploadInputUtility.getDelimiter(this.store);
		Map<String, String> dataTypesMap = UploadInputUtility.getCsvDataTypeMap(this.store);
		Map<String, String> newHeaders = UploadInputUtility.getNewCsvHeaders(this.store);
		Map<String, String> additionalDataTypeMap = UploadInputUtility.getAdditionalCsvDataTypes(this.store);
		File uploadFile = new File(filePath);
		String fileName = FilenameUtils.getBaseName(filePath);
		// TODO do we still need this????
		if (fileName.contains("_____UNIQUE")) {
			// ... yeah, this is not intuitive at all,
			// but I add a timestamp at the end to make sure every file is unique
			// but i want to remove it so things are "pretty"
			fileName = fileName.substring(0, fileName.indexOf("_____UNIQUE"));
		}

		int stepCounter = 1;
		logger.info("{}. Create smss file for database...", stepCounter);
		File owlFile = UploadUtilities.generateOwlFile(IEngine.CATALOG_TYPE.DATABASE, this.databaseId, newDatabaseName);
		this.tempSmss = UploadUtilities.createTemporaryRSmss(this.databaseId, newDatabaseName, owlFile, fileName,
				newHeaders, dataTypesMap, additionalDataTypeMap);
		UploadUtilities.addEngineToDIHelperToIgnoreEngineWatchers(this.databaseId, this.tempSmss.getAbsolutePath());
		logger.info("{}. Complete", stepCounter);
		stepCounter++;

		logger.info("{}. Parse data types...", stepCounter);
		this.helper = UploadUtilities.getHelper(filePath, delimiter, dataTypesMap, newHeaders);
		// parse the information
		Object[] headerTypesArr = UploadUtilities.getHeadersAndTypes(this.helper, dataTypesMap, additionalDataTypeMap);
		String[] headers = (String[]) headerTypesArr[0];
		SemossDataType[] types = (SemossDataType[]) headerTypesArr[1];
		String[] additionalTypes = (String[]) headerTypesArr[2];
		logger.info("{}. Complete", stepCounter);
		stepCounter++;

		logger.info("{}. Start generating database metadata", stepCounter);
		WriteOWLEngine owlEngine = this.database.getOWLEngineFactory().getWriteOWL();

		// table name is the file name
		String tableName = RDBMSEngineCreationHelper.cleanTableName(fileName).toUpperCase();
		// add the table
		owlEngine.addConcept(tableName, null, null);
		// add the props
		for (int i = 0; i < headers.length; i++) {
			owlEngine.addProp(tableName, headers[i], types[i].toString(), additionalTypes[i]);
		}
		// add descriptions and logical names
		UploadUtilities.insertFlatOwlMetadata(owlEngine, tableName, headers,
				UploadInputUtility.getCsvDescriptions(this.store), UploadInputUtility.getCsvLogicalNames(this.store));
		owlEngine.commit();
		owlEngine.export();
		owlEngine.close();
		logger.info("{}. Complete", stepCounter);
		stepCounter++;

		// move file
		File dataFile = SmssUtilities
				.getDataFile(Utility.loadProperties(Utility.normalizePath(this.tempSmss.getAbsolutePath())));
		FileUtils.copyFile(uploadFile, dataFile);

		logger.info("{}. Create database store...", stepCounter);
		this.database = new RNativeEngine();
		this.database.open(this.tempSmss.getAbsolutePath());
		logger.info("{}. Complete", stepCounter);
		stepCounter++;
	}

	@Override
	public void addToExistingDatabase(String filePath) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void closeFileHelpers() {
		if (this.helper != null) {
			this.helper.clear();
		}
	}

}
