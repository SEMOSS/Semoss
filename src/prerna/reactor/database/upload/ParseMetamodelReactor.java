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
package prerna.reactor.database.upload;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.poi.main.helper.CSVFileHelper;
import prerna.reactor.AbstractReactor;
import prerna.reactor.masterdatabase.util.GenerateMetamodelLayout;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.UploadInputUtility;

public class ParseMetamodelReactor extends AbstractReactor {
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public ParseMetamodelReactor() {
		this.keysToGet = new String[] { UploadInputUtility.FILE_PATH, UploadInputUtility.SPACE,
				UploadInputUtility.DELIMITER, UploadInputUtility.ROW_COUNT, UploadInputUtility.PROP_FILE };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String csvFilePath = UploadInputUtility.getFilePath(this.store, this.insight);
		if (!new File(csvFilePath).exists()) {
			throw new IllegalArgumentException("Unable to locate file");
		}
		String delimiter = UploadInputUtility.getDelimiter(this.store);
		char delim = delimiter.charAt(0);
		CSVFileHelper helper = new CSVFileHelper();
		helper.setDelimiter(delim);
		helper.parse(csvFilePath);
		return new NounMetadata(generateMetaModelFromProp(helper), PixelDataType.MAP);
	}

	/**
	 * Generates the Meta model data based on the definition of the prop file
	 */
	private Map<String, Object> generateMetaModelFromProp(CSVFileHelper helper) {
		Map<String, Object> metamodel = UploadInputUtility.getMetamodelFromPropFile(this.store, this.insight);
		if (metamodel == null) {
			String error = "Unable to read metamodel prop file.";
			NounMetadata noun = new NounMetadata(error, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException exception = new SemossPixelException(noun);
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// get file location and file name
		String filePath = helper.getFileLocation();
		String file = filePath.substring(filePath.lastIndexOf(DIR_SEPARATOR) + DIR_SEPARATOR.length(),
				filePath.lastIndexOf("."));
		try {
			file = file.substring(0, file.indexOf("_____UNIQUE"));
		} catch (Exception e) {
			// just in case that fails, this shouldnt because if its a filename
			// it should have a "."
			file = filePath.substring(filePath.lastIndexOf(DIR_SEPARATOR) + DIR_SEPARATOR.length(),
					filePath.lastIndexOf("."));
		}

		// store file path and file name to send to FE
		metamodel.put("fileLocation", filePath);
		metamodel.put("fileName", file);
		metamodel.put("headerModifications", helper.getChangedHeaders());
		// adding an empty map for consistency
		metamodel.put("additionalDataTypes", new HashMap<String, Object>());
		// position tables in metamodel to be spaced and not overlap
		Map<String, Map<String, Double>> nodePositionMap = GenerateMetamodelLayout.generateMetamodelPredictionLayout(
				(Map<String, List<String>>) metamodel.getOrDefault("nodeProp", new HashMap<>()),
				(List<Map<String, Object>>) metamodel.getOrDefault("relation", new ArrayList<>()));
		metamodel.put(Constants.POSITION_PROP, nodePositionMap);

		// need to close the helper
		helper.clear();

		return metamodel;
	}
}
