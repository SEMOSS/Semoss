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
package prerna.reactor.qs.source;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.query.querystruct.CsvQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class TextInputReactor extends AbstractQueryStructReactor {

	private static final Logger classLogger = LogManager.getLogger(TextInputReactor.class);

	// keys to get inputs from pixel command
	private static final String FILE_INFO = "fileData";
	private static final String DATA_TYPES = "dataTypeMap";
	private static final String DELIMITER = "delim";

	/**
	 * TextInput args
	 * 
	 * FILE_INFO=["fileInfo"] DELIMITER = ["delimiter"]
	 * 
	 * to set dataTypes dataTypesMap = [{"column", "type"}]
	 */

	@Override
	protected SelectQueryStruct createQueryStruct() {
		CsvQueryStruct qs = null;

		// get inputs
		Map<String, String> dataTypes = getDataTypes();
		String fileInfo = getFileInfo();

		// write the file on disk
		Date date = new Date();
		String modifiedDate = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSSS").format(date);
		String fileLocation = Utility.getBaseFolder() + DIR_SEPARATOR + "PastedData" + modifiedDate + ".csv";
		File file = new File(fileLocation);
		try (FileWriter fw = new FileWriter(file)) {
			fw.write(fileInfo);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}

		// set csv qs
		char delimiter = getDelimiter();
		qs = new CsvQueryStruct();
		qs.setFilePath(fileLocation);
		qs.setDelimiter(delimiter);
		qs.setColumnTypes(dataTypes);
		qs.merge(this.qs);
		return qs;
	}

	/**************************************************************************************************
	 ************************************* INPUT METHODS***********************************************
	 **************************************************************************************************/

	private String getFileInfo() {
		GenRowStruct fGrs = this.store.getGenRowStruct(FILE_INFO);
		String fileInfo = null;
		if (fGrs != null && !fGrs.isEmpty()) {
			fileInfo = fGrs.get(0).toString();
		} else {
			throw new IllegalArgumentException(
					"Need to specify " + FILE_INFO + "=[\"<encode>fileData</encode>\"] in pixel command");
		}
		return fileInfo;
	}

	private Map<String, String> getDataTypes() {
		GenRowStruct dataTypeGRS = this.store.getGenRowStruct(DATA_TYPES);
		Map<String, String> dataTypes = null;
		if (dataTypeGRS != null) {
			NounMetadata dataNoun = dataTypeGRS.getNoun(0);
			dataTypes = (Map<String, String>) dataNoun.getValue();
		}
		return dataTypes;
	}

	private char getDelimiter() {
		GenRowStruct delimGRS = this.store.getGenRowStruct(DELIMITER);
		String delimiter = "";
		char delim = ','; // default
		NounMetadata instanceIndexNoun;

		if (delimGRS != null) {
			instanceIndexNoun = delimGRS.getNoun(0);
			delimiter = (String) instanceIndexNoun.getValue();
		} else {
			throw new IllegalArgumentException("Need to specify " + DELIMITER + "=[delimiter] in pixel command");
		}

		// get char from input string
		if (delimiter.length() > 0) {
			delim = delimiter.charAt(0);
		}

		return delim;
	}
}