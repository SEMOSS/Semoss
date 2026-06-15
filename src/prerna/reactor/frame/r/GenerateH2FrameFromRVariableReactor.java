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
package prerna.reactor.frame.r;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.r.RSyntaxHelper;
import prerna.ds.rdbms.h2.H2Frame;
import prerna.query.querystruct.CsvQueryStruct;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.reactor.imports.RdbmsImporter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GenerateH2FrameFromRVariableReactor extends AbstractRFrameReactor {

	/**
	 * <p>
	 * This reactor takes an r frame and synchronizes it to an h2 frame in semoss
	 * </p>
	 *
	 * <p>
	 * The inputs to the reactor are:
	 * </p>
	 * <ul>
	 * <li>r data table name</li>
	 * </ul>
	 */

	public GenerateH2FrameFromRVariableReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.VARIABLE.getKey(), ReactorKeysEnum.OVERRIDE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		// get rFrameName
		String varName = getStringFromKeyOrCurRow(ReactorKeysEnum.VARIABLE.getKey(), 0);
		H2Frame newTable;
		try {
			newTable = new H2Frame(varName);
		} catch (Exception e) {
			throw new IllegalArgumentException("Error occurred instaniating new grid frame");
		}

		// sync R dataframe to H2Frame
		syncFromR(this.rJavaTranslator, varName, newTable);
		if (getBoolean(ReactorKeysEnum.OVERRIDE.getKey(), true)) {
			this.insight.setDataMaker(newTable);
		}
		NounMetadata noun = new NounMetadata(newTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
		// add the alias as a noun by default
		if (varName != null && !varName.isEmpty()) {
			this.insight.getVarStore().put(varName, noun);
		}

		return noun;
	}

	/**
	 * 
	 * @param rFrameName
	 * @param frame
	 */
	public void syncFromR(AbstractRJavaTranslator rJavaTranslator, String rFrameName, H2Frame frame) {
		// generate the QS
		// set the column names and types
		rJavaTranslator.executeEmptyR(RSyntaxHelper.asDataTable(rFrameName, rFrameName));
		// recreate a new frame and set the frame name
		String[] colNames = rJavaTranslator.getColumns(rFrameName);
		rJavaTranslator.runR(RSyntaxHelper.cleanFrameHeaders(rFrameName, colNames));
		colNames = rJavaTranslator.getColumns(rFrameName);
		String[] colTypes = rJavaTranslator.getColumnTypes(rFrameName);
		// change r dataTypes such as dates, logicals, etc to be displayed as strings
		StringBuilder dataTypeConversion = new StringBuilder();
		for (int i = 0; i < colTypes.length; i++) {
			SemossDataType smssType = SemossDataType.convertStringToDataType(colTypes[i]);
			if (smssType == SemossDataType.INT || smssType == SemossDataType.DOUBLE) {
				dataTypeConversion.append(RSyntaxHelper.alterColumnTypeToNumeric(rFrameName, colNames[i]) + ";");
			}
			if (smssType == SemossDataType.STRING || smssType == SemossDataType.DATE) {
				dataTypeConversion.append(RSyntaxHelper.alterColumnTypeToCharacter(rFrameName, colNames[i]) + ";");
			}
		}
		if (dataTypeConversion.toString().length() > 0) {
			rJavaTranslator.runR(dataTypeConversion.toString());
		}

		if (colNames == null || colTypes == null) {
			throw new IllegalArgumentException(
					"Please make sure the variable " + rFrameName + " exists and can be a valid data.table object");
		}

		CsvQueryStruct qs = new CsvQueryStruct();
		qs.setSelectorsAndTypes(colNames, colTypes);

		// we will make a temp file
		String tempFileLocation = Utility.getInsightCacheDir() + "\\" + Utility.getCsvInsightCacheDir();
		tempFileLocation += "\\" + Utility.getRandomString(10) + ".csv";
		tempFileLocation = tempFileLocation.replace("\\", "/");
		rJavaTranslator.executeEmptyR("fwrite(" + rFrameName + ", file='" + tempFileLocation + "')");

		// iterate through file and insert values
		qs.setFilePath(tempFileLocation);
		RdbmsImporter importer = new RdbmsImporter(frame, qs);
		// importer will create the necessary meta information
		importer.insertData();
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.VARIABLE.getKey())) {
			return "Name of the r variable";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
