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
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AddColumnReactor extends AbstractRFrameReactor {

	/**
	 * <p>
	 * This reactor adds an empty column to the frame
	 * </p>
	 *
	 * <p>
	 * The inputs to the reactor are:
	 * </p>
	 * <ul>
	 * <li>the name for the new column</li>
	 * <li>the new column type</li>
	 * </ul>
	 */

	public AddColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NEW_COLUMN.getKey(), ReactorKeysEnum.DATA_TYPE.getKey(),
				ReactorKeysEnum.ADDITIONAL_DATA_TYPE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		// get new column name from the gen row struct
		String newColName = this.keyValue.get(this.keysToGet[0]);
		if (newColName == null || newColName.isEmpty()) {
			throw new IllegalArgumentException("Need to define the new column name");
		}

		// get the column type and standardize
		String colType = this.keyValue.get(this.keysToGet[1]);
		if (colType == null) {
			colType = SemossDataType.convertStringToDataType("STRING").toString();
		}
		String adtlDataType = this.keyValue.get(this.keysToGet[2]);

		String table = frame.getName();
		// clean colName
		if (newColName.contains("__")) {
			String[] split = newColName.split("__");
			newColName = split[1];
		}
		// clean the column name to ensure that it is valid
		newColName = getCleanNewColName(frame, newColName);

		// define the script to be executed;
		// this assigns a new column name with no data in columns
		String script = table + "$" + newColName + " <- \"\" ";
		// execute the r script
		frame.executeRScript(script);
		this.addExecutedCode(script);

		// update the metadata to include this new column
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		metaData.addProperty(table, table + "__" + newColName);
		metaData.setAliasToProperty(table + "__" + newColName, newColName);
		if (adtlDataType != null && !adtlDataType.isEmpty()) {
			metaData.setAddtlDataTypeToProperty(frame.getName() + "__" + newColName, adtlDataType);
		}
		// temp table used to assign a data type to the new column
		String tempTable = null;
		if (Utility.isNumericType(colType)) {
			// update the metadata depending on the data type
			metaData.setDataTypeToProperty(table + "__" + newColName, SemossDataType.DOUBLE.toString());
			tempTable = Utility.getRandomString(6);
			script = tempTable + " <- as.numeric(" + table + "$" + newColName + ")";
			this.addExecutedCode(script);
			frame.executeRScript(script);
			script = table + "$" + newColName + "<-" + tempTable;
			frame.executeRScript(script);
			this.addExecutedCode(script);
		} else if (Utility.isDateType(colType)) {
			metaData.setDataTypeToProperty(table + "__" + newColName, SemossDataType.DATE.toString());
			tempTable = Utility.getRandomString(6);
			String dateFormat = "%Y/%m/%d";
			script = tempTable + " <- as.Date(" + table + "$" + newColName + ", format='" + dateFormat + "')";
			this.addExecutedCode(script);
			frame.executeRScript(script);
			script = table + "$" + newColName + " <- " + tempTable;
			frame.executeRScript(script);
			this.addExecutedCode(script);
		} else {
			// if not a number or a date then assign to string
			metaData.setDataTypeToProperty(table + "__" + newColName, SemossDataType.STRING.toString());
		}

		// r temp variable clean up
		if (tempTable != null) {
			script = "rm(" + tempTable + ");";
			frame.executeRScript(script);
			this.addExecutedCode(script);
			script = "gc();";
			frame.executeRScript(script);
			this.addExecutedCode(script);
		}
		frame.syncHeaders();

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata(newColName));
		return retNoun;
	}
}
