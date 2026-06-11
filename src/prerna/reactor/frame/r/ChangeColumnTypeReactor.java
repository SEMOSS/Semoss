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
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ChangeColumnTypeReactor extends AbstractRFrameReactor {

	/**
	 * <p>
	 * This reactor changes the data type of an existing column
	 * </p>
	 *
	 * <p>
	 * The inputs to the reactor are:
	 * </p>
	 * <ul>
	 * <li>the column to update</li>
	 * <li>the desired column type</li>
	 * </ul>
	 */

	public ChangeColumnTypeReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.DATA_TYPE.getKey(),
				ReactorKeysEnum.ADDITIONAL_DATA_TYPE.getKey(), "format" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		// get table name
		String table = frame.getName();
		// get inputs
		String column = this.keyValue.get(this.keysToGet[0]);
		if (column == null) {
			throw new IllegalArgumentException("Need to define " + ReactorKeysEnum.COLUMN.getKey());
		}
		if (column.contains("__")) {
			String[] split = column.split("__");
			column = split[1];
		}
		String newType = this.keyValue.get(this.keysToGet[1]);
		if (newType == null) {
			throw new IllegalArgumentException("Need to define " + ReactorKeysEnum.DATA_TYPE.getKey());
		}
		newType = SemossDataType.convertStringToDataType(newType).toString();

		String additionalDataType = this.keyValue.get(this.keysToGet[2]);

		OwlTemporalEngineMeta metadata = this.getFrame().getMetaData();
		String dataType = metadata.getHeaderTypeAsString(table + "__" + column);
		if (dataType == null) {
			return getWarning("Frame is out of sync / No Such Column. Cannot perform this operation");
		}

		// check if there is a new dataType
		if (!dataType.equals(newType)) {
			// define the r script to execute
			// script depends on the new data type
			String script = null;
			if (Utility.isStringType(newType)) {
				// df$column <- as.character(df$column);
				String df = getColumnType(table, column);
				// create temp table without scientific format for numeric
				// columns
				if (df.equalsIgnoreCase("numeric")) {
					String tempTable = Utility.getRandomString(6);
					StringBuilder rsb = new StringBuilder();
					rsb.append(tempTable + " <- format(" + table + "$" + column + ", scientific = FALSE); ");
					rsb.append(table + "$" + column + "<- " + tempTable + ";");
					rsb.append(table + "$" + column + " <- as.character(" + table + "$" + column + ");");
					// perform variable cleanup
					rsb.append("rm(" + tempTable + ");");
					rsb.append("gc();");
					this.rJavaTranslator.runR(rsb.toString());
					this.addExecutedCode(rsb.toString());
				} else {
					script = table + "$" + column + " <- as.character(" + table + "$" + column + ");";
					frame.executeRScript(script);
					this.addExecutedCode(script);
				}
			} else if (newType.equalsIgnoreCase("factor")) {
				// df$column <- as.factor(df$column);
				script = table + "$" + column + " <- as.factor(" + table + "$" + column + ");";
				frame.executeRScript(script);
				this.addExecutedCode(script);
			} else if (Utility.isDoubleType(newType)) {
				// r script syntax cleaning characters with regex
				script = table + "$" + column + " <- as.numeric(gsub('[^-\\\\.0-9]', '', " + table + "$" + column
						+ "));";
				frame.executeRScript(script);
				this.addExecutedCode(script);
			} else if (Utility.isDateType(newType)) {
				// we have a different script to run if it is a str to date
				// conversion
				// define date format
				String dateFormat = this.keyValue.get(this.keysToGet[3]);
				if (dateFormat == null) {
					dateFormat = "%Y-%m-%d";
				} else {
					// convert from java format to R
					dateFormat = RSyntaxHelper.translateJavaRDateTimeFormat(dateFormat);
				}
				script = RSyntaxHelper.alterColumnTypeToDate(table, dateFormat, column);
				this.rJavaTranslator.runR(script);
				this.addExecutedCode(script);
			}
			// update the metadata
			metadata.modifyDataTypeToProperty(table + "__" + column, table, newType);
		}

		if (additionalDataType != null && !additionalDataType.isEmpty()) {
			metadata.modifyAdditionalDataTypeToProperty(table + "__" + column, table, newType);
		}

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
	}
}
