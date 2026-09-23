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
package prerna.reactor.frame.rdbms;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class CountIfReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(CountIfReactor.class);

	@Override
	public NounMetadata execute() {
		AbstractRdbmsFrame frame = (AbstractRdbmsFrame) getFrame();
		String newColumnName = getNewColumn();
		String columnToCount = getCountColumn();
		String regex = getRegexString();
		String table = frame.getName();

		// clean column to count
		if (columnToCount.contains("__")) {
			String[] split = columnToCount.split("__");
			table = split[0];
			columnToCount = split[1];
		}
		String[] existCols = getColNames(frame);
		if (Arrays.asList(existCols).contains(columnToCount) != true) {
			throw new IllegalArgumentException("Column: " + columnToCount + " doesn't exist.");
		}
		// new column datatype is set to numeric
		String dataType = frame.getQueryUtil().cleanType(SemossDataType.INT.toString());
		// clean new column name
		newColumnName = getCleanNewColName(frame, newColumnName);

		// escape single quote for sql
		if (regex.contains("'")) {
			regex = regex.replace("'", "''");
		}
		// 1) first add new column name
		String addColumnSQL = "ALTER TABLE " + table + " ADD " + newColumnName + " " + dataType + ";";
		// 2) create a temp column to replace the matching string in the column to count
		// with a replacement string
		String tempColName = "REP_" + Utility.getRandomString(5);
		String addTempColumn = "ALTER TABLE " + table + " ADD  " + tempColName + " varchar(800);";
		String tempReplacementString = ";;;" + Utility.getRandomString(3) + ";;;";
		String updateTempColumn = "UPDATE " + table + " SET " + tempColName + "= REGEXP_REPLACE (" + columnToCount
				+ ", '" + regex + "', '" + tempReplacementString + "');";

		// 3) Update the count column by setting it to the length of the col - replacing
		// the temp column with empty string
		String updateCountColumn = "UPDATE " + table + " SET " + newColumnName + " = " + "LENGTH(" + tempColName
				+ ") - LENGTH(REPLACE(" + tempColName + ",'" + tempReplacementString + "',''));";

		// 4) Update the count with MOD (tempColumn, tempString - 1)
		updateCountColumn += "UPDATE " + table + " SET " + newColumnName + " = MOD(" + newColumnName + ","
				+ (tempReplacementString.length() - 1) + " );";

		// 5) Drop temp column
		String dropTempColumn = "ALTER TABLE " + table + " DROP COLUMN " + tempColName + ";";

		try {
			frame.getBuilder().runQuery(addColumnSQL);
			frame.getBuilder().runQuery(addTempColumn);
			frame.getBuilder().runQuery(updateTempColumn);
			frame.getBuilder().runQuery(updateCountColumn);
			frame.getBuilder().runQuery(dropTempColumn);

			// set metadata for new column name
			OwlTemporalEngineMeta metaData = frame.getMetaData();
			metaData.addProperty(table, table + "__" + newColumnName);
			metaData.setAliasToProperty(table + "__" + newColumnName, newColumnName);
			metaData.setDataTypeToProperty(table + "__" + newColumnName, dataType);
		} catch (Exception e) {
			classLogger.error("Failed to count regex matches for column {} into {} on table {} using regex {}",
					columnToCount, newColumnName, table, regex, e);
		}

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private String getCountColumn() {
		GenRowStruct inputsGRS = this.getCurRow();
		String countColumn = "";
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			countColumn = inputsGRS.getNoun(0).getValue() + "";
			if (countColumn.length() < 0) {
				throw new IllegalArgumentException("Need to define the column for count");
			}
			return countColumn;
		}
		throw new IllegalArgumentException("Need to define the column for count");
	}

	private String getRegexString() {
		GenRowStruct inputsGRS = this.getCurRow();
		String regex = "";
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			regex = inputsGRS.getNoun(1).getValue() + "";
			if (regex.length() < 0) {
				throw new IllegalArgumentException("Need to define the regex for count");
			}
			return regex;
		}
		throw new IllegalArgumentException("Need to define the regex for count");
	}

	private String getNewColumn() {
		GenRowStruct inputsGRS = this.getCurRow();
		String newColumn = "";
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			newColumn = inputsGRS.getNoun(2).getValue() + "";
			if (newColumn.length() < 0) {
				throw new IllegalArgumentException("Need to define the new column name for count");
			}
			return newColumn;
		}
		throw new IllegalArgumentException("Need to define the new column name for count");
	}

}
