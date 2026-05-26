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

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Use ConcatenateReactor
 * 
 * @deprecated
 */
@Deprecated
public class JoinColumnsReactor extends AbstractRFrameReactor {

	/**
	 * This reactor joins columns, and puts the joined string into a new column with
	 * values separated by a separator The inputs to the reactor are: 1) the new
	 * column name 2) the delimiter 3) the columns to join
	 */

	public JoinColumnsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NEW_COLUMN.getKey(), ReactorKeysEnum.DELIMITER.getKey(),
				ReactorKeysEnum.COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();

		// get table name
		String table = frame.getName();

		// create string builder to build the r script
		StringBuilder rsb = new StringBuilder();

		// first input is what we want to name the new column
		String newColName = this.keyValue.get(this.keysToGet[0]);
		if (newColName == null) {
			newColName = getNewColName();
		}
		// check if new colName is valid
		newColName = getCleanNewColName(frame, newColName);

		// second input is the delimeter/separator
		String separator = this.keyValue.get(this.keysToGet[1]);
		if (separator == null) {
			separator = getSeparator();
		}
		rsb.append(table + "$" + newColName + " <- paste(");
		// the remaining inputs are all of the columns that we want to join
		List<String> columnList = getColumns();
		for (int i = 0; i < columnList.size(); i++) {
			String column = columnList.get(i);
			// separate the column name from the frame name
			if (column.contains("__")) {
				column = column.split("__")[1];
			}

			// continue building the stringbuilder for the r script
			rsb.append(table + "$" + column);
			if (i < columnList.size() - 1) {
				// add a comma between each column entry
				rsb.append(", ");
			}
		}
		rsb.append(", sep = \"" + separator + "\")");
		// convert the stringbuiler to a string and execute
		// script will be of the form: FRAME$mynewcolumn <- paste(FRAME$Year,
		// FRAME$Title, FRAME$Director, sep = ", ")
		String script = rsb.toString();
		this.rJavaTranslator.runR(script);
		this.addExecutedCode(script);

		// update the metadata because column data has changed
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		metaData.addProperty(table, table + "__" + newColName);
		metaData.setAliasToProperty(table + "__" + newColName, newColName);
		metaData.setDataTypeToProperty(table + "__" + newColName, SemossDataType.STRING.toString());
		this.getFrame().syncHeaders();

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private String getNewColName() {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// first input is what we want to name the new column
			String newColName = inputsGRS.getNoun(0).getValue() + "";
			if (newColName.length() == 0) {
				throw new IllegalArgumentException("Need to define the new column name");
			}
			return newColName;
		}
		throw new IllegalArgumentException("Need to define the new column name");
	}

	private String getSeparator() {
		// get separator by index
		NounMetadata input2 = this.getCurRow().getNoun(1);
		String separator = input2.getValue() + "";
		return separator;
	}

	private List<String> getColumns() {
		List<String> columns = new Vector<>();
		// get columns by key
		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[2]);
		NounMetadata noun;
		if (grs != null) {
			for (int i = 0; i < grs.size(); i++) {
				noun = grs.getNoun(i);
				if (noun != null) {
					String column = noun.getValue() + "";
					if (column.length() > 0) {
						columns.add(column);
					}
				}
			}
		} else {

			// get columns by index
			int inputSize = this.getCurRow().size();
			for (int i = 2; i < inputSize; i++) {
				NounMetadata input = this.getCurRow().getNoun(i);
				String column = input.getValue() + "";
				columns.add(column);
			}
		}
		return columns;
	}
}
