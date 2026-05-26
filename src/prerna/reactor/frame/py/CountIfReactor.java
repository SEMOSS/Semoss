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
package prerna.reactor.frame.py;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class CountIfReactor extends AbstractPyFrameReactor {

	/**
	 * This reactor creates a new column based on the count of regex matches of an
	 * existing column The inputs to the reactor are: 1) the column to count regex
	 * instances in 2) the regex 3) the new column name
	 */

	public CountIfReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.REGEX.getKey(),
				ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		// get frame name
		String wrapperFrameName = frame.getWrapperName();
		// get inputs
		String column = this.keyValue.get(this.keysToGet[0]);
		if (column == null) {
			column = getExistingColumn();
		}
		// clean column name
		if (column.contains("__")) {
			column = column.split("__")[1];
		}
		String regexToCount = this.keyValue.get(this.keysToGet[1]);
		if (regexToCount == null) {
			regexToCount = getRegex();
		}
		String newColName = this.keyValue.get(this.keysToGet[2]);
		if (newColName == null) {
			newColName = getNewColumn();
		}
		// check if new colName is valid
		newColName = getCleanNewColName(frame, newColName);

		// TODO: doesn't this need to be updated/fixed?
		// TODO: doesn't this need to be updated/fixed?
		// TODO: doesn't this need to be updated/fixed?

		// this function only works on strings, so we must convert the data to a
		// string if it is not already
		boolean numeric = (boolean) frame.runScript(wrapperFrameName + ".is_numeric('" + column + "')");
		if (!numeric) {
			String script = wrapperFrameName + ".countif('" + column + "', '" + regexToCount + "', '" + newColName
					+ "')";
			frame.runScript(script);
			this.addExecutedCode(script);
		}

		// else
		// return an error ?

		// update the metadata
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();
		metaData.addProperty(frame.getName(), frame.getName() + "__" + newColName);
		metaData.setAliasToProperty(frame.getName() + "__" + newColName, newColName);
		metaData.setDataTypeToProperty(frame.getName() + "__" + newColName, SemossDataType.DOUBLE.toString());
		this.getFrame().syncHeaders();

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private String getExistingColumn() {
		// first input is the name of the column
		// that the operation is being done on
		NounMetadata noun = this.curRow.getNoun(0);
		String column = noun.getValue().toString();
		if (column.contains("__")) {
			column = column.split("__")[1];
		}
		return column;
	}

	private String getRegex() {
		// second input is the regex to count
		NounMetadata noun = this.curRow.getNoun(1);
		return noun.getValue().toString();
	}

	private String getNewColumn() {
		// third input is the new column name
		NounMetadata noun = this.curRow.getNoun(2);
		String column = noun.getValue().toString();
		return column;
	}

}
