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

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SortColumnReactor extends AbstractRFrameReactor {

	/**
	 * This reactor sorts a column based on a given sort direction The inputs to the
	 * reactor are: 1) the column to sort 2) the sort direction
	 */

	public SortColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.COLUMN.getKey(),
				ReactorKeysEnum.SORT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		init();

		// get frame
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();

		// get table name
		String table = frame.getName();

		// get inputs
		// the first input is the column to sort
		String column = this.keyValue.get(this.keysToGet[1]);
		if (column == null) {
			column = getSortColumn();
		}
		if (column.contains("__")) {
			column = column.split("__")[1];
		}

		// second input is the sort direction
		String sortDir = this.keyValue.get(this.keysToGet[2]);
		if (sortDir == null) {
			sortDir = getSortDirection();
		}

		// if not column throw warning
		String dataType = metaData.getHeaderTypeAsString(table + "__" + column);
		if (dataType == null) {
			return getWarning("Frame is out of sync / No Such Column. Cannot perform this operation");
		}

		// define the scripts based on the sort direction
		String script = null;
		if (sortDir == null || sortDir.equalsIgnoreCase("asc")) {
			script = table + " <- " + table + "[order(rank(" + column + "))]";
		} else if (sortDir.equalsIgnoreCase("desc")) {
			script = table + " <- " + table + "[order(-rank(" + column + "))]";
		}

		// execute the r script
		// script will be of the form: FRAME <- FRAME[order(rank(Director))]
		frame.executeRScript(script);
		this.addExecutedCode(script);

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////
	///////////////////////// GET PIXEL INPUT ////////////////////////////
	//////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////

	private String getSortColumn() {
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// the first input will be the column to sort
			NounMetadata input1 = inputsGRS.getNoun(0);
			String fullColumn = input1.getValue() + "";
			if (fullColumn.length() == 0) {
				throw new IllegalArgumentException("Need to define the column to sort");
			}
			return fullColumn;
		}
		throw new IllegalArgumentException("Need to define the column to sort");
	}

	private String getSortDirection() {
		// the second input will be the sort direction
		NounMetadata input2 = this.getCurRow().getNoun(1);
		String sortDir = input2.getValue() + "";
		if (sortDir.length() == 0) {
			throw new IllegalArgumentException("Need to specify sort direction");
		}
		return sortDir;
	}
}
