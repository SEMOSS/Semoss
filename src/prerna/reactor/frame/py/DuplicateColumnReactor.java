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

import java.util.Arrays;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DuplicateColumnReactor extends AbstractPyFrameReactor {

	/**
	 * This reactor duplicates and existing column and adds it to the frame. The
	 * inputs to the reactor are: 1) the name for the column to duplicate 2) the new
	 * column name
	 */

	public DuplicateColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		String wrapperFrameName = frame.getWrapperName();

		// get source column to duplicate
		String srcCol = this.keyValue.get(this.keysToGet[0]);

		// make sure source column exists
		String[] allCol = getColumns(frame);
		if (srcCol == null || !Arrays.asList(allCol).contains(srcCol)) {
			throw new IllegalArgumentException("Need to define an existing column to duplicate.");
		}

		// clean and validate new column name or use default name
		String newColName = getCleanNewColName(frame, srcCol + "_DUPLICATE");
		String inputColName = this.keyValue.get(this.keysToGet[1]);
		if (inputColName != null && !inputColName.isEmpty()) {
			inputColName = getCleanNewColName(frame, inputColName);
			// entire new name could be invalid characters
			if (!inputColName.equals("")) {
				newColName = inputColName;
			}
		}

		// run duplicate script
		String script = wrapperFrameName + ".dupecol('" + srcCol + "', '" + newColName + "')";
		frame.runScript(script);
		this.addExecutedCode(script);

		// get src column data type
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		String dataType = metaData.getHeaderTypeAsString(frame.getName() + "__" + srcCol);
		String adtlDataType = metaData.getHeaderAdtlType(frame.getName() + "__" + srcCol);

		// update meta data
		metaData.addProperty(frame.getName(), frame.getName() + "__" + newColName);
		metaData.setAliasToProperty(frame.getName() + "__" + newColName, newColName);
		metaData.setDataTypeToProperty(frame.getName() + "__" + newColName, dataType);
		if (adtlDataType != null && !adtlDataType.isEmpty()) {
			metaData.setAddtlDataTypeToProperty(frame.getName() + "__" + newColName, adtlDataType);
		}

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new AddHeaderNounMetadata(newColName));
		return retNoun;
	}

}