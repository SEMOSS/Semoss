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
import java.util.HashMap;
import java.util.Map;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.query.querystruct.transform.QSRenameColumnConverter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.ModifyHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class RenameColumnReactor extends AbstractPyFrameReactor {

	/**
	 * <p>
	 * This reactor renames a column
	 * </p>
	 *
	 * <p>
	 * The inputs to the reactor are:
	 * </p>
	 * <ul>
	 * <li>the original column</li>
	 * <li>the new column name</li>
	 * </ul>
	 */

	public RenameColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		// get inputs
		String originalColName = keyValue.get(this.keysToGet[0]);
		String updatedColName = keyValue.get(this.keysToGet[1]);

		// check that the frame isn't null
		String wrapperFrameName = frame.getWrapperName();
		// check if new colName is valid
		updatedColName = getCleanNewColName(frame, updatedColName);
		if (originalColName.contains("__")) {
			String[] split = originalColName.split("__");
			wrapperFrameName = split[0];
			originalColName = split[1];
		}
		// ensure new header name is valid
		// make sure that the new name we want to use is valid
		String[] existCols = getColNames(frame);
		if (Arrays.asList(existCols).contains(originalColName) != true) {
			throw new IllegalArgumentException("Column doesn't exist.");
		}

		String validNewHeader = getCleanNewColName(frame, updatedColName);
		if (validNewHeader.equals("")) {
			throw new IllegalArgumentException("Provide valid new column name (no special characters)");
		}

		// script is of the form: wrapper.rename_col('Genre', 'Genre_new')"
		String script = wrapperFrameName + ".rename_col('" + originalColName + "', '" + validNewHeader + "')";
		frame.runScript(script);
		this.addExecutedCode(script);

		// FE passes the column name
		// but meta will still be table __ column
		// update the metadata because column names have changed
		OwlTemporalEngineMeta metadata = this.getFrame().getMetaData();
		metadata.modifyPropertyName(frame.getName() + "__" + originalColName, frame.getName(),
				frame.getName() + "__" + validNewHeader);
		metadata.setAliasToProperty(frame.getName() + "__" + validNewHeader, validNewHeader);
		this.getFrame().syncHeaders();

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);
		ModifyHeaderNounMetadata metaNoun = new ModifyHeaderNounMetadata(frame.getName(), originalColName,
				validNewHeader);
		retNoun.addAdditionalReturn(metaNoun);

		// also modify the frame filters
		Map<String, String> modMap = new HashMap<String, String>();
		modMap.put(originalColName, validNewHeader);
		frame.setFrameFilters(QSRenameColumnConverter.convertGenRowFilters(frame.getFrameFilters(), modMap, false));

		// return the output
		return retNoun;
	}
}
