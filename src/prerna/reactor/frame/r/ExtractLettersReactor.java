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

import java.util.ArrayList;
import java.util.List;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ExtractLettersReactor extends AbstractRFrameReactor {

	public static final String ALPHA_COLUMN_NAME = "_ALPHA";

	public ExtractLettersReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.OVERRIDE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		// get table name
		String table = frame.getName();
		// get columns to extract alphabet characters
		List<String> columns = getListString(keysToGet[0], new ArrayList<>());
		// check if user want to override the column or create new columns
		boolean overrideColumn = getBoolean(keysToGet[1], false);
		// we need to check data types this will only be valid on non numeric values
		OwlTemporalEngineMeta metadata = frame.getMetaData();

		List<PixelOperationType> opTypes = new ArrayList<>();
		opTypes.add(PixelOperationType.FRAME_DATA_CHANGE);
		// update existing columns
		if (overrideColumn) {
			for (int i = 0; i < columns.size(); i++) {
				String column = columns.get(i);
				// check data type this is only valid on non numeric values
				SemossDataType dataType = metadata.getHeaderTypeAsEnum(table + "__" + column);
				if (dataType == null) {
					return getWarning("Frame is out of sync / No Such Column. Cannot perform this operation");
				}

				if (Utility.isStringType(dataType.toString())) {
					String update = table + "$" + column + " <- gsub('[^a-zA-Z_]', '', " + table + "$" + column + ")";
					frame.executeRScript(update);
					this.addExecutedCode(update);
				} else {
					throw new IllegalArgumentException("Column type must be string");
				}
			}
		}
		// create new column
		else {
			opTypes.add(PixelOperationType.FRAME_HEADERS_CHANGE);
			for (int i = 0; i < columns.size(); i++) {
				String column = columns.get(i);
				SemossDataType dataType = metadata.getHeaderTypeAsEnum(table + "__" + column);
				if (Utility.isStringType(dataType.toString())) {
					String newColumn = getCleanNewColName(frame, column + ALPHA_COLUMN_NAME);
					String update = table + "$" + newColumn + " <- \"\";";
					update += table + "$" + newColumn + " <- gsub('[^a-zA-Z_]', '', " + table + "$" + column + ");";
					this.rJavaTranslator.runR(update);
					this.addExecutedCode(update);
					metaData.addProperty(table, table + "__" + newColumn);
					metaData.setAliasToProperty(table + "__" + newColumn, newColumn);
					metaData.setDataTypeToProperty(table + "__" + newColumn, SemossDataType.STRING.toString());
				} else {
					throw new IllegalArgumentException("Column type must be string");
				}
			}
		}

		return new NounMetadata(frame, PixelDataType.FRAME, opTypes);
	}
}
