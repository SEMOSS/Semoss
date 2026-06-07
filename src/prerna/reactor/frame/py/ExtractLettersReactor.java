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

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class ExtractLettersReactor extends AbstractPyFrameReactor {

	public static final String ALPHA_COLUMN_NAME = "_ALPHA";
	private static final Logger classLogger = LogManager.getLogger(ExtractLettersReactor.class);

	public ExtractLettersReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.OVERRIDE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		// get table name
		String wrapperFrameName = frame.getWrapperName();
		// get columns to extract alphabet characters
		List<String> columns = getColumns();
		// check if user want to override the column or create new columns
		boolean overrideColumn = getOverride();
		// we need to check data types this will only be valid on non numeric values
		OwlTemporalEngineMeta metadata = frame.getMetaData();

		// extracts the letters
		// mv['add'] = mv.apply(lambda x: re.sub('\d+', '', x['MovieBudget']) if
		// not(isinstance(x['MovieBudget'], int)) else x['MovieBudget'] , axis=1)

		List<PixelOperationType> opTypes = new ArrayList<PixelOperationType>();
		opTypes.add(PixelOperationType.FRAME_DATA_CHANGE);
		StringBuilder commands = new StringBuilder();
		// update existing columns
		if (overrideColumn) {
			for (int i = 0; i < columns.size(); i++) {
				String column = columns.get(i);
				// check data type this is only valid on non numeric values
				SemossDataType dataType = metadata.getHeaderTypeAsEnum(frame.getName() + "__" + column);
				if (Utility.isStringType(dataType.toString())) {
					try {
						commands.append(wrapperFrameName + ".extract_alpha('" + column + "')\n");
					} catch (Exception e) {
						classLogger.error("Failed to prepare alphabetic extraction command for column {} on frame {}.",
								column, frame.getName(), e);
					}
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
				SemossDataType dataType = metadata.getHeaderTypeAsEnum(frame.getName() + "__" + column);
				if (Utility.isStringType(dataType.toString())) {
					String newColumn = getCleanNewColName(frame, column + ALPHA_COLUMN_NAME);
					commands.append(wrapperFrameName + ".extract_alpha('" + column + "',  '" + newColumn + "')\n");

					metaData.addProperty(frame.getName(), frame.getName() + "__" + newColumn);
					metaData.setAliasToProperty(frame.getName() + "__" + newColumn, newColumn);
					metaData.setDataTypeToProperty(frame.getName() + "__" + newColumn,
							SemossDataType.STRING.toString());
				} else {
					throw new IllegalArgumentException("Column type must be string");
				}
			}
		}
		insight.getPyTranslator().runEmptyPy(commands.toString());
		this.addExecutedCode(commands.toString());

		return new NounMetadata(frame, PixelDataType.FRAME, opTypes);
	}

	private List<String> getColumns() {
		GenRowStruct grs = this.store.getGenRowStruct(keysToGet[0]);
		List<String> columns = new ArrayList<String>();
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
		}
		return columns;
	}

	private boolean getOverride() {
		GenRowStruct grs = this.store.getGenRowStruct(keysToGet[1]);
		boolean override = false;
		NounMetadata noun;
		if (grs != null) {
			noun = grs.getNoun(0);
			override = (Boolean) noun.getValue();
		}
		return override;
	}

}
