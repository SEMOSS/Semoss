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
package prerna.reactor.frame.polars;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.py.PolarsFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Strictly converts a Polars column to a supported SEMOSS scalar type.
 */
public class ChangeColumnTypeReactor extends AbstractPolarsFrameReactor {

	public ChangeColumnTypeReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.DATA_TYPE.getKey(),
				ReactorKeysEnum.ADDITIONAL_DATA_TYPE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		PolarsFrame frame = getPolarsFrame();
		String column = cleanColumn(this.keyValue.get(this.keysToGet[0]));
		SemossDataType type = SemossDataType.convertStringToDataType(this.keyValue.get(this.keysToGet[1]));
		if (column == null || type == null) {
			throw new IllegalArgumentException("Column and data type are required");
		}
		frame.changeColumnType(column, type.toString());
		String additionalType = this.keyValue.get(this.keysToGet[2]);
		if (additionalType != null && !additionalType.isEmpty()) {
			frame.getMetaData().modifyAdditionalDataTypeToProperty(
					frame.getName() + "__" + column, frame.getName(), additionalType);
		}
		addExecutedCode("polars.cast(" + column + ", " + type + ")");
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
	}
}
