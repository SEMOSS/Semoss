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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class AddColumnReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(AddColumnReactor.class);

	/**
	 * This reactor adds an empty column to the frame The inputs to the reactor are:
	 * 1) the name for the new column 2) the new column type 3) the column to
	 * duplicate values from
	 */

	public AddColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NEW_COLUMN.getKey(), ReactorKeysEnum.DATA_TYPE.getKey(),
				ReactorKeysEnum.ADDITIONAL_DATA_TYPE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		AbstractRdbmsFrame frame = (AbstractRdbmsFrame) getFrame();
		String table = frame.getName();
		// get column
		String newColName = this.keyValue.get(this.keysToGet[0]);
		if (newColName == null || newColName.length() == 0) {
			throw new IllegalArgumentException("Need to define the new column name");
		}
		newColName = getCleanNewColName(frame, newColName);

		// get datatype
		String dataType = this.keyValue.get(this.keysToGet[1]);
		if (dataType == null || dataType.isEmpty()) {
			dataType = "STRING";
		}
		String adtlDataType = this.keyValue.get(this.keysToGet[2]);

		// get new column type or set default to string
		// make sql data type
		dataType = frame.getQueryUtil().cleanType(dataType);
		if (frame != null) {
			try {
				frame.getBuilder().runQuery(frame.getQueryUtil().alterTableAddColumn(table, newColName, dataType));
				// set metadata for new column
				OwlTemporalEngineMeta metaData = frame.getMetaData();
				metaData.addProperty(table, table + "__" + newColName);
				metaData.setAliasToProperty(table + "__" + newColName, newColName);
				metaData.setDataTypeToProperty(table + "__" + newColName, dataType);
				if (adtlDataType != null && !adtlDataType.isEmpty()) {
					metaData.setAddtlDataTypeToProperty(frame.getName() + "__" + newColName, adtlDataType);
				}
			} catch (Exception e) {
				classLogger.error("Failed to add column {} with type {} on table {}", newColName, dataType, table, e);
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
