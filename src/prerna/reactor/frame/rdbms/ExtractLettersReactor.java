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

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ExtractLettersReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(ExtractLettersReactor.class);

	public static final String COLUMNS = "columns";
	public static final String OVERRIDE = "override";
	public static final String ALPHA_COLUMN_NAME = "_ALPHA";

	@Override
	public NounMetadata execute() {
		AbstractRdbmsFrame frame = (AbstractRdbmsFrame) getFrame();
		// get table name
		String table = frame.getName();
		// get columns to extract alphabet characters
		List<String> columns = getListString(COLUMNS, new ArrayList<String>());
		// check if user want to override the column or create new columns
		boolean overrideColumn = getBoolean(OVERRIDE, false);
		// update existing columns
		if (overrideColumn) {
			String update = "";
			for (int i = 0; i < columns.size(); i++) {
				String column = columns.get(i);
				update += "UPDATE " + table + " SET " + column + "= REGEXP_REPLACE(" + column
						+ ", '[^a-zA-Z\\_]', ''); ";
			}
			try {
				frame.getBuilder().runQuery(update);
			} catch (Exception e) {
				classLogger.error("Failed to extract alphabetic characters in-place for columns {} on table {}",
						columns, table, e);
			}
		}
		// create new columns
		else {
			for (int i = 0; i < columns.size(); i++) {
				String column = columns.get(i);
				String newColumn = getCleanNewColName(frame, column + ALPHA_COLUMN_NAME);
				// add new column
				String update = "ALTER TABLE " + table + " ADD " + newColumn + " varchar(800);";
				// update extract alpha characters and underscores
				update += "UPDATE " + table + " SET " + newColumn + " = REGEXP_REPLACE(" + column
						+ ", '[^a-zA-Z\\_]', '');";
				try {
					frame.getBuilder().runQuery(update);
				} catch (Exception e) {
					classLogger.error("Failed to extract alphabetic characters from column {} into {} on table {}",
							column, newColumn, table, e);
				}
				// if query runs successfully add new column metadata
				OwlTemporalEngineMeta metaData = frame.getMetaData();
				metaData.addProperty(table, table + "__" + newColumn);
				metaData.setAliasToProperty(table + "__" + newColumn, newColumn);
				metaData.setDataTypeToProperty(table + "__" + newColumn, "String");
			}
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

}
