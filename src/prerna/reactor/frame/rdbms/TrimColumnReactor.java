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
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class TrimColumnReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(TrimColumnReactor.class);

	@Override
	public NounMetadata execute() {
		AbstractRdbmsFrame frame = (AbstractRdbmsFrame) getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();

		GenRowStruct inputsGRS = this.getCurRow();
		String update = "";
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			for (int selectIndex = 0; selectIndex < inputsGRS.size(); selectIndex++) {
				NounMetadata input = inputsGRS.getNoun(selectIndex);
				String columnName = input.getValue() + "";

				String table = frame.getName();
				String column = columnName;
				if (columnName.contains("__")) {
					String[] split = columnName.split("__");
					table = split[0];
					column = split[1];
				}

				String dataType = metaData.getHeaderTypeAsString(table + "__" + column);
				if (dataType.equals("STRING")) {
					// execute update table set column = UPPER(column);
					update += "UPDATE " + table + " SET " + column + " = TRIM(" + column + ");";
				}
			}
		}
		if (update.length() > 0) {
			try {
				frame.getBuilder().runQuery(update);
			} catch (Exception e) {
				classLogger.error("Failed to trim selected columns on frame {}", frame.getName(), e);
			}
		}

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}
}
