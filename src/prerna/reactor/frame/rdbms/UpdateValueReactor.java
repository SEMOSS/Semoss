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

import java.sql.PreparedStatement;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdateValueReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(UpdateValueReactor.class);

	@Override
	public NounMetadata execute() {
		AbstractRdbmsFrame frame = (AbstractRdbmsFrame) getFrame();
		String columnInput = "";
		String table = "";
		String column = "";
		GenRowStruct inputsGRS = this.getCurRow();
		// get column to update
		columnInput = inputsGRS.getNoun(0).getValue() + "";
		if (columnInput.contains("__")) {
			String[] split = columnInput.split("__");
			table = split[0];
			column = split[1];
		} else {
			table = frame.getName();
			column = columnInput;
		}

		// check the column exists, if not then throw warning
		String[] allCol = getColNames(frame);
		if (Arrays.asList(allCol).contains(column) != true) {
			throw new IllegalArgumentException("Column doesn't exist.");
		}

		// get old column value
		String oldValue = getOldValue();

		// get new column value
		String newValue = getNewValue();

		// create sql update table set column = REGEXP_REPLACE(column, oldValue,
		// newValue);
		String update = "UPDATE " + table + " SET " + column + " = REGEXP_REPLACE(" + column + ", ?, ?);";

		try (PreparedStatement statement = frame.getConn().prepareStatement(update)) {
			statement.setString(1, oldValue);
			statement.setString(2, newValue);
			statement.executeUpdate();
		} catch (Exception e) {
			classLogger.error("Failed to replace value in column {} on table {} (oldValue={}, newValue={})", column,
					table, oldValue, newValue, e);
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private String getNewValue() {
		GenRowStruct inputsGRS = this.getCurRow();
		NounMetadata noun = inputsGRS.getNoun(2);
		Object nounValue = noun.getValue();
		return nounValue == null ? null : nounValue.toString();
	}

	private String getOldValue() {
		GenRowStruct inputsGRS = this.getCurRow();
		NounMetadata noun = inputsGRS.getNoun(1);
		Object nounValue = noun.getValue();
		return nounValue == null ? null : nounValue.toString();
	}

}
