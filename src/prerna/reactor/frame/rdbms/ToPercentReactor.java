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

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.sql.RdbmsTypeEnum;

public class ToPercentReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(ToPercentReactor.class);

	private static final String BY100 = "by100";
	private static final String SIG_DIGITS = "sigDigits";

	public ToPercentReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), SIG_DIGITS, BY100,
				ReactorKeysEnum.NEW_COLUMN.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get frame
		AbstractRdbmsFrame frame = (AbstractRdbmsFrame) getFrame();
		OwlTemporalEngineMeta metaData = frame.getMetaData();
		String table = frame.getName();
		// get SQL dialect
		RdbmsTypeEnum dialect = frame.getQueryUtil().getDbType();
		// get new column name
		String newColName = this.keyValue.get(this.keysToGet[3]);
		// get remaining Keys
		String srcCol = this.keyValue.get(ReactorKeysEnum.COLUMN.getKey());
		int sigDigits = getValue(SIG_DIGITS);
		boolean by100 = getBoolean(BY100, false);
		// multiplying by 1 doesn't effect the calc, only set to 100 if by100 is true
		int multiplyFactor = 1;
		if (by100) {
			multiplyFactor = 100;
		}
		String update = "";
		// check for new col
		String updateCol = srcCol;
		if (newColName == null || newColName.equals("") || newColName.equals("null")) {
			newColName = getCleanNewColName(frame, newColName);
			// add column
			if (dialect == RdbmsTypeEnum.SQLITE) {
				update += "ALTER TABLE " + table + " ADD COLUMN " + newColName + " text;";
			} else {
				update += "ALTER TABLE " + table + " ADD COLUMN " + newColName + " VARCHAR (500);";
			}
			// duplicate column
			update += "UPDATE " + table + " SET " + newColName + " = " + srcCol + ";";
			updateCol = newColName;
		}
		if (dialect == RdbmsTypeEnum.SQLITE) {
			update += "UPDATE " + table + " SET " + updateCol + " = ROUND(" + srcCol + "*" + multiplyFactor + ", "
					+ sigDigits + ");";
			update += "UPDATE " + table + " SET " + updateCol + " = " + updateCol + " || '%';";
		} else { // perform update on current column using sql concat function
			update += "UPDATE " + table + " SET " + updateCol + " = CONCAT(ROUND(" + srcCol + "*" + multiplyFactor
					+ ", " + sigDigits + "),'%');";
		}
		update += "UPDATE " + table + " SET " + updateCol + " = NULL WHERE " + srcCol + " = '%';";
		if (update.length() > 0) {
			try {
				frame.getBuilder().runQuery(update);
				NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME,
						PixelOperationType.FRAME_DATA_CHANGE);
				if (newColName != null && !newColName.equals("")) {
					retNoun.addAdditionalOpTypes(PixelOperationType.FRAME_HEADERS_CHANGE);
					String addedColumnDataType = SemossDataType.STRING.toString();
					metaData.addProperty(table, table + "__" + newColName);
					metaData.setAliasToProperty(table + "__" + newColName, newColName);
					metaData.setDataTypeToProperty(table + "__" + newColName, addedColumnDataType);
					metaData.setDerivedToProperty(table + "__" + newColName, true);
					frame.syncHeaders();
				} else {
					metaData.modifyDataTypeToProperty(table + "__" + srcCol, table, SemossDataType.STRING.toString());
				}
				return retNoun;
			} catch (Exception e) {
				classLogger.error(
						"Failed to convert column {} to percent on table {} (newColumn={}, sigDigits={}, by100={})",
						srcCol, table, newColName, sigDigits, by100, e);
			}
		}

		throw new IllegalArgumentException("Unable to generate percent column");
	}

	private int getValue(String key) {
		GenRowStruct grs = this.store.getGenRowStruct(key);
		NounMetadata noun = grs.getNoun(0);

		if (noun.getNounType() == PixelDataType.CONST_INT) {
			return (int) grs.get(0);
		} else {
			throw new IllegalArgumentException(
					"Input of " + grs.get(0) + " is invalid. Significant digits must be an integer value.");
		}
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SIG_DIGITS)) {
			return "Indicates the number of significant digits you'd like to keep";
		} else if (key.equals(BY100)) {
			return "Indicates if you want to multiply by 100 to get in percent form.";
		} else {
			return super.getDescriptionForKey(key);
		}
	}
}
