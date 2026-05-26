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

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.AddHeaderNounMetadata;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AutoCleanColumnReactor extends AbstractRFrameReactor {

	public AutoCleanColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMN.getKey(), ReactorKeysEnum.OVERRIDE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		init();
		organizeKeys();
		String column = this.keyValue.get(this.keysToGet[0]);
		boolean override = overrideExistingColumn();
		RDataTable frame = (RDataTable) this.getFrame();
		String tableName = frame.getName();

		// check if packages are installed
		String[] packages = { "stringdist", "data.table", "tm", "cluster" };
		this.rJavaTranslator.checkPackages(packages);

		// make sure its a string
		String dataType = this.getColumnType(tableName, column);
		if (dataType == null) {
			return getWarning("Frame is out of sync / No Such Column. Cannot perform this operation");
		}

		if (SemossDataType.convertStringToDataType(dataType) != SemossDataType.STRING) {
			throw new IllegalArgumentException("The column type must be a String.");
		}

		// source teh scripts
		String baseFolder = Utility.getBaseFolder();
		String sourceScript = "source(\"" + baseFolder + "\\R\\Recommendations\\master_col_data.r\") ;";
		sourceScript = sourceScript.replace("\\", "/");
		this.rJavaTranslator.runR(sourceScript);

		List<PixelOperationType> opTypes = new Vector<PixelOperationType>();
		opTypes.add(PixelOperationType.FRAME_DATA_CHANGE);
		NounMetadata retNoun = null;

		OwlTemporalEngineMeta metaData = frame.getMetaData();
		if (!override) {
			// adding new column
			// data + headers change
			opTypes.add(PixelOperationType.FRAME_HEADERS_CHANGE);
			retNoun = new NounMetadata(frame, PixelDataType.FRAME, opTypes);

			// new col is mastered version of column
			String tempCol = Utility.getRandomString(8);
			String newHeaderName = getCleanNewHeader(tableName, column);
			StringBuilder script = new StringBuilder();
			script.append(tempCol + " <- " + tableName + "$" + column + ";");
			script.append(tempCol + " <- master_col_data(as.character(" + tempCol + "));");
			script.append(tableName + " <- " + "cbind(" + tableName + ", " + tempCol + ");");
			script.append(tableName + "$" + newHeaderName + " <- " + tempCol + "; ");
			script.append(tableName + " <- " + tableName + "[,-c('" + tempCol + "')];");
			script.append("rm(" + tempCol + ");");
			this.rJavaTranslator.runR(script.toString());
			this.addExecutedCode(script.toString());

			// add meta data to frame
			retNoun.addAdditionalReturn(new AddHeaderNounMetadata(newHeaderName));
			metaData.addProperty(tableName, tableName + "__" + newHeaderName);
			metaData.setAliasToProperty(tableName + "__" + newHeaderName, newHeaderName);
			metaData.setDataTypeToProperty(tableName + "__" + newHeaderName, SemossDataType.STRING.toString());
		} else {
			// override existing column
			// just a data change
			retNoun = new NounMetadata(frame, PixelDataType.FRAME, opTypes);

			// execute the script on the column and replace original
			this.rJavaTranslator
					.runR(tableName + "$" + column + " <- master_col_data(" + tableName + "$" + column + ");");
		}
		frame.syncHeaders();

		return retNoun;
	}

	/**
	 * Create new column or override existing column
	 * 
	 * @return
	 */
	private boolean overrideExistingColumn() {
		GenRowStruct boolGrs = this.store.getGenRowStruct(this.keysToGet[1]);
		if (boolGrs != null) {
			if (boolGrs.size() > 0) {
				List<Object> val = boolGrs.getValuesOfType(PixelDataType.BOOLEAN);
				return (boolean) val.get(0);
			}
		}
		return true;
	}

}
