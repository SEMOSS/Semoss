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

import org.apache.logging.log4j.Logger;

import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.reactor.imports.ImportUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GenerateFrameFromRVariableReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = GenerateFrameFromRVariableReactor.class.getName();

	public GenerateFrameFromRVariableReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.VARIABLE.getKey(), ReactorKeysEnum.OVERRIDE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		init();
		organizeKeys();
		String varName = getStringFromKeyOrCurRow(ReactorKeysEnum.VARIABLE.getKey(), 0);
		this.rJavaTranslator.executeEmptyR(RSyntaxHelper.asDataTable(varName, varName));
		// recreate a new frame and set the frame name
		String[] colNames = this.rJavaTranslator.getColumns(varName);
		// clean r frame column names
		this.rJavaTranslator.runR(RSyntaxHelper.cleanFrameHeaders(varName, colNames));
		colNames = this.rJavaTranslator.getColumns(varName);
		String[] colTypes = this.rJavaTranslator.getColumnTypes(varName);
		if (colNames == null || colTypes == null) {
			throw new IllegalArgumentException(
					"Please make sure the variable " + varName + " exists and can be a valid data.table object");
		}

		RDataTable newTable = new RDataTable(this.insight.getRJavaTranslator(logger), varName);
		ImportUtility.parseTableColumnsAndTypesToFlatTable(newTable.getMetaData(), colNames, colTypes, varName);
		NounMetadata noun = new NounMetadata(newTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
		if (getBoolean(ReactorKeysEnum.OVERRIDE.getKey(), true)) {
			this.insight.setDataMaker(newTable);
		}
		// add the alias as a noun by default
		if (varName != null && !varName.isEmpty()) {
			this.insight.getVarStore().put(varName, noun);
		}

		return noun;
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.VARIABLE.getKey())) {
			return "Name of the r variable";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
