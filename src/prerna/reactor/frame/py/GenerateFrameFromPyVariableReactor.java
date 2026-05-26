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

import org.apache.logging.log4j.Logger;

import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.ds.py.PyTranslator;
import prerna.reactor.imports.ImportUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GenerateFrameFromPyVariableReactor extends AbstractPyFrameReactor {

	private static final String CLASS_NAME = GenerateFrameFromPyVariableReactor.class.getName();

	public GenerateFrameFromPyVariableReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.VARIABLE.getKey(), ReactorKeysEnum.OVERRIDE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		// init();
		organizeKeys();
		String varName = getVarName();
		PyTranslator pyTranslator = this.insight.getPyTranslator();
		logger.info("Getting the columns for :" + varName);
		String[] colNames = pyTranslator.getStringArray(PandasSyntaxHelper.getColumns(varName));

		// I bet this is being done for pixel.. I will keep the same
		logger.info("Cleaning the columns for :" + varName);
		pyTranslator.runScript(PandasSyntaxHelper.cleanFrameHeaders(varName, colNames));
		colNames = pyTranslator.getStringArray(PandasSyntaxHelper.getColumns(varName));

		logger.info("Getting the column types for :" + varName);
		String[] colTypes = pyTranslator.getStringArray(PandasSyntaxHelper.getTypes(varName));

		if (colNames == null || colTypes == null) {
			throw new IllegalArgumentException(
					"Please make sure the variable " + varName + " exists and can be a valid data.table object");
		}
		PandasFrame frame = new PandasFrame(varName, pyTranslator);
		pyTranslator.runScript(PandasSyntaxHelper.makeWrapper(frame.getWrapperName(), varName));

		// create the pandas frame
		// and set up teverything else
		ImportUtility.parseTableColumnsAndTypesToFlatTable(frame.getMetaData(), colNames, colTypes, varName);

		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
		if (overrideFrame()) {
			this.insight.setDataMaker(frame);
		}
		// add the alias as a noun by default
		if (varName != null && !varName.isEmpty()) {
			this.insight.getVarStore().put(varName, noun);
		}

		return noun;
	}

	private boolean overrideFrame() {
		GenRowStruct overrideGrs = this.store.getGenRowStruct(ReactorKeysEnum.OVERRIDE.getKey());
		if (overrideGrs != null && !overrideGrs.isEmpty()) {
			return (boolean) overrideGrs.get(0);
		}
		// default is to override
		return true;
	}

	/**
	 * Get the input being the r variable name
	 * 
	 * @return
	 */
	private String getVarName() {
		// key based
		GenRowStruct overrideGrs = this.store.getGenRowStruct(ReactorKeysEnum.VARIABLE.getKey());
		if (overrideGrs != null && !overrideGrs.isEmpty()) {
			return (String) overrideGrs.get(0);
		}
		// first input
		return this.curRow.get(0).toString();
	}

	///////////////////////// KEYS /////////////////////////////////////

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.VARIABLE.getKey())) {
			return "Name of the py variable";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
