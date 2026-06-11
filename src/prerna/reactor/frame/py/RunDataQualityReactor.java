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
import prerna.ds.py.PandasFrame;
import prerna.ds.py.PandasSyntaxHelper;
import prerna.reactor.frame.FrameFactory;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RunDataQualityReactor extends AbstractPyFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(RunDataQualityReactor.class);

	private static final String RULE_KEY = "rule";
	private static final String COLUMNS_KEY = "column";
	private static final String OPTIONS_KEY = "options";
	private static final String INPUT_TABLE_KEY = "inputTable";

	public RunDataQualityReactor() {
		this.keysToGet = new String[] { RULE_KEY, COLUMNS_KEY, OPTIONS_KEY, INPUT_TABLE_KEY };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		PandasFrame frame = (PandasFrame) getFrame();
		String frameWrapper = frame.getWrapperName();
		String rule = getData(RULE_KEY);
		String column = getData(COLUMNS_KEY);
		List<Object> optionsList = getOptions(OPTIONS_KEY);
		PandasFrame inputTable = getInputTable();

		String retPyFrameName = null;
		if (inputTable != null) {
			retPyFrameName = inputTable.getName();
		} else {
			// did user define output table?
			retPyFrameName = getInputTableName();
			// no, make one up
			if (retPyFrameName == null) {
				retPyFrameName = "dataQualityTable_" + Utility.getRandomString(5);
			}
		}

		// load python module
		// SemossBase/py/DQ
		frame.runScript("from DQ import missionControl as mc");

		// create rule object
		// map of rules/ input for mission control
		StringBuilder str = new StringBuilder();
		String opt = PandasSyntaxHelper.createPandasColVec(optionsList, SemossDataType.STRING);
		String pyRule = "rule" + Utility.getRandomString(5);
		str.append(pyRule + " = {'rule': '" + rule + "', 'col': '" + column + "', 'options': " + opt + "}");
		frame.runScript(str.toString());

		if (inputTable == null) {
			// create empty frame to append rows to
			StringBuilder pyScript = new StringBuilder();
			pyScript.append(retPyFrameName).append(
					" = pd.DataFrame(columns=['Columns', 'Errors', 'Valid', 'Total', 'Rules', 'Description', 'toColor'])");
			frame.runScript(pyScript.toString());
		}

		// run mission control
		StringBuilder pyScript = new StringBuilder();
		pyScript.append(retPyFrameName)
				.append(" = mc.missionControl(" + frameWrapper + ", " + pyRule + ", " + retPyFrameName + ")");
		frame.runScript(pyScript.toString());

		if (inputTable != null) {
			return new NounMetadata(inputTable, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		}
		// make a new frame
		PandasFrame newFrame = null;
		try {
			newFrame = (PandasFrame) FrameFactory.getFrame(this.insight, "PY", retPyFrameName);
		} catch (Exception e) {
			classLogger.error("Failed to create output data-quality frame {}.", retPyFrameName, e);
			throw new IllegalArgumentException(e.getMessage());
		}

		// set data for new frame object
		frame.runScript(PandasSyntaxHelper.makeWrapper(newFrame.getWrapperName(), retPyFrameName));
		newFrame = (PandasFrame) recreateMetadata(newFrame, false);
		NounMetadata noun = new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
		this.insight.getVarStore().put(retPyFrameName, noun);
		return noun;
	}

	private List<Object> getOptions(String key) {
		// instantiate var ruleList as a list of strings
		List<Object> optionList = new ArrayList<>();
		GenRowStruct grs = this.store.getGenRowStruct(key);
		if (grs == null || grs.isEmpty()) {
			return optionList;
		}
		// Assign size to the length of grs
		int size = grs.size();
		// Iterate through the rule and add the value to the list
		for (int i = 0; i < size; i++) {
			optionList.add(grs.get(i) + "");
		}
		return optionList;
	}

	private String getData(String key) {
		GenRowStruct grs = this.store.getGenRowStruct(key);
		if (grs == null || grs.isEmpty()) {
			throw new IllegalArgumentException("Must set " + key);
		}
		return grs.get(0).toString();
	}

	private PandasFrame getInputTable() {
		GenRowStruct grs = this.store.getGenRowStruct(INPUT_TABLE_KEY);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		NounMetadata noun = grs.getNoun(0);
		if (noun.getNounType() == PixelDataType.FRAME) {
			return (PandasFrame) grs.get(0);
		}
		return null;
	}

	private String getInputTableName() {
		GenRowStruct grs = this.store.getGenRowStruct(INPUT_TABLE_KEY);
		if (grs == null || grs.isEmpty()) {
			return null;
		}
		NounMetadata noun = grs.getNoun(0);
		if (noun.getNounType() == PixelDataType.CONST_STRING) {
			return grs.get(0).toString();
		}
		return null;
	}
}
