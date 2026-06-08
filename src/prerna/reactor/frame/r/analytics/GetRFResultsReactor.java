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
package prerna.reactor.frame.r.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.reactor.task.constant.ConstantTaskCreationHelper;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.util.Utility;

public class GetRFResultsReactor extends AbstractRFrameReactor {

	private static final String CLASS_NAME = GetRFResultsReactor.class.getName();
	private static final String SORTBY = "sortBy";
	private static final String REQUESTITEM = "requestItem"; // either 'VarImp' or 'ConfMatrix'

	/**
	 * <p>
	 * GetRFResults(requestItem = [VarImp], panel = [99])
	 * </p>
	 *
	 * <p>
	 * GetRFResults(requestItem = [CONFMATRIX], panel = [99])
	 * </p>
	 *
	 * <p>
	 * This reactor only runs if the RRandomForestAlgorithmReactor created variable
	 * RF_VARIABLE_999988888877777, since this routine extracts and processes that
	 * variable.
	 * </p>
	 *
	 * <p>
	 * Input keys:
	 * </p>
	 * <ul>
	 * <li>sortBy (optional) - for classification results, sort variable importance
	 * by MeanDecreaseAccuracy (1) or MeanDecreaseGini (2). For regression results,
	 * sort by %IncMSE (1) or IncNodePurity (2). Default is 1</li>
	 * <li>requestItem (required) - must be either varimp (variable importance) or
	 * confmatrix (confusion matrix)</li>
	 * <li>panelID (required)</li>
	 * </ul>
	 */
	public GetRFResultsReactor() {
		this.keysToGet = new String[] { SORTBY, REQUESTITEM, ReactorKeysEnum.PANEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		init();
		String[] packages = new String[] { "data.table", "randomForest", "dplyr" };
		this.rJavaTranslator.checkPackages(packages);
		String panelId = getPanelId();
		StringBuilder sb = new StringBuilder();

		// retrieve inputs
		String sortBy = getStringInput(SORTBY);
		if (sortBy == null) {
			sortBy = "1";
		}
		String requestItem = getStringInput(REQUESTITEM);
		if (!new ArrayList<String>(Arrays.asList("varimp", "confmatrix")).contains(requestItem.toLowerCase())) {
			throw new IllegalArgumentException(
					"Invalid requestItem - requestItem must be either 'varimp' or 'confmatrix'.");
		}

		// random forest r script
		String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\RandomForest.R";
		scriptFilePath = scriptFilePath.replace("\\", "/");
		sb.append("source(\"" + scriptFilePath + "\");");

		String temp_R = "tempVar" + Utility.getRandomString(8);
		ITask taskData = null;
		switch (requestItem.toLowerCase()) {
		case "varimp":
			sb.append(temp_R + " <- getRFResults( RF_VARIABLE_999988888877777, 'varimp', sortBy=" + sortBy + ");");
			this.rJavaTranslator.runR(sb.toString());

			String[] varImpCols = this.rJavaTranslator.getColumns(temp_R + "$returnObject");
			List<Object[]> varImpData = this.rJavaTranslator.getBulkDataRow(temp_R + "$returnObject", varImpCols);
			// label,x,y,z,series
			String[] varImpAlignment = this.rJavaTranslator.getStringArray(temp_R + "$alignmentInfo");

			taskData = ConstantTaskCreationHelper.getScatterPlotData(panelId, varImpCols, varImpData,
					varImpAlignment[0], varImpAlignment[1], varImpAlignment[2], varImpAlignment[3], varImpAlignment[0],
					null);
			this.insight.getTaskStore().addTask(taskData);
			break;
		case "confmatrix":
			String rfType = this.rJavaTranslator.getString("RF_VARIABLE_999988888877777$type");
			if (rfType == "regression") {
				throw new IllegalArgumentException(
						"Confusion matrix is unavailable for regression-type random forest model.");
			}

			sb.append(temp_R + " <- getRFResults( RF_VARIABLE_999988888877777, 'confmatrix', sortBy=" + sortBy + ");");
			this.rJavaTranslator.runR(sb.toString());

			String[] confMatrixCols = this.rJavaTranslator.getColumns(temp_R);
			List<Object[]> confMatrixData = this.rJavaTranslator.getBulkDataRow(temp_R, confMatrixCols);

			taskData = ConstantTaskCreationHelper.getGridData(panelId, confMatrixCols, confMatrixData);
			this.insight.getTaskStore().addTask(taskData);
			break;
		}

		// clean up r temp variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + temp_R + ",getRF,getRFResults);");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());

		NounMetadata noun = new NounMetadata(taskData, PixelDataType.FORMATTED_DATA_SET, PixelOperationType.TASK_DATA);
		noun.addAdditionalReturn(new NounMetadata("Random Forest ran successfully!", PixelDataType.CONST_STRING,
				PixelOperationType.SUCCESS));
		return noun;
	}

	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////
	////////////////////// Input Methods///////////////////////////
	//////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////

	private String getStringInput(String keyName) {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getGenRowStruct(keyName);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		} else {
			if (keyName == REQUESTITEM) {
				throw new IllegalArgumentException("RequestItem of either 'varimp' or 'confmatrix' must be specified.");
			}
		}
		return null;
	}

	private String getPanelId() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getGenRowStruct(this.keysToGet[2]);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString();
			}
		}
		return null;
	}

}
