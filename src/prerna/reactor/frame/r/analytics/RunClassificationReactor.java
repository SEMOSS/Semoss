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
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class RunClassificationReactor extends AbstractRFrameReactor {

	/**
	 * RunClassification(classify=[Species],attributes=["PetalLength","PetalWidth","SepalLength","SepalWidth"],
	 * panel=[0])
	 * RunClassification(classify=[race],attributes=["age","workclass","education","marital_status","relationship","sex","capital_gain","capital_loss","income"],
	 * panel=[0])
	 */

	private static final String CLASS_NAME = RunClassificationReactor.class.getName();
	private static final String CLASSIFICATION_COLUMN = "classify";

	public RunClassificationReactor() {
		this.keysToGet = new String[] { CLASSIFICATION_COLUMN, ReactorKeysEnum.ATTRIBUTES.getKey(),
				ReactorKeysEnum.PANEL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		init();
		String[] packages = new String[] { "data.table", "partykit", "dplyr", "naniar" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
		String dtName = frame.getName();
		boolean implicitFilter = false;
		String dtNameIF = "dtFiltered" + Utility.getRandomString(6);
		StringBuilder rsb = new StringBuilder();
		// load packages
		rsb.append("library('partykit');library('naniar');");
		// figure out inputs
		String predictionCol = getClassificationColumn();
		String predictionCol_R = "predictionCol" + Utility.getRandomString(8);
		rsb.append(predictionCol_R + "<- \"" + predictionCol + "\";");

		List<String> attributes = getColumns();
		if (attributes.contains(predictionCol)) {
			attributes.remove(predictionCol);
		}
		if (attributes.isEmpty()) {
			throw new IllegalArgumentException(
					"Must define at least one attribute that is not the dimension to classify");
		}
		String attributes_R = "attributes" + Utility.getRandomString(8);
		rsb.append(attributes_R + "<- " + RSyntaxHelper.createStringRColVec(attributes.toArray()) + ";");

		// check if there are filters on the frame. if so then need to run algorithm on
		// subsetted data
		if (!frame.getFrameFilters().isEmpty()) {
			// create a new qs to retrieve filtered frame
			SelectQueryStruct qs = new SelectQueryStruct();
			List<String> selectedCols = new ArrayList<String>(attributes);
			selectedCols.add(predictionCol);
			for (String s : selectedCols) {
				qs.addSelector(new QueryColumnSelector(s));
			}
			qs.setImplicitFilters(frame.getFrameFilters());
			qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, meta);
			RInterpreter interp = new RInterpreter();
			interp.setQueryStruct(qs);
			interp.setDataTableName(dtName);
			interp.setColDataTypes(meta.getHeaderToTypeMap());
			String query = interp.composeQuery();
			this.rJavaTranslator.runR(dtNameIF + "<- {" + query + "}");
			implicitFilter = true;

			// cleanup the temp r variable in the query var
			this.rJavaTranslator.runR("rm(" + query.split(" <-")[0] + ");gc();");
		}

		String targetDt = implicitFilter ? dtNameIF : dtName;

		// validate that the count of unique values in the instance column != number of
		// rows in the frame
		int nrows = frame.getNumRows(targetDt);
		int uniqInstCount = this.rJavaTranslator
				.getInt("if (is.factor(" + targetDt + "$" + predictionCol + ")) " + "length(levels(" + targetDt + "$"
						+ predictionCol + ")) else length(unique(" + targetDt + "$" + predictionCol + "));");
		if (nrows == uniqInstCount) {
			throw new IllegalArgumentException(
					"Values in the column to classify are all unique; classification algorithm is not applicable.");
		}

		// clustering r script
		String classificationScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\Classification.R";
		classificationScriptFilePath = classificationScriptFilePath.replace("\\", "/");
		rsb.append("source(\"" + classificationScriptFilePath + "\");");
		String outputList_R = "outputList" + Utility.getRandomString(8);

		// set call to R function
		rsb.append(outputList_R + " <- getCTree( " + targetDt + "," + predictionCol_R + "," + attributes_R + ");");

		// execute R
		this.rJavaTranslator.runR(rsb.toString());

		String[] predictors = this.rJavaTranslator.getStringArray(outputList_R + "$predictors;");
		String accuracy = this.rJavaTranslator.getString(outputList_R + "$accuracy;");
		String[] ctreeArray = this.rJavaTranslator.getStringArray(outputList_R + "$tree;");

		//// clean up r temp variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append("rm(" + outputList_R + "," + predictionCol_R + "," + attributes_R + "," + dtNameIF
				+ ",getCTree,getUsefulPredictors);");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());

		if (ctreeArray == null || ctreeArray.length == 0) {
			Map<String, Object> vizData = new HashMap<String, Object>();
			vizData.put("name", "Decision Tree For " + predictionCol);
			vizData.put("layout", "Dendrogram");
			vizData.put("panelId", getPanelId());
			// make an empty map
			Map<String, Map> classificationMap = new HashMap<String, Map>();
			classificationMap.put("No Tree Generated", new HashMap());
			vizData.put("children", classificationMap);
			NounMetadata noun = new NounMetadata(vizData, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.VIZ_OUTPUT);
			noun.addAdditionalReturn(new NounMetadata(
					"A decision tree could not be constructed for the requested dataset. Please retry with different data points.",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			return noun;
		}

		Map<String, Object> vizData = new HashMap<String, Object>();
		vizData.put("name", "Decision Tree For " + predictionCol);
		vizData.put("layout", "Dendrogram");
		vizData.put("panelId", getPanelId());
		// add the actual data
		Map<String, Map> classificationMap = processTreeString(ctreeArray);
		vizData.put("children", classificationMap);
		// add the accuracy and predictors
		List<Map<String, String>> statList = new ArrayList<Map<String, String>>();
		Map<String, String> statHash = new HashMap<String, String>();
		statHash.put("Accuracy", accuracy);
		statList.add(statHash);
		if (predictors != null && predictors.length > 0) {
			statHash = new Hashtable<String, String>();
			statHash.put("Relevant Predictors", String.join(", ", predictors));
			statList.add(statHash);
		}
		vizData.put("stats", statList);

		// now return this object
		NounMetadata noun = new NounMetadata(vizData, PixelDataType.CUSTOM_DATA_STRUCTURE,
				PixelOperationType.VIZ_OUTPUT, PixelOperationType.FORCE_SAVE_VISUALIZATION);
		noun.addAdditionalReturn(new NounMetadata("Classification ran successfully!", PixelDataType.CONST_STRING,
				PixelOperationType.SUCCESS));
		return noun;
	}

	private Map<String, Map> processTreeString(String[] ctreeArray) {
		Map<String, Map> treeMap = new HashMap<String, Map>();
		int index = Arrays.asList(ctreeArray).indexOf("[1] root");
		if (index == -1) {
			// single node case
			index = Arrays.asList(ctreeArray).indexOf("Fitted party:") + 1;
			generateNodeTreeWithParenthesis(treeMap, ctreeArray[index]);
		} else {
			// multi node case
			String[] treeStringArr = new String[ctreeArray.length - index - 4];
			System.arraycopy(ctreeArray, index + 1, treeStringArr, 0, treeStringArr.length);
			for (int i = 0; i < treeStringArr.length; i++) {
				treeStringArr[i] = treeStringArr[i].replaceAll("\\|\\s*\\[[0-9]+\\]\\s", "");
			}
			generateTreeEndingWithParenthesis(treeMap, "", 0, treeStringArr, new Integer(0));
		}

		return treeMap;
	}

	private void generateNodeTreeWithParenthesis(Map<String, Map> rootMap, String nodeValue) {
		String lastRegex = "\\(n\\s=.*\\)";

		String key = nodeValue.substring(10).replaceFirst(lastRegex, "").trim();
		rootMap.put(key, new HashMap<String, Map>());
	}

	private void generateTreeEndingWithParenthesis(Map<String, Map> rootMap, String startKey, int subTreeIndex,
			String[] treeStringArr, Integer index) {
		String endRegex = ".*\\(n\\s=.*\\)";
		String lastRegex = "\\(n\\s=.*\\)";

		Map<String, Map> currTree = new HashMap<String, Map>();
		if (!startKey.isEmpty()) {
			rootMap.put(startKey, currTree);
		}

		for (; index < treeStringArr.length; index++) {
			String row = "";
			boolean newRowNeeded = false;
			while (newRowNeeded == false) {
				String rowSubstring = treeStringArr[index].split(": ")[0].replaceAll("^(\\|\\s*)*", "");
				if (rootMap.toString().contains(rowSubstring)) {
					index++;
					if (index >= treeStringArr.length) {
						return;
					}
				} else {
					newRowNeeded = true;
					row = treeStringArr[index];
				}
			}

			if (!row.startsWith("|")) {
				if (subTreeIndex > 0) {
					index--;
					return;
				}
				if (row.matches(endRegex)) {
					String[] keyVal = row.replaceFirst(lastRegex, "").split(": ");
					Map<String, Map> endMap = new HashMap<String, Map>();
					endMap.put(keyVal[1].trim(), new HashMap<String, Map>());
					rootMap.put(keyVal[0].trim(), endMap);
				} else {
					String newRow = row.trim();
					currTree = new HashMap<String, Map>();
					rootMap.put(newRow, currTree);
					startKey = newRow;
					subTreeIndex = 0;
				}
			} else if (row.lastIndexOf("| ") != subTreeIndex) {
				// either done with the currtree - need to pull back out and assess whether
				// rootMap needs to be grown out more
				return;
			} else if (row.matches(endRegex)) {
				String[] keyVal = row.substring(row.lastIndexOf("| ") + 1, row.length()).trim()
						.replaceFirst(lastRegex, "").split(": ");
				Map<String, Map> endMap = new HashMap<String, Map>();
				endMap.put(keyVal[1].trim(), new HashMap<String, Map>());
				currTree.put(keyVal[0].trim(), endMap);
			} else {
				index++;
				String newKey = row.substring(row.lastIndexOf("| ") + 1, row.length()).trim();
				// for a subtree to exist, there must be a new row after
				int newSubTreeIndex = treeStringArr[index].lastIndexOf("| ");
				generateTreeEndingWithParenthesis(currTree, newKey, newSubTreeIndex, treeStringArr, index);
			}
		}
	}

	private String getClassificationColumn() {
		String classificationColumn = getStringFromKeyOrCurRow(CLASSIFICATION_COLUMN, 0);
		if (classificationColumn == null || classificationColumn.isEmpty()) {
			String errorString = "Could not find the column predict";
			throw new IllegalArgumentException(errorString);
		}
		return classificationColumn;
	}

	private List<String> getColumns() {
		GenRowStruct columnGrs = this.store.getGenRowStruct(keysToGet[1]);
		if (columnGrs == null) {
			throw new IllegalArgumentException("Attribute columns must be specified.");
		}
		return getListStringFromKeyOrCurRow(keysToGet[1]);
	}

	private String getPanelId() {
		return getString(keysToGet[2]);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(CLASSIFICATION_COLUMN)) {
			return "The classification column";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
