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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.ds.r.RSyntaxHelper;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class DiscretizeReactor extends AbstractRFrameReactor {

	/**
	 * <p>
	 * Discretize([{"column":"PetalLength"}, {"column":"SepalLength",
	 * "breaks":"(4.3, 5.5, 6.7, 7.9)", "labels":"(Short,Medium,Long)"},
	 * {"column":"PetalWidth", "breaks":"0:5*.5"}])
	 * </p>
	 *
	 * <p>
	 * Discretize({"column":"MovieBudget", "numDigits":"10"})
	 * </p>
	 *
	 * <p>
	 * Input keys:
	 * </p>
	 * <ul>
	 * <li>column (required)</li>
	 * <li>breaks (conditionally required; required only if labels specified) - can
	 * be one of three types: integer, breakpoints as a list, or mathematical
	 * notation of range. Breakpoints as a list or mathematical notation of range
	 * must be specified in ascending order</li>
	 * <li>labels (optional)</li>
	 * <li>numDigits (optional) - specifies number of digits used in formatting the
	 * break/range numbers</li>
	 * </ul>
	 *
	 * <p>
	 * Return format: if labels is not specified, then the discretized ranges are
	 * wrapped with [ ] (inclusive), () (exclusive), or a combination and contain
	 * the lower and upper range comma-separated, for example [0, 100) which means 0
	 * =< x < 100.
	 * </p>
	 */

	private static final String requestMap = "requestMap";

	public DiscretizeReactor() {
		this.keysToGet = new String[] { requestMap };
	}

	@Override
	public NounMetadata execute() {
		init();
		String[] packages = new String[] { "data.table", "dplyr" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();

		String dtName = frame.getName();
		List<String> colNames = Arrays.asList(frame.getColumnNames());
		List<Object> reqList = this.curRow.getValuesOfType(PixelDataType.MAP);

		StringBuilder inputListSB = new StringBuilder();
		for (int i = 0; i < reqList.size(); i++) {
			StringBuilder listSB = new StringBuilder();
			Map<String, Object> parsedMap = (Map<String, Object>) reqList.get(i);
			String name = (String) parsedMap.get("column");
			if (name == null || name == "") {
				throw new IllegalArgumentException("Column name needs to be specified.");
			} else if (!colNames.contains(name)) {
				throw new IllegalArgumentException(
						"Specified column name, " + name + ", is unavailable in the data frame.");
			}

			// if we get to this point, we have a valid column name specified
			listSB.append(name + "=");

			// get column data type & only proceed if type = numeric
			String dataType = meta.getHeaderTypeAsString(dtName + "__" + name);

			if (!Utility.isNumericType(dataType)) {
				throw new IllegalArgumentException("Specified column name, " + name + ", must be a numeric type");
			}

			String breaks = (String) parsedMap.get("breaks");
			String labels = (String) parsedMap.get("labels");
			String numDigitsStr = (String) parsedMap.get("numDigits");

			// validate that if breaks specified, then it doesn't contain
			// any alpahbetical characters
			if (breaks == null || breaks.isEmpty()) {
				// breaks var was not specified
				listSB.append("list(");
			} else {
				breaks = breaks.replaceAll("[()]", "").trim();
				if (breaks != null && !breaks.isEmpty() && breaks.matches(".*[a-zA-z]+.*") == true) {
					throw new IllegalArgumentException("Breaks should be either a numerical integer or a "
							+ "numerical vector. No alphabetical characters allowed.");
				} else {
					// valid breaks specified
					listSB.append("list(breaks=c(" + breaks + ")");
				}
			}

			// validate that if labels specified, then valid breaks variable
			// is available also
			boolean isValidLabels = false;
			if (labels != null && !labels.isEmpty()) {
				if (breaks == null || breaks.isEmpty() || breaks.matches(".*[a-zA-z]+.*") == true) {
					throw new IllegalArgumentException("Please specify breaks (cannot contain "
							+ "alphabetical characters) - breaks are required if labels are provided.");
				} else {
					// check if labels contains whitespaces, then replace with
					// underscore
					labels = labels.replaceAll("[()]", "").trim();
					String[] labelsSplit = labels.split(",");
					List<String> labelsList = Arrays.asList(labelsSplit);
					for (int j = 0; j < labelsList.size(); j++) {
						String jLabel = "'" + labelsList.get(j).replaceAll("\"", "").trim().replaceAll("\\s", "_")
								+ "'";
						labelsList.set(j, jLabel);
					}
					labels = String.join(",", labelsList);
					listSB.append(", labels=c(" + labels + ")");
					isValidLabels = true;
				}
			}

			// validate that if numDigits specified AND labels is absent, then numDigits is
			// a positive integer > 0
			if (numDigitsStr == null || numDigitsStr.isEmpty() || isValidLabels == true) {
				listSB.append(")");
			} else {
				try {
					int numDigits = Integer.parseInt(numDigitsStr);
					if (numDigitsStr.replaceAll("[\\D]", "").matches("^[0-9]*[1-9][0-9]*$")) {
						if (listSB.indexOf("(") == listSB.length() - 1) {
							listSB.append("dig.lab=" + numDigits + ")");
						} else {
							listSB.append(", dig.lab=" + numDigits + ")");
						}
					} else {
						throw new IllegalArgumentException("Number of digits specified must be a positive integer.");
					}
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("Number of digits specified must be an integer.");
				}
			}

			if (listSB.length() > 0) {
				if (i == 0) {
					inputListSB.append(listSB);
				} else {
					inputListSB.append(", " + listSB);
				}
			}
		}

		StringBuilder sb = new StringBuilder();
		String inputList_R = "inputList" + Utility.getRandomString(8);
		sb.append(inputList_R + " <- list(" + inputListSB + ");");

		// discretize r scripts
		for (String fileName : new String[] { "Discretize_Source.R", "Discretize.R" }) {
			String scriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\" + fileName;
			scriptFilePath = scriptFilePath.replace("\\", "/");
			sb.append("source(\"" + scriptFilePath + "\");");
		}

		// set call to R function
		sb.append(dtName + " <- discretizeColumnsDt( " + dtName + "," + inputList_R + ");");

		// execute R
		this.rJavaTranslator.runR(sb.toString());
		this.addExecutedCode(sb.toString());

		// retrieve new columns to add to meta
		List<String> updatedDtColumns = new ArrayList<String>(Arrays.asList(this.rJavaTranslator.getColumns(dtName)));
		updatedDtColumns.removeAll(colNames);

		String colLevels_R = "colLevels" + Utility.getRandomString(8);
		if (!updatedDtColumns.isEmpty()) {
			for (String newColName : updatedDtColumns) {
				meta.addProperty(dtName, dtName + "__" + newColName);
				meta.setAliasToProperty(dtName + "__" + newColName, newColName);
				meta.setDataTypeToProperty(dtName + "__" + newColName, "FACTOR");
				this.rJavaTranslator.runR(new StringBuilder()
						.append(colLevels_R + "<-" + RSyntaxHelper.getOrderedLevelsFromRFactorCol(dtName, newColName))
						.toString());
				String orderedLevels = this.rJavaTranslator.getString(colLevels_R);
				meta.setOrderingToProperty(dtName + "__" + newColName, orderedLevels);
			}
		} else {
			// no results
			throw new IllegalArgumentException("The selected columns could not be discretized.");
		}

		// clean up r temp variables
		StringBuilder cleanUpScript = new StringBuilder();
		cleanUpScript.append(
				"rm(" + inputList_R + "," + colLevels_R + ",discretizeColumnsDt, discretize, getNewColumnName);");
		cleanUpScript.append("gc();");
		this.rJavaTranslator.runR(cleanUpScript.toString());
		this.addExecutedCode(cleanUpScript.toString());

		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);
		noun.addAdditionalReturn(NounMetadata
				.getSuccessNounMessage("Successfully added discretized column: " + updatedDtColumns.get(0)));
		return noun;
	}
}
