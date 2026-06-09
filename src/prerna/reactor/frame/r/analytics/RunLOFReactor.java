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

import org.apache.commons.lang3.StringUtils;

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

public class RunLOFReactor extends AbstractRFrameReactor {

	/**
	 * <p>
	 * RunLOF(instance = [Studio], kNeighbors = [10], attributes = ["MovieBudget",
	 * "Revenue_Domestic"])
	 * </p>
	 *
	 * <p>
	 * RunLOF(instance = [Studio], kNeighbors = ["10:12, 5"], attributes =
	 * ["MovieBudget", "Revenue_Domestic"])
	 * </p>
	 *
	 * <p>
	 * RunLOF(instance = [Studio], kNeighbors = ["10,11,12"], attributes =
	 * ["MovieBudget", "Revenue_Domestic"])
	 * </p>
	 *
	 * <p>
	 * RunLOF(instance = [Species], uniqInstPerRow = ["no"], kNeighbors = [2],
	 * attributes = ["SepalLength", "SepalWidth", "PetalLength", "PetalWidth"])
	 * </p>
	 *
	 * <p>
	 * Input keys:
	 * </p>
	 * <ul>
	 * <li>instance (required)</li>
	 * <li>kNeighbors (required) - can be one of three types: integer, integer range
	 * in quotes, or comma-separated list in quotes; examples: [10], ["5:7"], ["5,
	 * 6, 7"]</li>
	 * <li>attributes (required) - must be columns with numeric data type</li>
	 * <li>uniqInstPerRow (optional; if not passed in, assumes no) - if yes, each
	 * row is treated as a unique instance/record; if no, data in the attribute
	 * columns is aggregated by the instance column first</li>
	 * </ul>
	 */

	private static final String K_NEIGHBORS = "kNeighbors";
	private static final String UNIQUE_INSTANCE_PER_ROW = "uniqInstPerRow";

	private String instanceColumn;
	private List<String> attributeNamesList;
	private double[] kArray;

	public RunLOFReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.INSTANCE_KEY.getKey(), ReactorKeysEnum.ATTRIBUTES.getKey(),
				K_NEIGHBORS, UNIQUE_INSTANCE_PER_ROW };
	}

	@Override
	public NounMetadata execute() {
		init();
		String[] packages = new String[] { "Rlof", "data.table", "dplyr", "VGAM" };
		this.rJavaTranslator.checkPackages(packages);
		RDataTable frame = (RDataTable) getFrame();
		OwlTemporalEngineMeta meta = this.getFrame().getMetaData();
		String dtName = frame.getName();
		boolean implicitFilter = false;
		String dtNameIF = "dtFiltered" + Utility.getRandomString(6);
		String tempKeyCol = "tempGenUUID99SM_" + Utility.getRandomString(6);
		StringBuilder sb = new StringBuilder();

		// get first set of inputs in preparation for the first R function
		this.instanceColumn = getInstanceColumn();
		this.attributeNamesList = getAttrList();
		for (String attr : this.attributeNamesList) {
			String dataType = meta.getHeaderTypeAsString(dtName + "__" + attr);
			if (!Utility.isNumericType(dataType)) {
				throw new IllegalArgumentException("Attribute columns must be of numeric type.");
			}
		}

		// check if there are filters on the frame. if so then need to run algorithm on
		// subsetted data and later join
		if (!frame.getFrameFilters().isEmpty()) {
			// prep the original frame by adding a temporary column, serving as row index
			addUUIDColumnToOrigFrame(dtName, meta, tempKeyCol);

			// create a new qs to retrieve filtered frame
			SelectQueryStruct qs = new SelectQueryStruct();
			List<String> selectedCols = new ArrayList<String>(attributeNamesList);
			selectedCols.add(instanceColumn);
			selectedCols.add(tempKeyCol);
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

		// set R variables to run first R function
		String targetDt = implicitFilter ? dtNameIF : dtName;
		String uniqInstPerRowStr = getUniqInstPerRow();
		String uniqInstPerRow_R = "uniqInstPerRow" + Utility.getRandomString(8);
		if (uniqInstPerRowStr != null && uniqInstPerRowStr.equalsIgnoreCase("TRUE")) {
			sb.append(uniqInstPerRow_R + "<-TRUE;");
		} else {
			sb.append(uniqInstPerRow_R + "<-FALSE;");
		}
		String instCol_R = "instCol" + Utility.getRandomString(8);
		sb.append(instCol_R + "<- \"" + this.instanceColumn + "\";");
		String attrList_R = "attrList" + Utility.getRandomString(8);
		sb.append(attrList_R + "<- " + RSyntaxHelper.createStringRColVec(this.attributeNamesList.toArray()) + ";");
		String RLOFScriptFilePath = getBaseFolder() + "\\R\\AnalyticsRoutineScripts\\LOF.R";
		RLOFScriptFilePath = RLOFScriptFilePath.replace("\\", "/");
		sb.append("source(\"" + RLOFScriptFilePath + "\");");

		int sbLength = sb.length();
		String scaleUniqueData_R = "scaleUniqueData" + Utility.getRandomString(8);
		sb.append(scaleUniqueData_R + "<-scaleUniqueData(" + targetDt + "," + instCol_R + "," + attrList_R + ","
				+ uniqInstPerRow_R + ");");
		this.rJavaTranslator.runR(sb.toString());
		sb.delete(sbLength, sb.length());
		int nrows = this.rJavaTranslator.getInt(scaleUniqueData_R + "$dtSubset[,.N];");
		if (nrows == 1) {
			meta.dropProperty(dtName + "__" + tempKeyCol, dtName);
			this.rJavaTranslator.runR("rm(" + scaleUniqueData_R + "," + instCol_R + "," + attrList_R + ","
					+ uniqInstPerRow_R + "," + dtNameIF + ",scaleUniqueData,runLOF,getLOP,getNewColumnName);gc();");
			throw new IllegalArgumentException("Instance column contains only 1 unique record.");
		}

		String kStr = getK();
		boolean intK = this.rJavaTranslator.getBoolean("all(" + "c(" + kStr + ") == floor( " + "c(" + kStr + ")))");
		this.kArray = this.rJavaTranslator.getDoubleArray("c(" + kStr + ")");
		if (!intK || this.kArray == null || this.kArray.length == 0) {
			meta.dropProperty(dtName + "__" + tempKeyCol, dtName);
			this.rJavaTranslator.runR("rm(" + scaleUniqueData_R + "," + instCol_R + "," + attrList_R + ","
					+ uniqInstPerRow_R + "," + dtNameIF + ",scaleUniqueData,runLOF,getLOP,getNewColumnName);gc();");
			throw new IllegalArgumentException("K must be either a single integer or a list of integers only.");
		}
		for (double k : this.kArray) {
			if (k > nrows) {
				meta.dropProperty(dtName + "__" + tempKeyCol, dtName);
				this.rJavaTranslator.runR("rm(" + scaleUniqueData_R + "," + instCol_R + "," + attrList_R + ","
						+ uniqInstPerRow_R + "," + dtNameIF + ",scaleUniqueData,runLOF,getLOP,getNewColumnName);gc();");
				throw new IllegalArgumentException(
						"K must be less than the number of unique instances, " + nrows + ".");
			}
		}
		String k_R = "kNeighbors" + Utility.getRandomString(8);
		sb.append(k_R + "<- c(" + kStr + ");");

		// set call to R function
		sb.append(targetDt + " <- runLOF( " + scaleUniqueData_R + "," + instCol_R + "," + attrList_R + "," + k_R + ","
				+ uniqInstPerRow_R + "," + RSyntaxHelper.createStringRColVec(frame.getColumnHeaders()) + ");");

		// execute R
		this.rJavaTranslator.runR(sb.toString());

		// retrieve new columns to add to meta
		String[] updatedDfColumns = this.rJavaTranslator.getColumns(targetDt);

		// clean up r temp variables
		this.rJavaTranslator.runR("rm(" + scaleUniqueData_R + "," + instCol_R + "," + attrList_R + ","
				+ uniqInstPerRow_R + "," + k_R + ",scaleUniqueData,runLOF,getLOP,getNewColumnNam);gc();");

		List<String> origDfCols = new ArrayList<String>(Arrays.asList(frame.getColumnHeaders()));
		List<String> updatedDfCols = new ArrayList<String>(Arrays.asList(updatedDfColumns));
		updatedDfCols.removeAll(origDfCols);

		// drop the temporary column of row index from metadata
		meta.dropProperty(dtName + "__" + tempKeyCol, dtName);

		if (!updatedDfCols.isEmpty()) {
			// if implicitFilter == true, then need to join the resulting column to the
			// whole frame (dtName var)
			if (implicitFilter) {
				this.rJavaTranslator.runR(dtName + "<-merge(" + dtName + ", " + dtNameIF + "[,c('" + tempKeyCol + "',"
						+ "'" + StringUtils.join(updatedDfCols, "','") + "'" + "), with=FALSE],by ='" + tempKeyCol
						+ "', all.x=TRUE);" + dtName + "[," + tempKeyCol + " := NULL] ;");
			}
			this.rJavaTranslator.runR("rm(" + dtNameIF + ");gc()");

			// update metadata with the new column information
			for (String newColName : updatedDfCols) {
				meta.addProperty(dtName, dtName + "__" + newColName);
				meta.setAliasToProperty(dtName + "__" + newColName, newColName);
				meta.setDataTypeToProperty(dtName + "__" + newColName, "DOUBLE");
			}
		} else {
			// no results
			this.rJavaTranslator.runR("rm(" + dtNameIF + ");gc()");
			throw new IllegalArgumentException("LOF algorithm returned no results.");
		}

		// now return this object
		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
		noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage(
				"LOF ran succesfully! See new \"" + updatedDfCols.get(0) + "\" column in the grid."));
		return noun;
	}

	private void addUUIDColumnToOrigFrame(String frameName, OwlTemporalEngineMeta meta, String tempKeyCol) {
		this.rJavaTranslator.executeEmptyR(frameName + "$" + tempKeyCol + "<- seq.int(nrow(" + frameName + "));");

		meta.addProperty(frameName, frameName + "__" + tempKeyCol);
		meta.setAliasToProperty(frameName + "__" + tempKeyCol, tempKeyCol);
		meta.setDataTypeToProperty(frameName + "__" + tempKeyCol, "INT");
	}

	private String getInstanceColumn() {
		String instanceColumn = getStringFromKeyOrCurRow(keysToGet[0], 0);
		if (instanceColumn == null || instanceColumn.isEmpty()) {
			String errorString = "Could not find the instance column";
			throw new IllegalArgumentException(errorString);
		}
		return instanceColumn;
	}

	private String getUniqInstPerRow() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getGenRowStruct(UNIQUE_INSTANCE_PER_ROW);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				String value = columnGrs.get(0).toString().toUpperCase();
				if (value.equals("YES")) {
					return "TRUE";
				} else if (value.equals("NO")) {
					return "FALSE";
				}
			}
		} else {
			return "FALSE";
		}
		return null;
	}

	private String getK() {
		// see if defined as individual key
		GenRowStruct columnGrs = this.store.getGenRowStruct(K_NEIGHBORS);
		if (columnGrs != null) {
			if (columnGrs.size() > 0) {
				return columnGrs.get(0).toString().replaceAll("\\s", "");
			}
		} else {
			throw new IllegalArgumentException("Please specify kNeighbors.");
		}
		return null;
	}

	private List<String> getAttrList() {
		GenRowStruct columnGrs = this.store.getGenRowStruct(keysToGet[1]);
		if (columnGrs != null) {
			return convertAllValuesToString(columnGrs);
		}
		throw new IllegalArgumentException("Please specify attributes.");
	}
}
