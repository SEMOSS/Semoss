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
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.om.HeadersException;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdateRowValuesReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(UpdateRowValuesReactor.class);

	@Override
	public NounMetadata execute() {
		AbstractRdbmsFrame frame = (AbstractRdbmsFrame) getFrame();
		String updateColumnInput = "";
		String updateTable = "";
		String updateColumn = "";
		Object updateValue = null;
		PixelDataType updateValueType = null;
		GenRowStruct inputsGRS = this.getCurRow();
		// get update column
		updateColumnInput = inputsGRS.getNoun(0).getValue() + "";
		if (updateColumnInput.contains("__")) {
			String[] split = updateColumnInput.split("__");
			updateTable = split[0];
			updateColumn = split[1];
		} else {
			updateTable = frame.getName();
			updateColumn = updateColumnInput;
		}
		// get update value
		NounMetadata noun = inputsGRS.getNoun(1);
		updateValueType = noun.getNounType();
		updateValue = noun.getValue();

		// validate updateColumn exists
		String[] existCols = getColNames(frame);
		if (Arrays.asList(existCols).contains(updateColumn) != true) {
			throw new IllegalArgumentException("Column " + updateColumn + " doesn't exist.");
		}

		// clean update value
		HeadersException colNameChecker = HeadersException.getInstance();
		if (updateValueType.equals(PixelDataType.CONST_STRING) && updateValue != null) {
			updateValue = colNameChecker.removeIllegalCharacters(String.valueOf(updateValue));
		}

		// get filters
		NounMetadata filterNoun = inputsGRS.getNoun(2);
		PixelDataType filterNounType = filterNoun.getNounType();
		try {
			if (filterNounType.equals(PixelDataType.QUERY_STRUCT)) {
				SelectQueryStruct qs = (SelectQueryStruct) filterNoun.getValue();
				GenRowFilters grf = qs.getExplicitFilters();
				Set<String> filteredColumns = grf.getAllFilteredColumns();
				for (String column : filteredColumns) {
					List<SimpleQueryFilter> filterList = grf.getAllSimpleQueryFiltersContainingColumn(column);
					for (SimpleQueryFilter queryFilter : filterList) {
						String sqlCondition = "";
						// col to values
						NounMetadata leftComp = queryFilter.getLComparison();
						String columnComp = leftComp.getValue() + "";
						if (columnComp.contains("__")) {
							String[] split = columnComp.split("__");
							sqlCondition += split[1];
						} else {
							sqlCondition += columnComp;
						}
						// does sqlCondition exist
						if (Arrays.asList(existCols).contains(sqlCondition) != true) {
							throw new IllegalArgumentException("Column " + sqlCondition + " doesn't exist.");
						}
						String nounComparator = queryFilter.getComparator();
						// clean nounComparator for sql statement
						if (nounComparator.equals("==")) {
							nounComparator = "=";
						} else if (nounComparator.equals("<>")) {
							nounComparator = "!=";
						}
						// rightComp has values
						NounMetadata rightComp = queryFilter.getRComparison();
						Object value = rightComp.getValue();

						// clean value
						if (rightComp.getNounType().equals(PixelDataType.CONST_STRING) && value != null) {
							value = colNameChecker.removeIllegalCharacters(String.valueOf(value));
						}

						String updateSql = "UPDATE " + updateTable + " SET " + updateColumn + " = ? WHERE "
								+ sqlCondition + " " + nounComparator + " ?";
						try (PreparedStatement statement = frame.getConn().prepareStatement(updateSql)) {
							if (updateValueType.equals(PixelDataType.CONST_STRING)) {
								statement.setString(1, updateValue == null ? null : String.valueOf(updateValue));
							} else {
								statement.setObject(1, updateValue);
							}
							if (rightComp.getNounType().equals(PixelDataType.CONST_STRING)) {
								statement.setString(2, value == null ? null : String.valueOf(value));
							} else {
								statement.setObject(2, value);
							}
							statement.executeUpdate();
						}
					}
				}
			}
		} catch (Exception e) {
			classLogger.error("Failed to update column {} on table {} using filter type {}", updateColumn, updateTable,
					filterNounType, e);
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

}
