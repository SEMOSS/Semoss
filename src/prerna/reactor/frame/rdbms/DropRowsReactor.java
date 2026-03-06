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

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class DropRowsReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(DropRowsReactor.class);

	@Override
	public NounMetadata execute() {
		AbstractRdbmsFrame frame = (AbstractRdbmsFrame) getFrame();
		GenRowStruct inputsGRS = this.getCurRow();
		String sqlStatements = "";
		NounMetadata filterNoun = inputsGRS.getNoun(0);
		PixelDataType filterNounType = filterNoun.getNounType();
		if (filterNounType.equals(PixelDataType.QUERY_STRUCT)) {
			SelectQueryStruct qs = (SelectQueryStruct) filterNoun.getValue();
			GenRowFilters grf = qs.getExplicitFilters();
			Set<String> filteredColumns = grf.getAllFilteredColumns();
			for (String filColumn : filteredColumns) {
				List<SimpleQueryFilter> filterList = grf.getAllSimpleQueryFiltersContainingColumn(filColumn);
				for (SimpleQueryFilter queryFilter : filterList) {
					String table = "";
					String column = "";
					// col to values
					NounMetadata leftComp = queryFilter.getLComparison();
					String columnComp = leftComp.getValue() + "";
					if (columnComp.contains("__")) {
						String[] split = columnComp.split("__");
						table = split[0];
						column = split[1];
					}
					String nounComparator = queryFilter.getComparator();
					// clean nounComparator for sql statement
					if (nounComparator.equals("==")) {
						nounComparator = "=";
					} else if (nounComparator.equals("<>")) {
						nounComparator = "!=";
					}
					NounMetadata rightComp = queryFilter.getRComparison();
					Object value = rightComp.getValue();

					// escape single quote for sql
					if (String.valueOf(value).contains("'")) {
						value = String.valueOf(value).replaceAll("'", "''");
					}

					// check the column exists, if not then throw warning
					String[] allCol = getColNames(frame);
					if (Arrays.asList(allCol).contains(column) != true) {
						throw new IllegalArgumentException("Column doesn't exist.");
					}

					// put quotes if string
					if (rightComp.getNounType().equals(PixelDataType.CONST_STRING)) {
						sqlStatements += "DELETE FROM " + table + " WHERE " + column + " " + nounComparator + " '"
								+ value + "'; ";
					} else {
						sqlStatements += "DELETE FROM " + table + " WHERE " + column + " " + nounComparator + " "
								+ value + "; ";
					}
				}
			}
		}

		try {
			frame.getBuilder().runQuery(sqlStatements);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

}
