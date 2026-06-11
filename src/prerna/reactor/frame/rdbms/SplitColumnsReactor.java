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
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SplitColumnsReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(SplitColumnsReactor.class);

	private static final String COLUMNS_KEY = "columns";
	private static final String SEPARATOR_KEY = "sep";
	private static final String SEARCH_TYPE = "search";

	private static final String REGEX = "Regex";

	@Override
	public NounMetadata execute() {
		List<String> cols = getColumns();
		String separator = getSeparator();
		boolean isRegex = isRegex();
		if (!isRegex) {
			separator = Pattern.quote(separator);
		}
		AbstractRdbmsFrame frame = (AbstractRdbmsFrame) getFrame();
		PreparedStatement ps = null;

		for (int i = 1; i < cols.size(); i++) {
			String column = cols.get(i);
			SelectQueryStruct qs = new SelectQueryStruct();
			QueryColumnSelector selector = new QueryColumnSelector(column);
			qs.addSelector(selector);

			String table = frame.getName();
			if (column.contains("__")) {
				String[] split = column.split("__");
				column = split[1];
				table = split[0];
			}
			String colSplitBase = column + "_SPLIT_";
			Iterator<IHeadersDataRow> colIterator = null;
			try {
				colIterator = frame.query(qs);
			} catch (Exception e) {
				classLogger.error("Failed to query values for split column {} on table {}", column, table, e);
				throw new IllegalArgumentException("Error executing query with message = " + e.getMessage());
			}

			int highestIndex = 0;
			List<String> addedColumns = new Vector<>();

			// keep a batch size so we dont get heapspace
			final int batchSize = 5000;
			int count = 0;
			try {
				// iterate through the unique values
				while (colIterator.hasNext()) {
					// hold the existing value
					String nextVal = (String) colIterator.next().getRawValues()[0];
					// hold the array for the complex split
					String[] newVals = nextVal.split(separator);

					// since we do not know how many possible new columns will be generated
					// we need to check each time if we need to create a new "column" if not already
					// present
					if (newVals.length > highestIndex) {
						if (ps != null) {
							// since the update query now needs to change
							// flush all the current values in that were
							// not in the last batch
							ps.executeBatch();
						}
						Map<String, Set<String>> newEdgeHash = new LinkedHashMap<>();
						Set<String> set = new LinkedHashSet<>();
						for (int j = highestIndex; j < newVals.length; j++) {
							set.add(colSplitBase + j);
							addedColumns.add(colSplitBase + j);
						}
						newEdgeHash.put(column, set);
						// TODO: empty HashMap will default types to string, need to also be able to
						// create other type columns
						// in cases of splitting dates and decimals
						highestIndex = newVals.length;
						String[] columnTypes = new String[addedColumns.size()];
						String[] newColumns = new String[addedColumns.size()];
						for (int k = 0; k < columnTypes.length; k++) {
							newColumns[k] = addedColumns.get(k);
							columnTypes[k] = "STRING";
						}
						frame.addNewColumn(newColumns, columnTypes, frame.getName());
						ps = frame.getBuilder().createUpdatePreparedStatement(frame.getName(),
								addedColumns.toArray(new String[] {}), new String[] { column });
					}

					int colIndex = 0;
					if (ps == null) {
						throw new NullPointerException("PreparedStatement ps cannot be null here.");
					}

					for (; colIndex < newVals.length; colIndex++) {
						ps.setString(colIndex + 1, newVals[colIndex]);
					}
					// need to set empty values for the other columns
					// even if this split doesn't reach the end
					// otherwise the statement will error
					for (; colIndex < highestIndex; colIndex++) {
						ps.setString(colIndex + 1, "");
					}

					// now set the where variable in the ps
					ps.setString(colIndex + 1, nextVal);
					// add the update into the batch
					ps.addBatch();
					// batch commit based on size
					if (++count % batchSize == 0) {
						ps.executeBatch();
					}
				}
				// do not forget to add the final things in the batch that have not been
				// committed!
				if (ps != null) {
					ps.executeBatch();
				}
			} catch (SQLException e) {
				classLogger.error("Failed to split values for column {} on table {} with separator {}", column, table,
						separator, e);
			} finally {
				if (ps != null) {
					try {
						ps.close();
					} catch (SQLException e) {
						classLogger.error("Failed to close split update statement for column {} on table {}", column,
								table, e);
					}
				}
			}
			frame.syncHeaders();
		}

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private String getSeparator() {
		String separator = getString(SEPARATOR_KEY);
		if (separator == null || separator.isEmpty()) {
			throw new IllegalArgumentException("Need to define a separator to split the column with");
		}
		return separator;
	}

	private boolean isRegex() {
		GenRowStruct regexGrs = this.store.getGenRowStruct(SEARCH_TYPE);
		if (regexGrs == null || regexGrs.isEmpty()) {
			return true;
		}
		String val = regexGrs.get(0).toString();

		return val.equalsIgnoreCase(REGEX);
	}

	private List<String> getColumns() {
		List<String> cols = getListStringFromKeyOrCurRow(COLUMNS_KEY);
		if (!cols.isEmpty()) {
			return cols;
		}
		throw new IllegalArgumentException("Need to define the columns to split");
	}

}
