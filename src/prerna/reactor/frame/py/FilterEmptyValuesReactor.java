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

import prerna.ds.py.PandasFrame;
import prerna.query.interpreters.PandasInterpreter;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/*
 * This reactor filters out rows where the value is null (NaN, None, NaT) or empty ("") in the specified 
 * columns.
 */
public class FilterEmptyValuesReactor extends AbstractPyFrameReactor {

	protected static final String CLASS_NAME = FilterEmptyValuesReactor.class.getName();

	public FilterEmptyValuesReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		PandasFrame frame = (PandasFrame) getFrame();
		// get the frame name
		String frameName = frame.getName();
		String wrapperFrameName = frame.getWrapperName();
		String wrapperDataIndexed = wrapperFrameName + ".cache['data']";

		GenRowFilters grf = getFilters();

		StringBuilder pyFilterBuilder = new StringBuilder();
		PandasInterpreter pi = new PandasInterpreter();
		pi.setDataTableName(frameName, wrapperFrameName + ".cache['data']");
		pi.setDataTypeMap(frame.getMetaData().getHeaderToTypeMap());
		pi.addFilters(grf.getFilters(), wrapperFrameName, pyFilterBuilder, true);

		String script = wrapperDataIndexed + " = " + wrapperDataIndexed + "[" + pyFilterBuilder.toString() + "]";
		frame.runScript(script);
		this.addExecutedCode(script);
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private List<String> getColumns() {
		List<String> cols = new ArrayList<String>();

		// try its own key
		GenRowStruct colsGrs = this.store.getGenRowStruct(ReactorKeysEnum.COLUMNS.getKey());
		if (colsGrs != null && !colsGrs.isEmpty()) {
			int size = colsGrs.size();
			for (int i = 0; i < size; i++) {
				cols.add(colsGrs.get(i).toString());
			}
			return cols;
		}

		int inputSize = this.getCurRow().size();
		if (inputSize > 0) {
			for (int i = 0; i < inputSize; i++) {
				cols.add(this.getCurRow().get(i).toString());
			}
			return cols;
		}

		throw new IllegalArgumentException("Need to define the columns to filter");
	}

	private GenRowFilters getFilters() {
		// generate a grf with the wanted filters
		GenRowFilters grf = new GenRowFilters();
		List<String> columns = getColumns();
		List<Object> values = new ArrayList<Object>();
		values.add("");
		values.add(null);
		for (String col : columns) {
			grf.addFilters(SimpleQueryFilter.makeColToValFilter(col, "!=", values));
		}
		return grf;
	}
}
