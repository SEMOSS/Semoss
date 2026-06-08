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

import prerna.ds.py.PandasFrame;
import prerna.query.interpreters.PandasInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DropRowsReactor extends AbstractPyFrameReactor {

	/**
	 * <p>
	 * This reactor drops rows based on a comparison
	 * </p>
	 *
	 * <p>
	 * The inputs to the reactor are:
	 * </p>
	 * <ul>
	 * <li>the filter comparison for dropping rows</li>
	 * </ul>
	 */

	public DropRowsReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.QUERY_STRUCT.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		String frameName = frame.getName();
		String wrapperFrameName = frame.getWrapperName();

		// the first noun will be a query struct - the filter
		SelectQueryStruct qs = getQueryStruct();
		// get the filters from the query struct
		// and iterate through each filtered column
		GenRowFilters grf = qs.getExplicitFilters();

		// use RInterpreter to create filter syntax
		StringBuilder pyFilterBuilder = new StringBuilder();
		PandasInterpreter pi = new PandasInterpreter();
		pi.setDataTableName(frameName, wrapperFrameName + ".cache['data']");
		pi.setDataTypeMap(frame.getMetaData().getHeaderToTypeMap());
		pi.addFilters(grf.getFilters(), wrapperFrameName, pyFilterBuilder, true);

		// execute the r script
		// FRAME <- FRAME[!( FRAME$Director == "value"),]
		String script = wrapperFrameName + ".cache['data'] =  " + wrapperFrameName + ".cache['data'][~"
				+ pyFilterBuilder.toString() + "]";
		frame.runScript(script);
		this.addExecutedCode(script);

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

	private SelectQueryStruct getQueryStruct() {
		SelectQueryStruct qs = (SelectQueryStruct) getValueFromKeyOrCurRow(this.keysToGet[0], 0);
		if (qs == null) {
			throw new IllegalArgumentException("Need to define filter condition");
		}
		return qs;
	}

}
