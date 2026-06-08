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

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.query.interpreters.RInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DropRowsReactor extends AbstractRFrameReactor {

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
		init();
		// get frame
		RDataTable frame = (RDataTable) getFrame();
		String table = frame.getName();

		// the first noun will be a query struct - the filter
		SelectQueryStruct qs = getQueryStruct();
		// get the filters from the query struct
		// and iterate through each filtered column
		GenRowFilters grf = qs.getExplicitFilters();

		// use RInterpreter to create filter syntax
		OwlTemporalEngineMeta frameMetadata = frame.getMetaData();
		try {
			grf = QSAliasToPhysicalConverter.convertGenRowFilters(grf, frameMetadata, null);
		} catch (Exception ex) {
			return getWarning("Frame is out of sync / No Such Column. Cannot perform this operation");
		}
		StringBuilder rFilterBuilder = new StringBuilder();
		RInterpreter ri = new RInterpreter();
		ri.setColDataTypes(frameMetadata.getHeaderToTypeMap());
		ri.addFilters(grf.getFilters(), table, rFilterBuilder, true, true);

		// execute the r script
		// FRAME <- FRAME[!( FRAME$Director == "value"),]
		String newScript = table + "<- " + table + "[!(" + rFilterBuilder.toString() + "),]";
		frame.executeRScript(newScript);
		this.addExecutedCode(newScript);

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
