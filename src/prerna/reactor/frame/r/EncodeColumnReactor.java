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

import java.util.List;

import prerna.algorithm.api.SemossDataType;
import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.r.RDataTable;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class EncodeColumnReactor extends AbstractRFrameReactor {

	public EncodeColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// init R
		init();

		String[] packages = { "digest" };
		this.rJavaTranslator.checkPackages(packages);

		RDataTable frame = (RDataTable) getFrame();
		String frameName = frame.getName();
		List<String> columns = getColumns();
		if (columns == null || columns.isEmpty()) {
			throw new IllegalArgumentException("Need to pass in the columns to encode");
		}

		StringBuilder script = new StringBuilder();
		script.append("library(digest);encode <- function(value) digest(value, algo=\"sha256\");");

		for (String col : columns) {
			String select = frameName + "$" + col;
			script.append(select).append(" <- sapply(").append(select).append(", encode);");
		}

		this.rJavaTranslator.executeEmptyR(script.toString());
		this.addExecutedCode(script.toString());

		// upon successful execution
		OwlTemporalEngineMeta metadata = frame.getMetaData();
		for (String col : columns) {
			// set the type for all the columns to be string
			metadata.modifyDataTypeToProperty(frameName + "__" + col, frameName, SemossDataType.STRING.toString());
		}

		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		return noun;
	}

	private List<String> getColumns() {
		// EncodeColumn(columns=["a","b","c"]);
		GenRowStruct grs = this.store.getGenRowStruct(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			return grs.getAllStrValues();
		}

		// this is if passed in directly EncodeColumn("a","b","c");
		return this.curRow.getAllStrValues();
	}

}
