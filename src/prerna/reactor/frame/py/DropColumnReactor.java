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

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.nounmeta.RemoveHeaderNounMetadata;

public class DropColumnReactor extends AbstractPyFrameReactor {

	/**
	 * <p>
	 * This reactor drops columns from the frame.
	 * </p>
	 *
	 * <p>
	 * The inputs to the reactor are:
	 * </p>
	 * <ul>
	 * <li>list of columns to drop</li>
	 * </ul>
	 */

	public DropColumnReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// initialize rJavaTranslator
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();
		OwlTemporalEngineMeta metaData = this.getFrame().getMetaData();

		// get table name
		String wrapperFrameName = frame.getWrapperName();

		// store the list of names being removed
		List<String> remCols = new ArrayList<String>();

		// get inputs
		List<String> columns = getListStringFromKeyOrCurRow(this.keysToGet[0]);
		String[] remCommands = new String[columns.size()];
		for (int i = 0; i < columns.size(); i++) {
			String col = columns.get(i);
			if (col.contains("__")) {
				String[] split = col.split("__");
				col = split[1];
//				wrapperFrameName = split[0];
			}
			// define the script to be executed
			remCommands[i] = wrapperFrameName + ".drop_col('" + col + "')";
			remCols.add(col);

			metaData.dropProperty(frame.getName() + "__" + col, frame.getName());
			// drop filters with this column
			frame.getFrameFilters().removeColumnFilter(col);
		}
		// run the script
		insight.getPyTranslator().runEmptyPy(remCommands);
		for (String script : remCommands) {
			this.addExecutedCode(script);
		}

		// reset the frame headers
		frame.syncHeaders();

		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE,
				PixelOperationType.FRAME_DATA_CHANGE);
		retNoun.addAdditionalReturn(new RemoveHeaderNounMetadata(remCols));
		return retNoun;
	}

}
