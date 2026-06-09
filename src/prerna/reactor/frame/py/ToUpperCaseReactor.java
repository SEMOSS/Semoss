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

import java.util.List;

import prerna.ds.OwlTemporalEngineMeta;
import prerna.ds.py.PandasFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ToUpperCaseReactor extends AbstractPyFrameReactor {

	/**
	 * <p>
	 * This reactor changes columns to all upper case
	 * </p>
	 *
	 * <p>
	 * The inputs to the reactor are:
	 * </p>
	 * <ul>
	 * <li>the columns to update</li>
	 * </ul>
	 */

	public ToUpperCaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COLUMNS.getKey() };
	}

	@Override
	public NounMetadata execute() {
		// get frame
		PandasFrame frame = (PandasFrame) getFrame();

		OwlTemporalEngineMeta metaData = frame.getMetaData();

		// get the wrapper name
		// which is the framename with w in the end
		String wrapperFrameName = frame.getWrapperName();

		// get inputs
		List<String> columns = getListStringFromKeyOrCurRow(this.keysToGet[0]);
		StringBuilder commands = new StringBuilder();

		for (int i = 0; i < columns.size(); i++) {
			String col = columns.get(i);
			if (col.contains("__")) {
				String[] split = col.split("__");
				col = split[1];
//				wrapperFrameName = split[0];
			}

			String dataType = metaData.getHeaderTypeAsString(frame.getName() + "__" + col);
			if (dataType.equalsIgnoreCase("STRING")) {
				// script will be of the form:
				// wrapper.toupper(column_name)
				// insight.getPyTranslator().runEmptyPy(wrapperFrameName + ".upper('" + col +
				// "')");
				// this should get replaced with pandas pythonic way
				// wrapperFrameName.cache['data']['col'] =
				// wrapperFrameName.cache['data'].apply(lambda x: x['col'].upper(), axis = 1)
				commands.append((wrapperFrameName + ".cache['data']['" + col + "'] = " + wrapperFrameName
						+ ".cache['data'].apply(lambda x: str(x['" + col + "']).upper(), axis = 1)\n"));
				// insight.getPyTranslator().runEmptyPy(wrapperFrameName + ".cache['data']['" +
				// col + "'] = " +
				// wrapperFrameName + ".cache['data'].apply(lambda x: str(x['" + col +
				// "']).upper(), axis = 1)");
			}
		}
		insight.getPyTranslator().runEmptyPy(commands.toString());
		this.addExecutedCode(commands.toString());

		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
	}

}
