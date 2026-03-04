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
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class TransposeReactor extends AbstractPyFrameReactor {

	@Override
	public NounMetadata execute() {
		PandasFrame frame = (PandasFrame) getFrame();
		// get table name
		String table = frame.getWrapperName();

		String transposeScript = frame.getName() + "=" + table
				+ ".cache['data'].transpose().rename_axis('rn', axis=0).add_prefix('V').reset_index()";
		// python transpose creates numeric columns
		// need to make str type
		// change all datatypes to string
		String objectType = frame.getName() + "=" + frame.getName() + ".astype(str)";
		String stringType = frame.getName() + "=" + frame.getName() + ".astype('string')";
		insight.getPyTranslator().runEmptyPy(transposeScript, objectType, stringType);
		this.addExecutedCode(transposeScript);
		this.addExecutedCode(objectType);
		this.addExecutedCode(stringType);

		// String[] colTypes = getColumnTypes(frame);
		// insight.getPyTranslator().runPyAndReturnOutput(transposeScript,
		// stringHeaderType, stringType);

		// the column data has changed
		frame = (PandasFrame) recreateMetadata(frame);
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE,
				PixelOperationType.FRAME_HEADERS_CHANGE);
	}

}
