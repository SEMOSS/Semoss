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
package prerna.reactor.frame.polars;

import prerna.ds.py.PolarsFrame;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Registers an existing {@code polars.DataFrame} variable as a SEMOSS frame.
 * Arbitrary Python expressions and lazy frames are intentionally rejected.
 */
public class GenerateFrameFromPolarsVariableReactor extends AbstractReactor {

	public GenerateFrameFromPolarsVariableReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.VARIABLE.getKey(), ReactorKeysEnum.OVERRIDE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String variableName = getStringFromKeyOrCurRow(ReactorKeysEnum.VARIABLE.getKey(), 0);
		if (variableName == null || variableName.isEmpty()) {
			throw new IllegalArgumentException("A Polars Python variable name is required");
		}
		PolarsFrame frame = new PolarsFrame(variableName, this.insight.getPyTranslator());
		frame.registerExistingVariable(variableName);
		NounMetadata noun = new NounMetadata(frame, PixelDataType.FRAME,
				PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		if (getBoolean(ReactorKeysEnum.OVERRIDE.getKey(), true)) {
			this.insight.setDataMaker(frame);
		}
		this.insight.getVarStore().put(variableName, noun);
		return noun;
	}
}
